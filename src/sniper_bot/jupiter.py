"""Jupiter quote provider in quote-only mode."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import httpx

from .errors import QuoteStaleError, QuoteUnavailableError, RateLimitExceededError
from .models import QuoteResponse, RoundTripQuote
from .quote_journal import QuoteJournal


def _journal_time(
    record: dict[str, Any], key: str, fallback: datetime
) -> datetime:
    raw = record.get(key)
    if not raw:
        return fallback
    try:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return fallback
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


class JupiterQuoteProvider:
    def __init__(
        self,
        api_key: str,
        *,
        quote_mint: str | None = None,
        quote_mint_decimals: int = 6,
        base_url: str = "https://api.jup.ag/swap/v2",
        timeout_seconds: float = 2.5,
        max_retries: int = 2,
        rate_limit_per_second: float = 3.0,
        cache_seconds: float = 1.0,
        cache_floor_usd: Decimal = Decimal("100"),
        replay_mode: bool = False,
        record_quotes: bool = False,
        quote_journal_path: str | None = None,
        recorder: Callable[..., Awaitable[str]] | None = None,
        metrics: Any | None = None,
    ) -> None:
        if (replay_mode or record_quotes) and not quote_journal_path:
            raise ValueError("quote_journal_path is required when replay or record mode is enabled")
        self._api_key = api_key
        self._quote_mint = quote_mint
        self._quote_mint_decimals = quote_mint_decimals
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._max_retries = max_retries
        self._rate_window = 1.0 / max(1e-6, rate_limit_per_second)
        self._cache_seconds = cache_seconds
        self._cache_floor_usd = cache_floor_usd
        self._quote_cache: dict[str, tuple[datetime, QuoteResponse]] = {}
        self._last_call = datetime.min.replace(tzinfo=timezone.utc)
        self._rate_lock = asyncio.Lock()
        self._replay_mode = replay_mode
        self._record_quotes = record_quotes
        self._quote_journal = QuoteJournal(quote_journal_path) if quote_journal_path else None
        self._recorder = recorder
        self._metrics = metrics
        self._last_sol_usd_price = Decimal("0")
        self._clock: Callable[[], datetime] = lambda: datetime.now(tz=timezone.utc)

    def set_clock(self, clock: Callable[[], datetime]) -> None:
        self._clock = clock

    async def get_quote(
        self,
        token_in: str,
        token_out: str,
        amount_usd: Decimal,
        *,
        force_no_taker: bool = True,
        slippage_bps: int = 120,
        cache: bool = False,
        cache_ttl_seconds: float | None = None,
    ) -> QuoteResponse:
        if token_in == token_out:
            raise QuoteUnavailableError("token_in and token_out must differ")
        if amount_usd <= 0:
            raise ValueError("amount_usd must be positive")

        payload: dict[str, Any] = {
            "inputMint": token_in,
            "outputMint": token_out,
            "amount": self._to_amount_int(amount_usd, is_quote_mint=token_in == (self._quote_mint or "")),
            "slippageBps": int(slippage_bps),
            "swapMode": "ExactIn",
            "onlyDirectRoutes": False,
        }
        # Omitting taker is the Jupiter V2 quote-only contract. An empty taker is not sent.

        if cache and self._is_cacheable(token_in, amount_usd):
            cached = self._get_cached_quote(payload, cache_ttl_seconds)
            if cached is not None:
                return cached

        request_key = self._quote_request_key(payload)
        if self._quote_journal is not None:
            record = self._quote_journal.get_record(request_key)
            cached_result = record.get("response") if record else None
            if isinstance(cached_result, dict):
                assert record is not None
                requested_at = _journal_time(record, "requested_at", self._clock())
                received_at = _journal_time(record, "received_at", requested_at)
                quote = self._build_quote(
                    token_in=token_in,
                    token_out=token_out,
                    response=cached_result,
                    requested_at=requested_at,
                    received_at=received_at,
                    latency_ms=int(record.get("latency_ms") or 0),
                )
                if cache and self._is_cacheable(token_in, amount_usd):
                    self._set_cached_quote(payload, cache_ttl_seconds, quote)
                return quote
            if self._replay_mode:
                raise QuoteUnavailableError("replay data missing for quote request")

        requested_at = self._clock()
        result, received_at, latency_ms = await self._request_with_retry("/order", payload)
        quote = self._build_quote(
            token_in=token_in,
            token_out=token_out,
            response=result,
            requested_at=requested_at,
            received_at=received_at,
            latency_ms=latency_ms,
        )

        if cache and self._is_cacheable(token_in, amount_usd):
            self._set_cached_quote(payload, cache_ttl_seconds, quote)
        if self._quote_journal is not None and self._record_quotes and not self._replay_mode:
            self._quote_journal.record(
                request_key,
                result,
                requested_at=requested_at,
                received_at=received_at,
                latency_ms=latency_ms,
            )
        return quote

    def _build_quote(
        self,
        token_in: str,
        token_out: str,
        response: dict[str, Any],
        *,
        requested_at: datetime,
        received_at: datetime,
        latency_ms: int,
    ) -> QuoteResponse:
        if not response.get("outAmount"):
            raise QuoteUnavailableError("no executable route returned")

        try:
            in_amount = Decimal(str(response["inAmount"]))
            out_amount = Decimal(str(response["outAmount"]))
        except (KeyError, ValueError, TypeError) as exc:
            raise QuoteUnavailableError(
                "invalid amount format from Jupiter"
            ) from exc
        if (
            not in_amount.is_finite()
            or not out_amount.is_finite()
            or in_amount <= 0
            or out_amount <= 0
        ):
            raise QuoteUnavailableError(
                "Jupiter returned non-positive or non-finite amounts"
            )
        in_usdc = max(
            Decimal("0"),
            self._pick_amount_usd(
            response,
            "inUsdValue",
            "inAmountUsd",
            "inAmountUSD",
            "inAmountPriceUsd",
                "inAmountValueUsd",
            ),
        )
        out_usdc = max(
            Decimal("0"),
            self._pick_amount_usd(
            response,
            "outUsdValue",
            "outAmountUsd",
            "outAmountUSD",
                "outAmountValueUsd",
            ),
        )
        expires_at = received_at + timedelta(seconds=1.5)
        expire_at = response.get("expireAt")
        if expire_at:
            try:
                parsed = datetime.fromisoformat(str(expire_at).replace("Z", "+00:00"))
                expires_at = min(expires_at, parsed.astimezone(timezone.utc))
            except ValueError:
                pass
        price_impact = self._price_impact_fraction(response)
        platform_fee = response.get("platformFee") or {}
        platform_fee_usd = max(
            Decimal("0"),
            self._pick_amount_usd(platform_fee, "usdValue", "amountUsd"),
        )
        network_fee_lamports = sum(
            max(Decimal("0"), Decimal(str(response.get(key) or 0)))
            for key in (
                "signatureFeeLamports",
                "prioritizationFeeLamports",
                "rentFeeLamports",
            )
        )

        quote = QuoteResponse(
            request_id=str(response.get("requestId") or ""),
            requested_at=requested_at,
            received_at=received_at,
            latency_ms=latency_ms,
            token_in=token_in,
            token_out=token_out,
            in_amount=in_amount,
            out_amount=out_amount,
            in_amount_usd=in_usdc,
            out_amount_usd=out_usdc,
            route=(
                response["route"]
                if isinstance(response.get("route"), dict)
                else {"routePlan": response.get("routePlan", [])}
            ),
            price_impact_pct=price_impact,
            platform_fee_usd=platform_fee_usd,
            estimated_network_fee_usd=(
                network_fee_lamports
                / Decimal("1000000000")
                * self._last_sol_usd_price
            ),
            expires_at=expires_at,
            raw=response,
        )

        if quote.is_stale(self._clock()):
            raise QuoteStaleError("quote is stale")
        return quote

    async def get_buy_quote(self, quote_token: str, token: str, usdc_amount: Decimal) -> QuoteResponse:
        return await self.get_quote(
            token_in=quote_token,
            token_out=token,
            amount_usd=usdc_amount,
            cache=False,
            force_no_taker=True,
        )

    async def get_sell_quote(self, token: str, quote_token: str, token_amount: Decimal) -> QuoteResponse:
        return await self.get_quote(
            token_in=token,
            token_out=quote_token,
            amount_usd=token_amount,
            cache=False,
            force_no_taker=True,
        )

    async def get_sell_quote_mark_to_market(
        self,
        token: str,
        quote_token: str,
        token_amount: Decimal,
    ) -> QuoteResponse:
        # Conservative mark-to-market path can use cache for same quote inputs.
        return await self.get_quote(
            token_in=token,
            token_out=quote_token,
            amount_usd=token_amount,
            cache=True,
            force_no_taker=True,
            cache_ttl_seconds=2.0,
        )

    async def get_round_trip_quote(
        self,
        *,
        quote_token: str,
        token: str,
        usdc_amount: Decimal,
    ) -> RoundTripQuote:
        buy = await self.get_buy_quote(quote_token, token, usdc_amount)
        sell = await self.get_sell_quote(token, quote_token, buy.out_amount)
        starting = buy.in_amount_usd if buy.in_amount_usd and buy.in_amount_usd > 0 else usdc_amount
        ending = sell.out_amount_usd if sell.out_amount_usd and sell.out_amount_usd > 0 else sell.out_amount
        loss = max(Decimal("0"), Decimal("1") - ending / starting) if starting > 0 else Decimal("1")
        return RoundTripQuote(
            buy=buy,
            sell=sell,
            starting_usd=starting,
            ending_usd=ending,
            loss_pct=loss,
        )

    async def get_sol_usd_price(self, sol_mint: str, usdc_mint: str) -> Decimal:
        quote = await self.get_quote(
            token_in=sol_mint,
            token_out=usdc_mint,
            amount_usd=Decimal("1000000000"),
            cache=True,
            cache_ttl_seconds=5.0,
        )
        if quote.out_amount_usd and quote.out_amount_usd > 0:
            price = quote.out_amount_usd
        else:
            price = quote.out_amount / Decimal("1000000")
        self._last_sol_usd_price = price
        return price

    @staticmethod
    def _pick_amount_usd(data: dict[str, Any], *keys: str) -> Decimal:
        for key in keys:
            value = data.get(key)
            if value is None:
                continue
            try:
                return Decimal(str(value))
            except Exception:
                continue
        return Decimal("0")

    @staticmethod
    def _price_impact_fraction(data: dict[str, Any]) -> Decimal:
        if data.get("priceImpact") is not None:
            return abs(Decimal(str(data["priceImpact"]))) / Decimal("100")
        return abs(Decimal(str(data.get("priceImpactPct", "0"))))

    def _cache_key(self, payload: dict[str, Any]) -> str:
        return "|".join(
            [
                str(payload.get("inputMint", "")),
                str(payload.get("outputMint", "")),
                str(payload.get("amount", "")),
                str(payload.get("slippageBps", "")),
            ]
        )

    def _quote_request_key(self, payload: dict[str, Any]) -> str:
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()

    def _is_cacheable(self, token_in: str, amount_usd: Decimal) -> bool:
        if self._cache_seconds <= 0:
            return False
        if amount_usd < self._cache_floor_usd:
            return False
        return self._quote_mint is not None and token_in == self._quote_mint

    def _get_cached_quote(self, payload: dict[str, Any], cache_ttl_seconds: float | None) -> QuoteResponse | None:
        key = self._cache_key(payload)
        cache_entry = self._quote_cache.get(key)
        if cache_entry is None:
            return None
        cached_at, quote = cache_entry
        ttl = cache_ttl_seconds if cache_ttl_seconds is not None else self._cache_seconds
        if (self._clock() - cached_at).total_seconds() > ttl:
            self._quote_cache.pop(key, None)
            return None
        return quote

    def _set_cached_quote(self, payload: dict[str, Any], cache_ttl_seconds: float | None, quote: QuoteResponse) -> None:
        self._quote_cache[self._cache_key(payload)] = (
            self._clock(),
            quote,
        )
        # Respect TTL policy by pruning at write-time only; stale reads are filtered lazily.
        if cache_ttl_seconds and cache_ttl_seconds <= 0:
            self._quote_cache.pop(self._cache_key(payload), None)

    def _to_amount_int(self, amount: Decimal, is_quote_mint: bool = True) -> str:
        # For quote side we treat values as USDC-like or SOL-like decimal inputs (6 decimals for USDC/WSOL).
        if amount <= 0:
            raise ValueError("amount must be positive")
        if is_quote_mint:
            multiplier = Decimal(10) ** Decimal(max(0, self._quote_mint_decimals))
        else:
            multiplier = Decimal("1")
        return str(int(amount * multiplier))

    async def _request_with_retry(
        self, path: str, payload: dict[str, Any]
    ) -> tuple[dict[str, Any], datetime, int]:
        last_error: Exception | None = None
        for attempt in range(self._max_retries + 1):
            await self._enforce_rate()
            requested_at = self._clock()
            started = time.perf_counter()
            try:
                async with httpx.AsyncClient(timeout=self._timeout_seconds) as client:
                    response = await client.get(
                        f"{self._base_url}{path}",
                        params=payload,
                        headers={"x-api-key": self._api_key},
                    )
                latency_ms = int((time.perf_counter() - started) * 1000)
                received_at = self._clock()
                if response.status_code == 429 or response.status_code >= 500:
                    await self._record_call(
                        path, payload, None, requested_at, received_at,
                        latency_ms, response.status_code, f"HTTP_{response.status_code}",
                    )
                    if attempt < self._max_retries:
                        await self._sleep_with_backoff(attempt)
                        continue
                    raise RateLimitExceededError(
                        f"Jupiter transient quote failure status={response.status_code}"
                    )
                if response.status_code != 200:
                    await self._record_call(
                        path, payload, None, requested_at, received_at,
                        latency_ms, response.status_code, f"HTTP_{response.status_code}",
                    )
                    raise QuoteUnavailableError(
                        f"Jupiter quote rejected status={response.status_code}"
                    )
                try:
                    data = response.json()
                except (TypeError, ValueError) as exc:
                    await self._record_call(
                        path,
                        payload,
                        None,
                        requested_at,
                        received_at,
                        latency_ms,
                        response.status_code,
                        "INVALID_RESPONSE",
                    )
                    raise QuoteUnavailableError(
                        "invalid response format from Jupiter"
                    ) from exc
                if not isinstance(data, dict):
                    await self._record_call(
                        path,
                        payload,
                        None,
                        requested_at,
                        received_at,
                        latency_ms,
                        response.status_code,
                        "INVALID_RESPONSE",
                    )
                    raise QuoteUnavailableError(
                        "invalid response format from Jupiter"
                    )
                if data.get("error") and not data.get("outAmount"):
                    await self._record_call(
                        path, payload, data, requested_at, received_at,
                        latency_ms, 200, "NO_ROUTE",
                    )
                    raise QuoteUnavailableError("no executable route returned")
                await self._record_call(
                    path, payload, data, requested_at, received_at,
                    latency_ms, 200, None,
                )
                return data, received_at, latency_ms
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                last_error = exc
                received_at = self._clock()
                latency_ms = int((time.perf_counter() - started) * 1000)
                await self._record_call(
                    path,
                    payload,
                    None,
                    requested_at,
                    received_at,
                    latency_ms,
                    0,
                    type(exc).__name__.upper(),
                )
                if attempt < self._max_retries:
                    await self._sleep_with_backoff(attempt)
                    continue
                break
        raise RateLimitExceededError("Jupiter quote retries exhausted") from last_error

    async def _record_call(
        self,
        path: str,
        request: dict[str, Any],
        response: dict[str, Any] | None,
        requested_at: datetime,
        received_at: datetime,
        latency_ms: int,
        status: int,
        error_code: str | None,
    ) -> None:
        if self._metrics is not None:
            label = "ok" if status == 200 and error_code is None else (error_code or f"http_{status}").lower()
            self._metrics.jupiter_requests.labels(status=label).inc()
            self._metrics.jupiter_latency_ms.observe(latency_ms)
            if error_code == "NO_ROUTE":
                self._metrics.jupiter_no_route.inc()
        if self._recorder is None:
            return
        await self._recorder(
            provider="jupiter",
            endpoint=path,
            request_json=request,
            response_json=response,
            requested_at=requested_at,
            received_at=received_at,
            latency_ms=latency_ms,
            http_status=status,
            error_code=error_code,
        )

    async def _enforce_rate(self) -> None:
        async with self._rate_lock:
            now = datetime.now(tz=timezone.utc)
            delay = self._rate_window - (now - self._last_call).total_seconds()
            if delay > 0:
                await asyncio.sleep(delay)
            self._last_call = datetime.now(tz=timezone.utc)

    async def _sleep_with_backoff(self, attempt: int) -> None:
        await asyncio.sleep((2**attempt) * 0.25)
