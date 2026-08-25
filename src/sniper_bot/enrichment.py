"""Rate-limited Dexscreener metadata enrichment; never a signal source."""

from __future__ import annotations

import asyncio
import math
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

import httpx
from pydantic import BaseModel, Field

from .external_journal import ExternalJournal


class TokenEnrichment(BaseModel):
    mint: str
    name: str | None = None
    symbol: str | None = None
    image_url: str | None = None
    website_url: str | None = None
    socials: list[dict[str, Any]] = Field(default_factory=list)
    pair_url: str | None = None
    pair_address: str | None = None
    dex_id: str | None = None
    price_usd: str | None = None
    boosts_active: int = 0
    observed_at: datetime


class DexscreenerClient:
    def __init__(
        self,
        *,
        base_url: str = "https://api.dexscreener.com",
        timeout_seconds: float = 2.5,
        minimum_interval_seconds: float = 0.25,
        cache_seconds: int = 300,
        replay_mode: bool = False,
        journal: ExternalJournal | None = None,
        recorder: Callable[..., Awaitable[object]] | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_seconds
        self._interval = minimum_interval_seconds
        self._cache_seconds = cache_seconds
        self._replay_mode = replay_mode
        self._journal = journal
        self._recorder = recorder
        self._lock = asyncio.Lock()
        self._last_call = 0.0
        self._cache: dict[str, TokenEnrichment] = {}

    async def get_token(self, mint: str) -> TokenEnrichment:
        cached = self._cache.get(mint)
        now = datetime.now(tz=timezone.utc)
        if cached and now - cached.observed_at <= timedelta(seconds=self._cache_seconds):
            return cached
        endpoint = f"/latest/dex/tokens/{mint}"
        key = ExternalJournal.key("dexscreener", endpoint, {"mint": mint})
        stored = self._journal.get(key) if self._journal else None
        if stored is not None:
            payload = stored.get("response")
            observed_at = datetime.fromisoformat(str(stored["observed_at"]).replace("Z", "+00:00"))
        elif self._replay_mode:
            raise RuntimeError("replay data missing for Dexscreener request")
        else:
            async with self._lock:
                delay = self._interval - (time.monotonic() - self._last_call)
                if delay > 0:
                    await asyncio.sleep(delay)
                requested_at = datetime.now(tz=timezone.utc)
                started = time.perf_counter()
                async with httpx.AsyncClient(timeout=self._timeout) as client:
                    response = await client.get(f"{self._base_url}{endpoint}")
                self._last_call = time.monotonic()
            latency_ms = int((time.perf_counter() - started) * 1000)
            observed_at = datetime.now(tz=timezone.utc)
            if response.status_code != 200:
                if self._recorder:
                    await self._recorder(
                        provider="dexscreener", endpoint=endpoint,
                        request_json={"mint": mint}, response_json=None,
                        requested_at=requested_at, received_at=observed_at,
                        latency_ms=latency_ms, http_status=response.status_code,
                        error_code=f"HTTP_{response.status_code}",
                    )
                raise RuntimeError(f"Dexscreener enrichment failed status={response.status_code}")
            payload = response.json()
            if self._journal:
                self._journal.record(key, payload, observed_at=observed_at)
            if self._recorder:
                await self._recorder(
                    provider="dexscreener", endpoint=endpoint,
                    request_json={"mint": mint}, response_json=payload,
                    requested_at=requested_at, received_at=observed_at,
                    latency_ms=latency_ms, http_status=200, error_code=None,
                )
        if not isinstance(payload, dict):
            raise RuntimeError("Dexscreener enrichment returned invalid payload")
        raw_pairs = payload.get("pairs")
        if raw_pairs is None:
            raw_pairs = []
        if not isinstance(raw_pairs, list):
            raise RuntimeError("Dexscreener enrichment returned invalid pairs")
        pairs = [
            item
            for item in raw_pairs
            if isinstance(item, dict) and item.get("chainId") == "solana"
        ]
        pair: dict[str, Any] = max(
            pairs,
            key=_pair_liquidity_usd,
            default={},
        )
        raw_info = pair.get("info")
        info = raw_info if isinstance(raw_info, dict) else {}
        raw_websites = info.get("websites")
        websites = (
            [item for item in raw_websites if isinstance(item, dict)]
            if isinstance(raw_websites, list)
            else []
        )
        raw_socials = info.get("socials")
        socials = (
            [item for item in raw_socials if isinstance(item, dict)]
            if isinstance(raw_socials, list)
            else []
        )
        raw_base_token = pair.get("baseToken")
        base_token = raw_base_token if isinstance(raw_base_token, dict) else {}
        raw_boosts = pair.get("boosts")
        boosts = raw_boosts if isinstance(raw_boosts, dict) else {}
        enrichment = TokenEnrichment(
            mint=mint,
            name=base_token.get("name"),
            symbol=base_token.get("symbol"),
            image_url=info.get("imageUrl") or info.get("header"),
            website_url=websites[0].get("url") if websites else None,
            socials=socials,
            pair_url=pair.get("url"), pair_address=pair.get("pairAddress"),
            dex_id=pair.get("dexId"), price_usd=pair.get("priceUsd"),
            boosts_active=_non_negative_int(boosts.get("active")),
            observed_at=observed_at,
        )
        self._cache[mint] = enrichment
        return enrichment


def _pair_liquidity_usd(pair: dict[str, Any]) -> float:
    raw_liquidity = pair.get("liquidity")
    if not isinstance(raw_liquidity, dict):
        return 0.0
    try:
        liquidity = float(raw_liquidity.get("usd") or 0)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, liquidity) if math.isfinite(liquidity) else 0.0


def _non_negative_int(value: object) -> int:
    if not isinstance(value, (str, bytes, bytearray, int, float)):
        return 0
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return 0
    return max(0, parsed)
