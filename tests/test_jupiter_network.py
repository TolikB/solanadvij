from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest

from sniper_bot.errors import (
    QuoteStaleError,
    QuoteUnavailableError,
    RateLimitExceededError,
)
from sniper_bot.jupiter import JupiterQuoteProvider


class _Response:
    def __init__(self, status_code: int, payload: object) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> object:
        if isinstance(self._payload, BaseException):
            raise self._payload
        return self._payload


def _install_client(
    monkeypatch: pytest.MonkeyPatch,
    outcomes: list[object],
    calls: list[dict[str, object]],
) -> None:
    pending = list(outcomes)

    class _Client:
        async def __aenter__(self) -> "_Client":
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def get(
            self,
            url: str,
            *,
            params: dict[str, Any],
            headers: dict[str, str],
        ) -> _Response:
            calls.append(
                {
                    "method": "GET",
                    "url": url,
                    "params": params,
                    "headers": headers,
                }
            )
            outcome = pending.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            assert isinstance(outcome, _Response)
            return outcome

    factory: Callable[..., _Client] = lambda *_args, **_kwargs: _Client()
    monkeypatch.setattr("sniper_bot.jupiter.httpx.AsyncClient", factory)


def _quote_payload(**updates: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "requestId": "request",
        "inAmount": "1000000",
        "outAmount": "2500000",
        "inAmountUsd": "1",
        "outAmountUsd": "0.98",
        "priceImpactPct": "0.01",
        "route": {"label": "quote-only"},
        "timeTaken": 1000,
    }
    payload.update(updates)
    return payload


def _provider(*, retries: int = 2) -> JupiterQuoteProvider:
    provider = JupiterQuoteProvider(
        "test-key",
        quote_mint="USDC",
        quote_mint_decimals=6,
        max_retries=retries,
        rate_limit_per_second=1_000_000,
    )
    provider._enforce_rate = AsyncMock()
    provider._sleep_with_backoff = AsyncMock()
    return provider


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [429, 503])
async def test_transient_http_status_retries_then_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    status: int,
) -> None:
    calls: list[dict[str, object]] = []
    _install_client(
        monkeypatch,
        [_Response(status, {}), _Response(status, {})],
        calls,
    )
    provider = _provider(retries=1)

    with pytest.raises(RateLimitExceededError, match="transient quote failure"):
        await provider.get_quote("USDC", "TOKEN", Decimal("1"))

    assert len(calls) == 2
    assert provider._sleep_with_backoff.await_count == 1


@pytest.mark.asyncio
async def test_transient_5xx_recovers_on_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []
    _install_client(
        monkeypatch,
        [_Response(500, {}), _Response(200, _quote_payload())],
        calls,
    )
    provider = _provider(retries=1)

    quote = await provider.get_quote("USDC", "TOKEN", Decimal("1"))

    assert quote.out_amount == Decimal("2500000")
    assert len(calls) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_factory",
    [
        lambda: httpx.ReadTimeout(
            "timeout",
            request=httpx.Request("GET", "https://api.jup.ag"),
        ),
        lambda: httpx.ConnectError(
            "network",
            request=httpx.Request("GET", "https://api.jup.ag"),
        ),
    ],
)
async def test_network_failures_exhaust_bounded_retries(
    monkeypatch: pytest.MonkeyPatch,
    error_factory: Callable[[], Exception],
) -> None:
    calls: list[dict[str, object]] = []
    _install_client(
        monkeypatch,
        [error_factory(), error_factory(), error_factory()],
        calls,
    )
    provider = _provider(retries=2)

    with pytest.raises(RateLimitExceededError, match="retries exhausted"):
        await provider.get_quote("USDC", "TOKEN", Decimal("1"))

    assert len(calls) == 3
    assert provider._sleep_with_backoff.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"error": "no route"},
        [],
        ValueError("invalid json"),
    ],
)
async def test_no_route_and_malformed_payloads_are_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    payload: object,
) -> None:
    calls: list[dict[str, object]] = []
    _install_client(monkeypatch, [_Response(200, payload)], calls)
    provider = _provider(retries=0)

    with pytest.raises(QuoteUnavailableError):
        await provider.get_quote("USDC", "TOKEN", Decimal("1"))

    assert len(calls) == 1


@pytest.mark.asyncio
async def test_quote_request_is_get_only_order_without_taker_or_execute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []
    _install_client(monkeypatch, [_Response(200, _quote_payload())], calls)
    provider = _provider(retries=0)

    await provider.get_quote("USDC", "TOKEN", Decimal("1"))

    assert calls[0]["method"] == "GET"
    assert str(calls[0]["url"]).endswith("/order")
    assert "execute" not in str(calls[0]["url"]).lower()
    params = calls[0]["params"]
    assert isinstance(params, dict)
    assert "taker" not in params


def test_stale_quote_is_rejected() -> None:
    now = datetime(2026, 9, 1, tzinfo=timezone.utc)
    provider = _provider(retries=0)
    provider.set_clock(lambda: now)

    with pytest.raises(QuoteStaleError, match="stale"):
        provider._build_quote(
            "USDC",
            "TOKEN",
            _quote_payload(
                expireAt=(now - timedelta(seconds=1)).isoformat()
            ),
            requested_at=now - timedelta(seconds=2),
            received_at=now - timedelta(seconds=2),
            latency_ms=10,
        )


def test_non_positive_amounts_are_not_executable() -> None:
    now = datetime(2026, 9, 1, tzinfo=timezone.utc)
    provider = _provider(retries=0)
    provider.set_clock(lambda: now)

    with pytest.raises(QuoteUnavailableError, match="non-positive"):
        provider._build_quote(
            "USDC",
            "TOKEN",
            _quote_payload(outAmount="0"),
            requested_at=now,
            received_at=now,
            latency_ms=1,
        )