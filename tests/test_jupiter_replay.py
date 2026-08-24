from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from sniper_bot.errors import QuoteUnavailableError
from sniper_bot.jupiter import JupiterQuoteProvider
from sniper_bot.quote_journal import QuoteJournal


def _sample_quote_response() -> dict[str, Any]:
    return {
        "inAmount": "120000000",
        "outAmount": "48000000",
        "inAmountUsd": "120",
        "outAmountUsd": "47.8",
        "priceImpactPct": "0.012",
        "route": {"label": "unit-test"},
        "timeTaken": 60000,
    }


def _request_key(provider: JupiterQuoteProvider, token_in: str, token_out: str, amount_usd: Decimal) -> str:
    payload: dict[str, Any] = {
        "inputMint": token_in,
        "outputMint": token_out,
        "amount": provider._to_amount_int(amount_usd, is_quote_mint=token_in == (provider._quote_mint or "")),
        "slippageBps": 120,
        "swapMode": "ExactIn",
        "onlyDirectRoutes": False,
    }
    return provider._quote_request_key(payload)


@pytest.mark.asyncio
async def test_replay_mode_reads_cached_journal_quote_without_network(tmp_path: Path) -> None:
    journal_path = tmp_path / "quotes.ndjson"
    response = _sample_quote_response()

    seed_provider = JupiterQuoteProvider("key", quote_mint="SOL", quote_mint_decimals=6)
    key = _request_key(seed_provider, "SOL", "TOKEN", Decimal("120"))
    journal = QuoteJournal(str(journal_path))
    journal.record(key, response)

    provider = JupiterQuoteProvider(
        "key",
        quote_mint="SOL",
        quote_mint_decimals=6,
        replay_mode=True,
        quote_journal_path=str(journal_path),
    )

    provider._request_with_retry = AsyncMock(side_effect=RuntimeError("network should not be used in replay"))

    quote = await provider.get_quote("SOL", "TOKEN", Decimal("120"))

    assert quote.in_amount == Decimal(response["inAmount"])
    assert quote.out_amount == Decimal(response["outAmount"])
    assert quote.in_amount_usd == Decimal(response["inAmountUsd"])
    assert quote.out_amount_usd == Decimal(response["outAmountUsd"])
    assert quote.route == response["route"]
    assert provider._request_with_retry.call_count == 0


@pytest.mark.asyncio
async def test_replay_mode_without_journal_data_raises(tmp_path: Path) -> None:
    provider = JupiterQuoteProvider(
        "key",
        quote_mint="SOL",
        quote_mint_decimals=6,
        replay_mode=True,
        quote_journal_path=str(tmp_path / "empty-quotes.ndjson"),
    )
    provider._request_with_retry = AsyncMock(side_effect=RuntimeError("should not be called"))

    with pytest.raises(QuoteUnavailableError, match="replay data missing"):
        await provider.get_quote("SOL", "TOKEN", Decimal("120"))

    assert provider._request_with_retry.call_count == 0


@pytest.mark.asyncio
async def test_record_mode_persists_quote_for_replay(tmp_path: Path) -> None:
    journal_path = tmp_path / "quotes.ndjson"
    response = _sample_quote_response()

    record_provider = JupiterQuoteProvider(
        "key",
        quote_mint="SOL",
        quote_mint_decimals=6,
        record_quotes=True,
        quote_journal_path=str(journal_path),
    )
    now = datetime.now(tz=timezone.utc)
    record_provider._request_with_retry = AsyncMock(return_value=(response, now, 10))

    quote_first = await record_provider.get_quote("SOL", "TOKEN", Decimal("120"))
    assert quote_first.out_amount == Decimal(response["outAmount"])

    lines = [line for line in journal_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) == 1

    replay_provider = JupiterQuoteProvider(
        "key",
        quote_mint="SOL",
        quote_mint_decimals=6,
        replay_mode=True,
        quote_journal_path=str(journal_path),
    )
    replay_provider._request_with_retry = AsyncMock(side_effect=RuntimeError("network should not be used in replay"))

    quote_second = await replay_provider.get_quote("SOL", "TOKEN", Decimal("120"))

    assert quote_second.in_amount == Decimal(response["inAmount"])
    assert quote_second.out_amount == Decimal(response["outAmount"])
    assert replay_provider._request_with_retry.call_count == 0


def test_jupiter_provider_requires_journal_path_when_replay_or_record_enabled() -> None:
    with pytest.raises(ValueError, match="quote_journal_path is required"):
        JupiterQuoteProvider("key", quote_mint="SOL", quote_mint_decimals=6, replay_mode=True)
    with pytest.raises(ValueError, match="quote_journal_path is required"):
        JupiterQuoteProvider(
            "key",
            quote_mint="SOL",
            quote_mint_decimals=6,
            record_quotes=True,
        )


def test_quote_journal_preserves_repeated_request_sequence(tmp_path: Path) -> None:
    journal_path = tmp_path / "quotes.ndjson"
    first_at = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
    journal = QuoteJournal(str(journal_path))
    journal.record("same-key", {"sequence": 1}, requested_at=first_at, latency_ms=10)
    journal.record("same-key", {"sequence": 2}, requested_at=first_at, latency_ms=20)

    replay = QuoteJournal(str(journal_path))
    first = replay.get_record("same-key")
    second = replay.get_record("same-key")

    assert first is not None and first["response"] == {"sequence": 1}
    assert second is not None and second["response"] == {"sequence": 2}
    assert replay.get_record("same-key") is None
