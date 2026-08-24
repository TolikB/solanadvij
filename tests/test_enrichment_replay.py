from __future__ import annotations

from datetime import datetime, timezone

import pytest

from sniper_bot.enrichment import DexscreenerClient
from sniper_bot.external_journal import ExternalJournal


@pytest.mark.asyncio
async def test_dexscreener_replay_uses_journal_without_network(tmp_path, monkeypatch) -> None:
    mint = "TOKEN"
    endpoint = f"/latest/dex/tokens/{mint}"
    journal = ExternalJournal(tmp_path / "external.ndjson")
    key = journal.key("dexscreener", endpoint, {"mint": mint})
    journal.record(
        key,
        {
            "pairs": [
                {
                    "chainId": "solana", "pairAddress": "PAIR", "dexId": "pumpswap",
                    "baseToken": {"name": "Token", "symbol": "TOK"},
                    "liquidity": {"usd": 100000}, "priceUsd": "0.1",
                    "info": {"websites": [{"url": "https://example.test"}], "socials": []},
                }
            ]
        },
        observed_at=datetime(2026, 8, 18, tzinfo=timezone.utc),
    )

    def forbidden_client(*_args, **_kwargs):
        raise AssertionError("network must not be used in replay")

    monkeypatch.setattr("sniper_bot.enrichment.httpx.AsyncClient", forbidden_client)
    result = await DexscreenerClient(replay_mode=True, journal=journal).get_token(mint)

    assert result.pair_address == "PAIR"
    assert result.symbol == "TOK"


def test_external_journal_preserves_repeated_request_sequence(tmp_path) -> None:
    path = tmp_path / "external.ndjson"
    journal = ExternalJournal(path)
    journal.record("same-key", {"sequence": 1})
    journal.record("same-key", {"sequence": 2})

    replay = ExternalJournal(path)
    first = replay.get("same-key")
    second = replay.get("same-key")

    assert first is not None and first["response"] == {"sequence": 1}
    assert second is not None and second["response"] == {"sequence": 2}
    assert replay.get("same-key") is None
