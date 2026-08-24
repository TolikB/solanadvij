from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from sniper_bot.config import AppConfig
from sniper_bot.jupiter import JupiterQuoteProvider
from sniper_bot.quote_journal import QuoteJournal
from sniper_bot.replay import ReplayAction, ReplayRunner
from sniper_bot.runtime import SniperRuntime


def _base_config(*, mode: str, replay_seed: int | None = 17, time_zone: str = "Europe/Kyiv") -> dict[str, object]:
    return {
        "APP_MODE": mode,
        "HELIUS_API_KEY": "helius-key",
        "JUPITER_API_KEY": "jupiter-key",
        "POSTGRES_DSN": "postgresql://user:pass@localhost:5432/db",
        "TELEGRAM_BOT_TOKEN": "telegram-token",
        "TELEGRAM_ADMIN_CHAT_ID": 123456,
        "STARTING_EQUITY_USD": Decimal("500"),
        "TIME_ZONE": time_zone,
        "BASE_QUOTE_MINT": "SOL",
        "BASE_QUOTE_DECIMALS": 6,
        "REPLAY_SEED": replay_seed,
    }


def _quote_request_key(provider: JupiterQuoteProvider, token_in: str, token_out: str, amount: Decimal) -> str:
    payload = {
        "inputMint": token_in,
        "outputMint": token_out,
        "amount": provider._to_amount_int(amount, is_quote_mint=token_in == (provider._quote_mint or "")),
        "slippageBps": 120,
        "swapMode": "ExactIn",
        "onlyDirectRoutes": False,
    }
    return provider._quote_request_key(payload)


def _seed_jupiter_quotes(journal_path: Path) -> None:
    provider = JupiterQuoteProvider("key", quote_mint="SOL", quote_mint_decimals=6)
    journal = QuoteJournal(str(journal_path))

    open_key = _quote_request_key(provider, "SOL", "TOKEN", Decimal("20"))
    journal.record(
        open_key,
        {
            "inAmount": "20000000",
            "outAmount": "50",
            "inAmountUsd": "20",
            "outAmountUsd": "20",
            "priceImpactPct": "0.012",
            "route": {"label": "replay-open"},
            "timeTaken": 60000,
        },
    )

    close_key = _quote_request_key(provider, "TOKEN", "SOL", Decimal("49"))
    journal.record(
        close_key,
        {
            "inAmount": "49",
            "outAmount": "60",
            "inAmountUsd": "49",
            "outAmountUsd": "60",
            "priceImpactPct": "0.012",
            "route": {"label": "replay-close"},
            "timeTaken": 60000,
        },
    )


def _build_replay_runtime(tmp_path: Path, *, strategy_seed: int = 17, time_zone: str = "Europe/Kyiv") -> SniperRuntime:
    journal_path = tmp_path / "quotes.ndjson"
    _seed_jupiter_quotes(journal_path)
    config = AppConfig(
        **{
            **_base_config(mode="paper", replay_seed=strategy_seed, time_zone=time_zone),
            "JUPITER_REPLAY_MODE": True,
            "JUPITER_QUOTE_JOURNAL_PATH": str(journal_path),
            "TELEGRAM_BOT_TOKEN": "telegram-token",
        }
    )
    return SniperRuntime(config, data_dir=tmp_path / f"runtime-{strategy_seed}-{time_zone}")


def _replay_actions() -> list[ReplayAction]:
    return [
        ReplayAction(kind="open", token_mint="TOKEN", amount="20", order_id="open-1"),
        ReplayAction(kind="close", token_mint="TOKEN", amount="49", order_id="close-1"),
    ]


def _to_str_map(values: dict[str, object]) -> dict[str, str]:
    return {key: str(value) for key, value in values.items()}


@pytest.mark.asyncio
async def test_replay_matches_golden_artifact(tmp_path: Path) -> None:
    fixture = json.loads(
        (Path(__file__).parent / "fixtures" / "golden_replay_stage16.json")
        .read_text(encoding="utf-8")
    )

    runtime = _build_replay_runtime(tmp_path / "run", strategy_seed=fixture["strategy_seed"])
    result = await ReplayRunner(runtime).run(
        [ReplayAction(**action) for action in fixture["actions"]]
    )

    assert runtime.config.strategy_version == fixture["strategy_version"]
    assert runtime.config.config_hash == fixture["config_hash"]
    assert result.input_hash == fixture["input_hash"]
    assert result.output_hash == fixture["output_hash"]
    assert _to_str_map(result.final_reconcile) == fixture["final_reconcile"]
