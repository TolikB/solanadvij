from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from sniper_bot.config import AppConfig
from sniper_bot.jupiter import JupiterQuoteProvider
from sniper_bot.quote_journal import QuoteJournal
from sniper_bot.replay import ReplayAction, ReplayRunner, ReplayRunStore, VirtualClock


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


def _build_replay_runtime(tmp_path: Path, *, strategy_seed: int = 17, time_zone: str = "Europe/Kyiv") -> object:
    journal_path = tmp_path / "quotes.ndjson"
    config = AppConfig(
        **{
            **_base_config(mode="paper", replay_seed=strategy_seed, time_zone=time_zone),
            "JUPITER_REPLAY_MODE": True,
            "JUPITER_QUOTE_JOURNAL_PATH": str(journal_path),
            "TELEGRAM_BOT_TOKEN": "telegram-token",
        }
    )
    from sniper_bot.runtime import SniperRuntime

    _seed_jupiter_quotes(journal_path)
    return SniperRuntime(config, data_dir=tmp_path / f"runtime-{strategy_seed}-{time_zone}")


def _replay_actions() -> list[ReplayAction]:
    return [
        ReplayAction(kind="open", token_mint="TOKEN", amount="20", order_id="open-1"),
        ReplayAction(kind="close", token_mint="TOKEN", amount="49", order_id="close-1"),
    ]


@pytest.mark.asyncio
async def test_replay_run_is_deterministic_for_same_seed(tmp_path: Path) -> None:
    runtime_first = _build_replay_runtime(tmp_path / "run1", strategy_seed=42)
    runtime_second = _build_replay_runtime(tmp_path / "run2", strategy_seed=42)

    clock = VirtualClock()
    result_first = await ReplayRunner(runtime_first, clock=clock).run(_replay_actions())
    result_second = await ReplayRunner(runtime_second, clock=clock).run(_replay_actions())

    assert result_first.input_hash == result_second.input_hash
    assert result_first.output_hash == result_second.output_hash
    assert result_first.final_reconcile["realized_pnl_usd"] == result_second.final_reconcile["realized_pnl_usd"]


@pytest.mark.asyncio
async def test_replay_run_records_by_strategy_version_and_seed(tmp_path: Path) -> None:
    journal_path = tmp_path / "quotes.ndjson"
    _seed_jupiter_quotes(journal_path)
    store = ReplayRunStore(tmp_path / "replay_runs.json")

    config_v1 = AppConfig(
        **{
            **_base_config(mode="paper", replay_seed=17, time_zone="Europe/Kyiv"),
            "JUPITER_REPLAY_MODE": True,
            "JUPITER_QUOTE_JOURNAL_PATH": str(journal_path),
        }
    )
    config_v2 = AppConfig(
        **{
            **_base_config(mode="paper", replay_seed=17, time_zone="Europe/Prague"),
            "JUPITER_REPLAY_MODE": True,
            "JUPITER_QUOTE_JOURNAL_PATH": str(journal_path),
        }
    )

    from sniper_bot.runtime import SniperRuntime

    runtime_v1 = SniperRuntime(config_v1, data_dir=tmp_path / "v1")
    runtime_v2 = SniperRuntime(config_v2, data_dir=tmp_path / "v2")

    result_v1 = await ReplayRunner(runtime_v1, store=store).run(_replay_actions())
    result_v2 = await ReplayRunner(runtime_v2, store=store).run(_replay_actions())

    assert result_v1.run_id != result_v2.run_id
    assert result_v1.output_hash == result_v2.output_hash
    assert result_v1.final_reconcile == result_v2.final_reconcile
    assert len(store.list_runs()) == 2
    assert store.get_run_by_signature(strategy_version=config_v1.strategy_version, input_hash=result_v1.input_hash) is not None
    assert store.get_run_by_signature(strategy_version=config_v1.strategy_version, input_hash=result_v2.input_hash) is None
    assert store.get_run_by_signature(strategy_version=config_v2.strategy_version, input_hash=result_v2.input_hash) is not None
