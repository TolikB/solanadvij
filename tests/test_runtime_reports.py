from __future__ import annotations

from decimal import Decimal

import pytest

from sniper_bot.config import AppConfig
from sniper_bot.runtime import SniperRuntime


def _base_config(mode: str) -> dict[str, object]:
    return {
        "APP_MODE": mode,
        "HELIUS_API_KEY": "helius-key",
        "JUPITER_API_KEY": "jupiter-key",
        "POSTGRES_DSN": "postgresql://user:pass@localhost:5432/db",
        "TELEGRAM_BOT_TOKEN": "telegram-token",
        "TELEGRAM_ADMIN_CHAT_ID": 123456,
        "STARTING_EQUITY_USD": Decimal("500"),
    }


def test_reports_build_with_no_trades(tmp_path) -> None:
    runtime = SniperRuntime(AppConfig(**_base_config("paper")), data_dir=tmp_path)

    daily = runtime.build_daily_report("2026-08-18")
    all_time = runtime.build_all_time_report()

    assert daily["period"] == "daily"
    assert all_time["period"] == "all_time"
    assert daily["date"] == "2026-08-18"
    assert all_time["date"] == "all_time"
    assert daily["equity_usd"] == "500"
    assert daily["reconcile"]["is_reconciled"] is True
    assert daily["sample_size_warning"] is True
    assert daily["open_positions"] == []
    assert daily["reconcile"]["equity_usd"] == all_time["reconcile"]["equity_usd"]
    assert daily["reconcile"]["realized_pnl_usd"] == all_time["reconcile"]["realized_pnl_usd"]
    assert daily["reconcile"]["expected_equity_usd"] == "500"
    assert daily["pnl"] == "0"
    assert all_time["pnl"] == "0"
    assert all_time["period"] != daily["period"]


def test_daily_report_matches_ledger_reconcile_in_realized_pnl(tmp_path) -> None:
    runtime = SniperRuntime(AppConfig(**_base_config("paper")), data_dir=tmp_path)
    # create one entry + one close fill to produce realized PnL and verify report aggregation
    runtime.ledger.open_position(
        token_mint="TOKEN",
        usd_amount=Decimal("10"),
        token_amount=Decimal("2"),
        order_id="entry-1",
        quote_id="quote-entry-1",
    )
    runtime.ledger.close_position(
        token_mint="TOKEN",
        usd_received=Decimal("11"),
        token_closed=Decimal("2"),
        order_id="exit-1",
        quote_id="quote-exit-1",
    )

    reconcile = runtime.ledger.reconcile()
    daily = runtime.build_daily_report()

    assert daily["period"] == "daily"
    assert daily["pnl"] == str(reconcile["realized_pnl_usd"])
    assert daily["reconcile"]["is_reconciled"] is True
    assert daily["reconcile"]["realized_pnl_usd"] == str(reconcile["realized_pnl_usd"])


@pytest.mark.asyncio
async def test_daily_report_if_not_sent_is_idempotent(tmp_path) -> None:
    runtime = SniperRuntime(AppConfig(**_base_config("record")), data_dir=tmp_path)

    runtime._today_key = lambda: "2026-08-18"  # type: ignore[method-assign]

    first = await runtime.daily_report_if_not_sent(date="2026-08-18")
    second = await runtime.daily_report_if_not_sent(date="2026-08-18")

    assert first is not None
    assert second is None
    assert runtime._report_runs["last_daily_report_date"] == "2026-08-18"
    assert runtime._report_runs["last_daily_report_id"] == str(first["report_id"])


@pytest.mark.asyncio
async def test_daily_report_direct_delivery_retries_before_marking_sent(tmp_path) -> None:
    class FlakyNotifier:
        def __init__(self) -> None:
            self.calls = 0
            self.messages: list[str] = []

        async def send(self, message: str) -> None:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("temporary Telegram failure")
            self.messages.append(message)

    runtime = SniperRuntime(AppConfig(**_base_config("record")), data_dir=tmp_path)
    notifier = FlakyNotifier()
    runtime.notifier = notifier
    runtime._today_key = lambda: "2026-08-18"  # type: ignore[method-assign]

    first = await runtime.daily_report_if_not_sent(date="2026-08-18", send=True)

    assert first is None
    assert runtime._report_runs.get("last_daily_report_date") != "2026-08-18"

    second = await runtime.daily_report_if_not_sent(date="2026-08-18", send=True)

    assert second is not None
    assert notifier.calls == 2
    assert notifier.messages[0].startswith("Щоденний звіт про тестову торгівлю\n")
    assert "report_id" not in notifier.messages[0]

@pytest.mark.asyncio
async def test_missing_historical_snapshot_notifies_once_and_allows_backfill(tmp_path) -> None:
    class MissingHistoricalSnapshotDatabase:
        def __init__(self) -> None:
            self.bounds: dict[str, object] | None = None
            self.lookup_error = False
            self.outbox_calls: list[dict[str, object]] = []
            self.store_calls: list[dict[str, object]] = []

        async def load_daily_equity_bounds(self, **_kwargs: object) -> dict[str, object] | None:
            if self.lookup_error:
                raise RuntimeError("lookup failed")
            return self.bounds

        async def enqueue_outbox(self, **kwargs: object) -> bool:
            self.outbox_calls.append(kwargs)
            return len(self.outbox_calls) == 1

        async def store_daily_report(self, **kwargs: object) -> bool:
            self.store_calls.append(kwargs)
            return True

    runtime = SniperRuntime(AppConfig(**_base_config("paper")), data_dir=tmp_path)
    database = MissingHistoricalSnapshotDatabase()
    runtime.database = database  # type: ignore[assignment]
    runtime.database_available = True
    runtime.config.telegram.include_all_time_with_daily = False
    runtime._today_key = lambda: "2026-08-19"  # type: ignore[method-assign]

    database.lookup_error = True
    lookup_failure = await runtime.daily_report_if_not_sent(date="2026-08-18")
    database.lookup_error = False
    first = await runtime.daily_report_if_not_sent(date="2026-08-18", send=True)
    second = await runtime.daily_report_if_not_sent(date="2026-08-18", send=True)

    assert lookup_failure is not None
    assert lookup_failure["data_status"] == "unavailable"
    assert first is not None
    assert first["equity_usd"] is None
    assert first["pnl"] is None
    assert first["open_positions"] is None
    assert first["reconcile"] == {
        "is_reconciled": False,
        "reason": "historical_equity_snapshot_unavailable",
    }
    assert second is None
    assert len(database.outbox_calls) == 2
    assert database.outbox_calls[0]["payload"] == {
        "text": (
            "Щоденний звіт про тестову торгівлю\n"
            "Дата: 2026-08-18\n"
            "Дані за цей день недоступні."
        )
    }
    assert database.store_calls == []

    database.bounds = {
        "starting_equity_usd": "500",
        "ending_equity_usd": "500",
        "starting_unrealized_pnl_usd": "0",
        "ending_unrealized_pnl_usd": "0",
        "equity_path_usd": ["500"],
    }
    recovered = await runtime.daily_report_if_not_sent(date="2026-08-18", send=True)

    assert recovered is not None
    assert "data_status" not in recovered
    assert len(database.store_calls) == 1
    assert database.store_calls[0]["report"] == recovered
