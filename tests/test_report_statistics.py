from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

from sniper_bot.ledger import PaperLedger
from sniper_bot.reports import ReportBuilder


class _MetricValue:
    def get(self) -> int:
        return 0


class _Metric:
    _value = _MetricValue()


def test_reports_count_a_position_once_and_include_required_statistics(tmp_path) -> None:
    opened_at = datetime(2026, 8, 24, 12, tzinfo=timezone.utc)
    ledger = PaperLedger(
        tmp_path / "ledger.json",
        Decimal("500"),
        "strategy-v1",
        "config-hash",
    )
    ledger.open_position(
        "TOKEN",
        Decimal("100"),
        Decimal("10"),
        "open-order",
        "open-quote",
        position_id="position-1",
        fill_id="entry-fill",
        created_at=opened_at,
        entry_score=Decimal("88"),
        entry_liquidity_usd=Decimal("80000"),
        entry_pool_age_seconds=Decimal("100"),
        strategy_version="strategy-v1",
        price_impact_pct=Decimal("0.01"),
        other_cost_usd=Decimal("0.50"),
        adverse_fill_bps=50,
        quote_latency_ms=100,
        round_trip_cost_pct=Decimal("0.04"),
    )
    ledger.close_position(
        "TOKEN",
        Decimal("60"),
        Decimal("5"),
        "close-order-1",
        "close-quote-1",
        exit_reason="TP1",
        close_fill_id="exit-fill-1",
        created_at=opened_at + timedelta(seconds=30),
        price_impact_pct=Decimal("0.02"),
        other_cost_usd=Decimal("0.20"),
        adverse_fill_bps=50,
        quote_latency_ms=200,
    )
    ledger.close_position(
        "TOKEN",
        Decimal("40"),
        Decimal("5"),
        "close-order-2",
        "close-quote-2",
        exit_reason="TIME_STOP",
        close_fill_id="exit-fill-2",
        created_at=opened_at + timedelta(seconds=60),
        price_impact_pct=Decimal("0.03"),
        other_cost_usd=Decimal("0.30"),
        adverse_fill_bps=50,
        quote_latency_ms=300,
    )

    runtime = SimpleNamespace(
        ledger=ledger,
        config=SimpleNamespace(
            time_zone="Europe/Kyiv",
            starting_equity_usd=Decimal("500"),
            strategy_version="strategy-v1",
            config_hash="config-hash",
            reporting=SimpleNamespace(
                include_operational_costs=False,
                monthly_infrastructure_cost_usd=Decimal("0"),
                minimum_sample_warning_trades=30,
            ),
            paper=SimpleNamespace(adverse_fill_bps=50),
        ),
        pipeline=SimpleNamespace(candidates={}),
        metrics=SimpleNamespace(
            websocket_reconnects=_Metric(),
            websocket_gap_recoveries=_Metric(),
            jupiter_no_route=_Metric(),
        ),
        health_status=lambda: "HEALTHY",
    )
    builder = ReportBuilder(runtime)

    daily = builder.daily("2026-08-24")
    assert daily["trades"]["closed"] == 1
    assert daily["trades"]["average_holding_seconds"] == 60
    assert daily["trades"]["max_consecutive_wins"] == 0
    assert daily["trades"]["max_consecutive_losses"] == 0
    assert daily["capital"]["simulated_costs_usd"] == "1.00"
    assert daily["execution_quality"]["average_buy_impact_pct"] == "0.01"
    assert daily["execution_quality"]["average_sell_impact_pct"] == "0.025"

    historical = builder.daily(
        "2026-08-24",
        capital_bounds={
            "starting_equity_usd": Decimal("500"),
            "ending_equity_usd": Decimal("480"),
            "ending_unrealized_pnl_usd": Decimal("-20"),
            "equity_path_usd": [Decimal("500"), Decimal("510"), Decimal("480")],
        },
    )
    assert historical["capital"]["starting_equity_usd"] == "500"
    assert historical["capital"]["ending_equity_usd"] == "480"
    assert historical["capital"]["unrealized_pnl_usd"] == "-20"
    assert historical["capital"]["net_return_pct"] == "-0.04"
    assert Decimal(historical["max_intraday_drawdown_pct"]) == Decimal("30") / Decimal("510")

    all_time = builder.all_time(max_drawdown_pct=Decimal("0.125"))
    assert all_time["closed_trades"] == 1
    assert Decimal(all_time["score_buckets"]["85_89"]) == 0
    assert Decimal(all_time["liquidity_buckets"]["75k_150k"]) == 0
    assert Decimal(all_time["pool_age_buckets"]["76_120s"]) == 0
    assert all_time["max_drawdown_pct"] == "0.125"


def test_daily_risk_pnl_uses_configured_iana_timezone(tmp_path) -> None:
    ledger = PaperLedger(
        tmp_path / "timezone-ledger.json",
        Decimal("500"),
        "strategy-v1",
        "config-hash",
        time_zone="Europe/Kyiv",
    )
    opened_at = datetime(2026, 8, 24, 20, tzinfo=timezone.utc)
    ledger.open_position(
        "TOKEN",
        Decimal("10"),
        Decimal("1"),
        "open-order",
        "open-quote",
        created_at=opened_at,
    )
    ledger.close_position(
        "TOKEN",
        Decimal("9"),
        Decimal("1"),
        "close-order",
        "close-quote",
        created_at=opened_at + timedelta(hours=1, minutes=30),
    )

    assert ledger.daily_pnl("2026-08-24") == 0
    assert ledger.daily_pnl("2026-08-25") == Decimal("-1")
