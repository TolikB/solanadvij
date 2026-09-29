from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from sniper_bot.config import AppConfig, ScoreAnchor, ScoringConfig
from sniper_bot.features import FeatureSnapshot
from sniper_bot.ledger import PaperLedger
from sniper_bot.risk import RiskDecision, RiskManager
from sniper_bot.scoring import DeveloperHistory, ScoreContext, ScoringEngine
from sniper_bot.sizing import PositionSizingInput, calculate_position_size

NOW = datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)


def _legacy_total(context: ScoreContext) -> Decimal:
    """The pre-config scoring formula with its literal anchors."""

    def clamp(value: Decimal, low: Decimal, high: Decimal) -> Decimal:
        return max(low, min(value, high))

    def linear(value: Decimal, low: str, high: str, maximum: str) -> Decimal:
        lo, hi, mx = Decimal(low), Decimal(high), Decimal(maximum)
        return clamp((value - lo) / (hi - lo) * mx, Decimal("0"), mx)

    def inverse(value: Decimal, good: str, bad: str, maximum: str) -> Decimal:
        go, ba, mx = Decimal(good), Decimal(bad), Decimal(maximum)
        return clamp((ba - value) / (ba - go) * mx, Decimal("0"), mx)

    f = context.features
    organic = min(
        linear(Decimal(f.unique_buyers_60s), "5", "25", "8")
        + linear(f.buyer_acceleration, "0.8", "1.5", "6")
        + linear(f.unique_buyer_ratio, "0.2", "0.75", "4")
        + linear(f.buy_sell_volume_ratio, "1", "3", "4")
        + inverse(f.transactions_per_trader, "2", "6", "3"),
        Decimal("25"),
    )
    distribution = min(
        inverse(f.top_10_holders_pct, "0.15", "0.30", "6")
        + inverse(f.largest_related_cluster_pct, "0.05", "0.15", "6")
        + inverse(f.dev_cluster_holding_pct, "0.02", "0.05", "4")
        + inverse(f.top_5_buyer_volume_share, "0.35", "0.70", "4"),
        Decimal("20"),
    )
    execution = min(
        inverse(context.round_trip_loss_pct, "0.03", "0.08", "8")
        + inverse(
            max(context.buy_price_impact_pct, context.sell_price_impact_pct),
            "0.01",
            "0.035",
            "6",
        )
        + context.sell_route_reliability * Decimal("6"),
        Decimal("20"),
    )
    liquidity = min(
        linear(f.quote_liquidity_usd, "40000", "100000", "5")
        + linear(f.quote_liquidity_change_30s, "-0.03", "0", "6")
        + inverse(f.market_cap_to_quote_liquidity, "5", "15", "4"),
        Decimal("15"),
    )
    price = min(
        (Decimal("4") if Decimal("0.10") <= f.drawdown_from_local_high <= Decimal("0.25") else Decimal("0"))
        + (Decimal("4") if context.vwap_reclaimed else Decimal("0"))
        + (Decimal("2") if f.return_since_pool_creation <= Decimal("2.5") else Decimal("0")),
        Decimal("10"),
    )
    total = organic + distribution + execution + liquidity + Decimal("5") + price
    return total.quantize(Decimal("0.01"))


def test_default_score_anchors_reproduce_the_specified_formula() -> None:
    rng = random.Random(7)
    engine = ScoringEngine()
    for _ in range(300):
        features = FeatureSnapshot(
            pool_address="POOL",
            snapshot_time=NOW,
            pool_age_seconds=Decimal("90"),
            unique_buyers_60s=rng.randint(0, 40),
            buyer_acceleration=Decimal(str(round(rng.uniform(0, 3), 3))),
            unique_buyer_ratio=Decimal(str(round(rng.uniform(0, 1), 3))),
            buy_sell_volume_ratio=Decimal(str(round(rng.uniform(0, 6), 3))),
            transactions_per_trader=Decimal(str(round(rng.uniform(1, 8), 3))),
            top_10_holders_pct=Decimal(str(round(rng.uniform(0, 0.5), 3))),
            largest_related_cluster_pct=Decimal(str(round(rng.uniform(0, 0.3), 3))),
            dev_cluster_holding_pct=Decimal(str(round(rng.uniform(0, 0.1), 3))),
            top_5_buyer_volume_share=Decimal(str(round(rng.uniform(0, 1), 3))),
            quote_liquidity_usd=Decimal(rng.randint(10_000, 150_000)),
            quote_liquidity_change_30s=Decimal(str(round(rng.uniform(-0.1, 0.1), 3))),
            market_cap_to_quote_liquidity=Decimal(str(round(rng.uniform(0, 30), 3))),
            drawdown_from_local_high=Decimal(str(round(rng.uniform(0, 0.4), 3))),
            return_since_pool_creation=Decimal(str(round(rng.uniform(0, 4), 3))),
        )
        context = ScoreContext(
            features=features,
            round_trip_loss_pct=Decimal(str(round(rng.uniform(0, 0.12), 4))),
            buy_price_impact_pct=Decimal(str(round(rng.uniform(0, 0.05), 4))),
            sell_price_impact_pct=Decimal(str(round(rng.uniform(0, 0.05), 4))),
            sell_route_reliability=Decimal(rng.choice(["0", "1"])),
            developer_history=DeveloperHistory(),
            vwap_reclaimed=rng.random() < 0.5,
        )
        assert engine.score(context).total_score == _legacy_total(context)


def test_score_anchors_are_configurable_and_part_of_the_strategy_hash() -> None:
    base = dict(
        APP_MODE="paper",
        APP_REVISION="",
        HELIUS_API_KEY="helius-key",
        JUPITER_API_KEY="jupiter-key",
        POSTGRES_DSN="postgresql://user:pass@localhost:5432/db",
        TELEGRAM_BOT_TOKEN="telegram-token",
        TELEGRAM_ADMIN_CHAT_ID=123456,
    )
    default = AppConfig(**base)
    changed = AppConfig(
        **base,
        scoring=ScoringConfig(
            quote_liquidity=ScoreAnchor(
                zero=Decimal("20000"), full=Decimal("100000"), points=Decimal("5")
            )
        ),
    )
    providers = AppConfig(**base, providers={"jupiter_base_url": "http://fake:9000"})
    assert changed.config_hash != default.config_hash
    assert providers.config_hash == default.config_hash


def test_sizing_uses_the_configured_caps() -> None:
    base = PositionSizingInput(
        current_equity_usd=Decimal("500"),
        daily_pnl_usd=Decimal("0"),
        quote_liquidity_usd=Decimal("100000"),
        estimated_round_trip_cost_pct=Decimal("0.02"),
        score=Decimal("95"),
        maximum_position_usd=Decimal("1000"),
    )
    default = calculate_position_size(base)
    # 500 * 0.005 / (0.15 + 0.02 + 0.01)
    assert default.position_size_usd == Decimal("13.88")

    riskier = calculate_position_size(
        base.model_copy(update={"risk_per_trade_pct": Decimal("0.01")})
    )
    assert riskier.position_size_usd == Decimal("20.00")  # 4% of equity cap
    capped = calculate_position_size(
        base.model_copy(
            update={
                "risk_per_trade_pct": Decimal("0.01"),
                "maximum_position_equity_pct": Decimal("0.1"),
                "maximum_position_to_liquidity_pct": Decimal("0.0002"),
            }
        )
    )
    assert capped.position_size_usd == Decimal("20.00")  # 0.02% of 100k liquidity


def test_risk_manager_pauses_on_the_injected_clock(tmp_path: Path) -> None:
    ledger = PaperLedger(
        storage_path=tmp_path / "ledger.json",
        starting_equity_usd=Decimal("500"),
        strategy_version="strategy",
        config_hash="hash",
        time_zone="Europe/Kyiv",
    )
    config = AppConfig(
        APP_MODE="paper",
        APP_REVISION="",
        HELIUS_API_KEY="helius-key",
        JUPITER_API_KEY="jupiter-key",
        POSTGRES_DSN="postgresql://user:pass@localhost:5432/db",
        TELEGRAM_BOT_TOKEN="telegram-token",
        TELEGRAM_ADMIN_CHAT_ID=123456,
    )
    clock_now = [NOW]
    manager = RiskManager(config.risk, ledger, clock=lambda: clock_now[0])
    ledger.set_pause_until(NOW + timedelta(minutes=30))

    assert manager.evaluate_entry(Decimal("10")).reason == "CONSECUTIVE_LOSS_PAUSE"
    clock_now[0] = NOW + timedelta(minutes=31)
    assert manager.evaluate_entry(Decimal("10")).decision == RiskDecision.ALLOW
