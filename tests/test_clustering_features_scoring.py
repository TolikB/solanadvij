from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sniper_bot.clustering import RelationEvidence, build_clusters, score_relation
from sniper_bot.features import (
    EventTimeFeatureEngine,
    HolderObservation,
    LiquidityObservation,
    TradeObservation,
    TradeSide,
)
from sniper_bot.scoring import ScoreContext, ScoringEngine


def test_weak_wallet_signal_does_not_cluster_but_two_independent_signals_do() -> None:
    weak = score_relation("a", "b", RelationEvidence(same_slot_buying=True))
    combined = score_relation(
        "a",
        "b",
        RelationEvidence(same_slot_buying=True, identical_trade_amount_pattern=True),
    )

    assert weak.eligible_for_cluster is False
    assert combined.relation_score >= Decimal("0.70")
    assert combined.eligible_for_cluster is True
    clusters = build_clusters({"a", "b", "c"}, [combined])
    assert [cluster.wallets for cluster in clusters] == [["a", "b"], ["c"]]
    assert combined.evidence == ["same_slot_buying", "identical_trade_amount_pattern"]


def _organic_snapshot():
    engine = EventTimeFeatureEngine()
    start = datetime(2026, 8, 24, 12, 0, tzinfo=timezone.utc)
    engine.register_pool("POOL", start)
    engine.ingest_liquidity(
        LiquidityObservation(
            event_id="liq-0",
            pool_address="POOL",
            event_time=start,
            quote_liquidity_usd=Decimal("50000"),
            market_cap_usd=Decimal("300000"),
        )
    )
    engine.ingest_holders(
        HolderObservation(
            event_id="holders-0",
            pool_address="POOL",
            event_time=start,
            holder_count=10,
            top_10_holders_pct=Decimal("0.25"),
            dev_cluster_holding_pct=Decimal("0.03"),
            largest_related_cluster_pct=Decimal("0.08"),
        )
    )
    for index in range(10):
        engine.ingest_trade(
            TradeObservation(
                event_id=f"buy-old-{index}",
                pool_address="POOL",
                event_time=start + timedelta(seconds=5 + index * 2),
                side=TradeSide.BUY,
                wallet=f"old-{index}",
                volume_usd=Decimal("100"),
                price_usd=Decimal("1"),
            )
        )
    for index in range(15):
        engine.ingest_trade(
            TradeObservation(
                event_id=f"buy-new-{index}",
                pool_address="POOL",
                event_time=start + timedelta(seconds=31 + index),
                side=TradeSide.BUY,
                wallet=f"new-{index}",
                volume_usd=Decimal("100"),
                price_usd=Decimal("1.2") if index < 14 else Decimal("1.02"),
            )
        )
    for index in range(5):
        engine.ingest_trade(
            TradeObservation(
                event_id=f"sell-{index}",
                pool_address="POOL",
                event_time=start + timedelta(seconds=46 + index),
                side=TradeSide.SELL,
                wallet=f"seller-{index}",
                volume_usd=Decimal("100"),
                price_usd=Decimal("1.02"),
            )
        )
    engine.ingest_liquidity(
        LiquidityObservation(
            event_id="liq-55",
            pool_address="POOL",
            event_time=start + timedelta(seconds=55),
            quote_liquidity_usd=Decimal("60000"),
            market_cap_usd=Decimal("300000"),
        )
    )
    engine.ingest_holders(
        HolderObservation(
            event_id="holders-55",
            pool_address="POOL",
            event_time=start + timedelta(seconds=55),
            holder_count=30,
            top_10_holders_pct=Decimal("0.15"),
            dev_cluster_holding_pct=Decimal("0.01"),
            largest_related_cluster_pct=Decimal("0.03"),
        )
    )
    future = TradeObservation(
        event_id="future",
        pool_address="POOL",
        event_time=start + timedelta(seconds=70),
        side=TradeSide.BUY,
        wallet="future-wallet",
        volume_usd=Decimal("999999"),
        price_usd=Decimal("99"),
    )
    assert engine.ingest_trade(future) is True
    assert engine.ingest_trade(future) is False
    return engine.snapshot("POOL", start + timedelta(seconds=60))


def test_feature_engine_is_duplicate_safe_and_anti_lookahead() -> None:
    snapshot = _organic_snapshot()

    assert snapshot.unique_buyers_60s == 25
    assert snapshot.buyer_acceleration == Decimal("1.5")
    assert snapshot.external_successful_sellers == 5
    assert snapshot.buyer_volume_hhi == Decimal("0.0400")
    assert snapshot.quote_liquidity_change_since_pool_creation == Decimal("0.2")
    assert snapshot.holder_growth_60s == 20
    assert snapshot.current_price_usd == Decimal("1.02")
    assert snapshot.local_high == Decimal("1.2")


def test_scoring_categories_are_capped_and_organic_case_exceeds_entry_score() -> None:
    snapshot = _organic_snapshot().model_copy(
        update={
            "drawdown_from_local_high": Decimal("0.15"),
            "top_5_buyer_volume_share": Decimal("0.30"),
            "unique_buyer_ratio": Decimal("0.8"),
            "buy_sell_volume_ratio": Decimal("3"),
            "transactions_per_trader": Decimal("1.2"),
            "quote_liquidity_change_30s": Decimal("0"),
        }
    )
    result = ScoringEngine().score(
        ScoreContext(
            features=snapshot,
            round_trip_loss_pct=Decimal("0.02"),
            buy_price_impact_pct=Decimal("0.005"),
            sell_price_impact_pct=Decimal("0.007"),
            sell_route_reliability=Decimal("1"),
            vwap_reclaimed=True,
        )
    )

    assert result.total_score >= Decimal("80")
    assert result.organic_score <= Decimal("25")
    assert result.distribution_score <= Decimal("20")
    assert result.execution_score <= Decimal("20")
    assert result.liquidity_score <= Decimal("15")


def _trade(
    event_id: str,
    at: datetime,
    price: str,
) -> TradeObservation:
    return TradeObservation(
        event_id=event_id,
        pool_address="ORDERED-POOL",
        event_time=at,
        side=TradeSide.BUY,
        wallet=event_id,
        volume_usd=Decimal("10"),
        price_usd=Decimal(price),
    )


def test_feature_engine_ordered_append_does_not_sort_existing_history() -> None:
    class NoSortList(list):
        def sort(self, *args, **kwargs) -> None:
            raise AssertionError("ordered ingestion must not sort history")

    engine = EventTimeFeatureEngine()
    now = datetime(2026, 8, 24, tzinfo=timezone.utc)
    engine._trades["ORDERED-POOL"] = NoSortList()

    assert engine.ingest_trade(_trade("first", now, "1")) is True
    assert (
        engine.ingest_trade(
            _trade("second", now + timedelta(seconds=1), "2")
        )
        is True
    )
    assert [event.event_id for event in engine.trades("ORDERED-POOL")] == [
        "first",
        "second",
    ]


def test_feature_engine_out_of_order_insert_matches_ordered_snapshot() -> None:
    now = datetime(2026, 8, 24, tzinfo=timezone.utc)
    events = [
        _trade("first", now, "1"),
        _trade("second", now + timedelta(seconds=1), "2"),
        _trade("third", now + timedelta(seconds=2), "3"),
    ]
    ordered = EventTimeFeatureEngine()
    recovered = EventTimeFeatureEngine()
    for event in events:
        ordered.ingest_trade(event)
    for event in reversed(events):
        recovered.ingest_trade(event)

    at = now + timedelta(seconds=3)
    assert recovered.snapshot("ORDERED-POOL", at) == ordered.snapshot(
        "ORDERED-POOL",
        at,
    )