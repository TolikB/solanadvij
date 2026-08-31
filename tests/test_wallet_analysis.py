from __future__ import annotations

from datetime import datetime, timedelta, timezone

import sniper_bot.wallet_analysis as wallet_analysis_module

from sniper_bot.events import ChainEventType, EventEnvelope, EventSource, Protocol
from sniper_bot.wallet_analysis import WalletAnalyzer


def _event(
    event_type: ChainEventType,
    at: datetime,
    *,
    signature: str,
    mint: str = "TOKEN",
    payload: dict[str, object] | None = None,
) -> EventEnvelope:
    return EventEnvelope(
        source=EventSource.REPLAY,
        protocol=Protocol.PUMPSWAP,
        event_type=event_type,
        slot=int(at.timestamp()),
        signature=signature,
        instruction_index=0,
        block_time=at,
        observed_at=at,
        mint=mint,
        pool_address="POOL",
        payload=payload or {},
    )


def test_same_initial_funder_is_strong_cluster_evidence() -> None:
    analyzer = WalletAnalyzer()
    now = datetime(2026, 8, 18, tzinfo=timezone.utc)
    analyzer.observe(
        _event(
            ChainEventType.SWAP_BUY, now, signature="a",
            payload={"user": "wallet-a", "funder": "funding-wallet", "quote_amount_in": 10},
        )
    )
    _, relations = analyzer.observe(
        _event(
            ChainEventType.SWAP_BUY, now + timedelta(seconds=1), signature="b",
            payload={"user": "wallet-b", "funder": "funding-wallet", "quote_amount_in": 11},
        )
    )

    relation = next(item for item in relations if {item.wallet_a, item.wallet_b} == {"wallet-a", "wallet-b"})
    assert relation.eligible_for_cluster is True
    assert "same_initial_funder" in relation.evidence


def test_developer_history_query_does_not_use_future_tokens() -> None:
    analyzer = WalletAnalyzer()
    now = datetime(2026, 8, 18, tzinfo=timezone.utc)
    analyzer.observe(
        _event(
            ChainEventType.TOKEN_CREATED, now, signature="create-1", mint="TOKEN-1",
            payload={"creator": "dev"},
        )
    )
    analyzer.observe(
        _event(
            ChainEventType.TOKEN_CREATED, now + timedelta(days=1), signature="create-2",
            mint="TOKEN-2", payload={"creator": "dev"},
        )
    )

    historical = analyzer.profile("dev", at=now + timedelta(hours=1))
    current = analyzer.profile("dev", at=now + timedelta(days=2))

    assert historical is not None and historical.tokens_created_total == 1
    assert current is not None and current.tokens_created_total == 2


def test_sell_and_repeated_evidence_emit_no_relation_delta(monkeypatch) -> None:
    analyzer = WalletAnalyzer()
    now = datetime(2026, 8, 18, tzinfo=timezone.utc)
    calls = 0
    original = wallet_analysis_module.score_relation

    def counted_score(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(
        wallet_analysis_module,
        "score_relation",
        counted_score,
    )
    analyzer.observe(
        _event(
            ChainEventType.SWAP_BUY,
            now,
            signature="delta-a",
            payload={
                "user": "wallet-a",
                "funder": "funding-wallet",
                "quote_amount_in": 10,
            },
        )
    )
    _, created = analyzer.observe(
        _event(
            ChainEventType.SWAP_BUY,
            now + timedelta(seconds=1),
            signature="delta-b",
            payload={
                "user": "wallet-b",
                "funder": "funding-wallet",
                "quote_amount_in": 11,
            },
        )
    )
    assert created
    calls = 0

    changed_profiles, sell_relations = analyzer.observe(
        _event(
            ChainEventType.SWAP_SELL,
            now + timedelta(seconds=2),
            signature="delta-sell",
            payload={"user": "wallet-a"},
        )
    )
    _, repeated_relations = analyzer.observe(
        _event(
            ChainEventType.SWAP_BUY,
            now + timedelta(seconds=3),
            signature="delta-repeat",
            payload={
                "user": "wallet-b",
                "quote_amount_in": 11,
            },
        )
    )

    assert changed_profiles == []
    assert sell_relations == []
    assert repeated_relations == []
    assert calls == 1


def test_new_buyer_relation_work_is_bounded_to_current_scope(monkeypatch) -> None:
    analyzer = WalletAnalyzer()
    now = datetime(2026, 8, 18, tzinfo=timezone.utc)
    calls = 0
    original = wallet_analysis_module.score_relation

    def counted_score(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(
        wallet_analysis_module,
        "score_relation",
        counted_score,
    )
    for index in range(40):
        analyzer.observe(
            _event(
                ChainEventType.SWAP_BUY,
                now + timedelta(seconds=index),
                signature=f"bounded-{index}",
                payload={
                    "user": f"wallet-{index:02d}",
                    "quote_amount_in": index + 1,
                },
            )
        )
    calls = 0

    analyzer.observe(
        _event(
            ChainEventType.SWAP_BUY,
            now + timedelta(seconds=41),
            signature="bounded-new",
            payload={
                "user": "wallet-new",
                "quote_amount_in": 10_000,
            },
        )
    )

    assert calls <= 39
    assert len(analyzer._buyer_first_order["TOKEN"]) == 20
    assert len(analyzer._buyer_first_members["TOKEN"]) == 20
    assert len(analyzer._buyer_top_volume["TOKEN"]) == 20

def test_out_of_order_buy_does_not_use_future_relation_evidence() -> None:
    analyzer = WalletAnalyzer()
    now = datetime(2026, 8, 18, tzinfo=timezone.utc)
    analyzer.observe(
        _event(
            ChainEventType.SWAP_BUY,
            now + timedelta(seconds=10),
            signature="future-buy",
            payload={
                "user": "wallet-future",
                "quote_amount_in": 10,
            },
        )
    )

    _, historical_relations = analyzer.observe(
        _event(
            ChainEventType.SWAP_BUY,
            now,
            signature="historical-buy",
            payload={
                "user": "wallet-historical",
                "quote_amount_in": 10,
            },
        )
    )

    assert historical_relations == []

    _, current_relations = analyzer.observe(
        _event(
            ChainEventType.SWAP_BUY,
            now + timedelta(seconds=11),
            signature="current-buy",
            payload={
                "user": "wallet-historical",
                "quote_amount_in": 11,
            },
        )
    )

    relation = next(
        item
        for item in current_relations
        if {item.wallet_a, item.wallet_b}
        == {"wallet-future", "wallet-historical"}
    )
    assert "identical_trade_amount_pattern" in relation.evidence
