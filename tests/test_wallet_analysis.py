from __future__ import annotations

from datetime import datetime, timedelta, timezone

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
