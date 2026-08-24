from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select

from sniper_bot.database import Database
from sniper_bot.db_models import (
    DailyReportRow,
    EventDedupRow,
    OutboxEventRow,
    RawChainEventRow,
)
from sniper_bot.events import ChainEventType, EventEnvelope, EventSource, Protocol
from sniper_bot.registry import WSOL_MINT, PoolRecord, TokenRecord


@pytest.mark.asyncio
async def test_database_event_and_outbox_idempotency(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'state.db'}")
    await database.create_schema_for_tests()
    now = datetime(2026, 8, 24, tzinfo=timezone.utc)
    await database.register_strategy(
        strategy_id="strategy-v1",
        version="strategy-v1",
        config_hash="hash",
        config_json={"mode": "paper"},
        now=now,
    )
    token = TokenRecord(mint="TOKEN", creation_time=now, updated_at=now)
    pool = PoolRecord(
        pool_address="POOL",
        base_mint="TOKEN",
        quote_mint=WSOL_MINT,
        creation_signature="sig",
        creation_slot=1,
        creation_time=now,
        base_decimals=6,
        quote_decimals=9,
        updated_at=now,
    )
    await database.upsert_token(token)
    await database.upsert_pool(pool)
    event = EventEnvelope(
        source=EventSource.REPLAY,
        protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.POOL_CREATED,
        slot=1,
        signature="sig",
        instruction_index=1,
        block_time=now,
        observed_at=now,
        mint="TOKEN",
        pool_address="POOL",
        payload={"quote_mint": WSOL_MINT},
    )

    assert await database.record_event(event) is True
    assert await database.record_event(event) is False
    await database.mark_event_processed(event.event_id, processed_at=now)
    assert await database.load_protocol_checkpoints() == {"pumpswap": "sig"}
    assert await database.enqueue_outbox(
        idempotency_key="entry:1", event_type="paper_entry", payload={"mint": "TOKEN"}
    ) is True
    assert await database.enqueue_outbox(
        idempotency_key="entry:1", event_type="paper_entry", payload={"mint": "TOKEN"}
    ) is False

    async with database.sessions() as session:
        assert len(list((await session.scalars(select(RawChainEventRow))).all())) == 1
        assert len(list((await session.scalars(select(OutboxEventRow))).all())) == 1
    pending = await database.pending_outbox()
    assert len(pending) == 1
    assert pending[0].claim_token is not None
    await database.mark_outbox_delivered(pending[0].id, pending[0].claim_token)
    assert await database.outbox_count() == 0
    await database.close()


@pytest.mark.asyncio
async def test_failed_event_can_be_reclaimed_and_completed(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'recovery.db'}")
    await database.create_schema_for_tests()
    now = datetime(2026, 8, 24, tzinfo=timezone.utc)
    event = EventEnvelope(
        source=EventSource.REPLAY,
        protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.SWAP_BUY,
        slot=2,
        signature="recovery-sig",
        instruction_index=0,
        block_time=now,
        observed_at=now,
        mint="TOKEN",
        pool_address="POOL",
        payload={"base_amount_out": "1", "quote_amount_in": "1"},
    )

    assert await database.record_event(event) is True
    assert await database.load_unprocessed_events() == []
    await database.mark_event_failed(event.event_id, RuntimeError("transient failure"))
    recovered = await database.load_unprocessed_events()
    assert [row.event_id for row in recovered] == [event.event_id]
    assert await database.record_event(event, reclaim=True) is True
    await database.mark_event_processed(event.event_id, processed_at=now + timedelta(seconds=1))
    assert await database.load_unprocessed_events() == []
    async with database.sessions() as session:
        claim = await session.get(EventDedupRow, event.event_id)
        assert claim is not None
        assert claim.processing_status == "PROCESSED"
        assert claim.processing_attempts == 2
        assert claim.last_error is None
    await database.close()


@pytest.mark.asyncio
async def test_outbox_expired_send_is_uncertain_and_report_delivery_is_atomic(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'outbox.db'}")
    await database.create_schema_for_tests()
    now = datetime(2026, 8, 24, tzinfo=timezone.utc)
    await database.register_strategy(
        strategy_id="strategy-v1",
        version="strategy-v1",
        config_hash="hash",
        config_json={},
        now=now,
    )
    assert await database.enqueue_outbox(
        idempotency_key="uncertain:1",
        event_type="paper_entry",
        payload={"text": "entry"},
    )
    claimed = await database.pending_outbox()
    assert len(claimed) == 1
    stale_claim_token = claimed[0].claim_token
    assert stale_claim_token is not None
    async with database.sessions.begin() as session:
        row = await session.get(OutboxEventRow, claimed[0].id)
        assert row is not None
        row.claimed_at = now - timedelta(minutes=2)
    assert await database.pending_outbox() == []
    async with database.sessions() as session:
        uncertain = await session.get(OutboxEventRow, claimed[0].id)
        assert uncertain is not None
        assert uncertain.delivery_state == "UNCERTAIN"
    assert await database.resolve_uncertain_outbox(
        claimed[0].id, action="retry"
    ) == "FAILED"
    retried = await database.pending_outbox()
    assert [row.id for row in retried] == [claimed[0].id]
    assert retried[0].claim_token is not None
    await database.mark_outbox_uncertain(
        claimed[0].id, retried[0].claim_token, "OPERATOR_CHECK"
    )
    assert await database.resolve_uncertain_outbox(
        claimed[0].id,
        action="delivered",
        telegram_message_id="telegram-reconciled",
    ) == "DELIVERED"
    with pytest.raises(RuntimeError, match="superseded"):
        await database.mark_outbox_delivered(
            claimed[0].id, stale_claim_token, "late-worker-message"
        )

    async with database.sessions.begin() as session:
        session.add(
            DailyReportRow(
                report_date=now.date(),
                timezone="Europe/Kyiv",
                strategy_version_id="strategy-v1",
                report_json={"date": now.date().isoformat()},
                generated_at=now,
            )
        )
    assert await database.enqueue_outbox(
        idempotency_key="daily:1",
        event_type="daily_report",
        payload={
            "text": "daily",
            "_daily_report": {
                "date": now.date().isoformat(),
                "timezone": "Europe/Kyiv",
                "strategy_version": "strategy-v1",
            },
        },
    )
    report_event = await database.pending_outbox()
    assert len(report_event) == 1
    assert report_event[0].claim_token is not None
    await database.mark_outbox_delivered(
        report_event[0].id, report_event[0].claim_token, "telegram-42"
    )
    async with database.sessions() as session:
        report = await session.get(
            DailyReportRow,
            (now.date(), "Europe/Kyiv", "strategy-v1"),
        )
        assert report is not None
        assert report.telegram_message_id == "telegram-42"
        assert report.sent_at is not None
    stored = await database.load_daily_report(
        now.date().isoformat(),
        timezone_name="Europe/Kyiv",
        strategy_version="strategy-v1",
    )
    assert stored == {"date": now.date().isoformat()}
    await database.close()


@pytest.mark.asyncio
async def test_daily_equity_bounds_preserve_historical_unrealized_pnl(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'equity-marks.db'}")
    await database.create_schema_for_tests()
    before_day = datetime(2026, 8, 23, 20, tzinfo=timezone.utc)
    during_day = datetime(2026, 8, 24, 10, tzinfo=timezone.utc)
    await database.initialize_paper_account(
        account_id="paper-main",
        starting_equity=Decimal("500"),
        now=before_day,
    )
    await database.update_paper_marks(
        account_id="paper-main",
        positions=[],
        executable_values={},
        account_snapshot={
            "equity_usd": "480",
            "peak_equity_usd": "500",
            "unrealized_pnl_usd": "-20",
            "locked_capital_usd": "10",
        },
        observed_at=during_day,
    )

    bounds = await database.load_daily_equity_bounds(
        account_id="paper-main",
        report_date="2026-08-24",
        timezone_name="Europe/Kyiv",
    )
    assert bounds is not None
    assert bounds["starting_equity_usd"] == Decimal("500")
    assert bounds["ending_equity_usd"] == Decimal("480")
    assert bounds["ending_unrealized_pnl_usd"] == Decimal("-20")
    assert bounds["equity_path_usd"] == [Decimal("500"), Decimal("480")]
    await database.close()
