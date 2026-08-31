from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import event as sqlalchemy_event
from sqlalchemy import select

from sniper_bot.database import (
    SQLITE_SAFE_BOUND_PARAMETER_BUDGET,
    Database,
    _telegram_report_text,
)
from sniper_bot.db_models import (
    DailyReportRow,
    EventDedupRow,
    OutboxEventRow,
    RawChainEventRow,
    TokenRow,
)
from sniper_bot.events import ChainEventType, EventEnvelope, EventSource, Protocol
from sniper_bot.metrics import BotMetrics
from sniper_bot.pipeline import ConfirmationPipeline
from sniper_bot.registry import WSOL_MINT, PoolRecord, TokenRecord
from sniper_bot.stream import EntryGate


def test_daily_telegram_report_is_human_readable_and_trade_only() -> None:
    text = _telegram_report_text(
        {
            "period": "daily",
            "date": "2026-08-25",
            "capital": {
                "starting_equity_usd": "500",
                "ending_equity_usd": "512.34",
                "realized_pnl_usd": "10",
                "unrealized_pnl_usd": "2.34",
            },
            "signals": {"paper_entries": 3},
            "trades": {
                "closed": 2,
                "profitable": 1,
                "losing": 1,
                "win_rate": "0.5",
            },
            "open_positions": [{}],
            "strategy_version": "must-not-be-shown",
            "config_hash": "must-not-be-shown",
            "report_id": "must-not-be-shown",
        }
    )

    assert text == (
        "Щоденний звіт про тестову торгівлю\n"
        "Дата: 2026-08-25\n"
        "Баланс: $500.00 -> $512.34\n"
        "Результат дня: +$12.34\n"
        "Закритий PnL: +$10.00\n"
        "Відкритий PnL: +$2.34\n"
        "Угоди: відкрито 3, закрито 2\n"
        "Результати: прибуткових 1, збиткових 1\n"
        "Частка прибуткових: 50.0%\n"
        "Відкриті позиції: 1"
    )
    assert "strategy" not in text
    assert "config" not in text
    assert "report_id" not in text


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
    assert await database.load_stream_checkpoint() == (1, "sig", now)
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


@pytest.mark.asyncio
async def test_event_state_transaction_rolls_back_and_preserves_claim(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'event-state.db'}")
    await database.create_schema_for_tests()
    now = datetime(2026, 8, 25, tzinfo=timezone.utc)
    token = TokenRecord(mint="TOKEN", creation_time=now, updated_at=now)
    pool = PoolRecord(
        pool_address="POOL",
        base_mint="TOKEN",
        quote_mint=WSOL_MINT,
        creation_signature="atomic-sig",
        creation_slot=3,
        creation_time=now,
        base_decimals=6,
        quote_decimals=9,
        updated_at=now,
    )
    event = EventEnvelope(
        source=EventSource.REPLAY,
        protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.POOL_CREATED,
        slot=3,
        signature="atomic-sig",
        instruction_index=0,
        block_time=now,
        observed_at=now,
        mint="TOKEN",
        pool_address="POOL",
        payload={"quote_mint": WSOL_MINT},
    )

    assert await database.record_event(event) is True
    with pytest.raises(RuntimeError, match="force rollback"):
        async with database.event_state_transaction():
            await database.upsert_token(token)
            await database.upsert_pool(pool)
            await database.mark_event_processed(event.event_id, processed_at=now)
            raise RuntimeError("force rollback")

    async with database.sessions() as session:
        assert await session.get(TokenRow, "TOKEN") is None
        claim = await session.get(EventDedupRow, event.event_id)
        assert claim is not None
        assert claim.processing_status == "PROCESSING"

    await database.mark_event_failed(event.event_id, RuntimeError("rolled back"))
    assert await database.record_event(event, reclaim=True) is True
    async with database.event_state_transaction():
        await database.upsert_token(token)
        await database.upsert_pool(pool)
        await database.mark_event_processed(event.event_id, processed_at=now)
    database.release_event_claim(event.event_id)

    async with database.sessions() as session:
        claim = await session.get(EventDedupRow, event.event_id)
        assert claim is not None
        assert claim.processing_status == "PROCESSED"
    await database.close()


@pytest.mark.asyncio
async def test_pipeline_requests_rebuild_when_failure_marker_is_cancelled(
    tmp_path, monkeypatch
) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'commit-failure.db'}")
    await database.create_schema_for_tests()
    now = datetime(2026, 8, 25, tzinfo=timezone.utc)
    event = EventEnvelope(
        source=EventSource.REPLAY,
        protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.SWAP_BUY,
        slot=4,
        signature="commit-failure",
        instruction_index=0,
        block_time=now,
        observed_at=now,
        mint="TOKEN",
        pool_address="POOL",
        payload={"base_amount_out": "1", "quote_amount_in": "1"},
    )
    original_transaction = database.event_state_transaction
    fatal_errors: list[BaseException] = []

    @asynccontextmanager
    async def fail_after_transaction_body():
        async with original_transaction():
            yield
        raise RuntimeError("simulated commit failure")

    async def fail_to_mark_event(_event_id: str, _error: BaseException) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(database, "event_state_transaction", fail_after_transaction_body)
    monkeypatch.setattr(database, "mark_event_failed", fail_to_mark_event)
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(BotMetrics()),
        metrics=BotMetrics(),
        database=database,
        fatal_handler=fatal_errors.append,
        record_raw=False,
    )

    with pytest.raises(asyncio.CancelledError):
        await pipeline.process_event(event)

    assert len(fatal_errors) == 1
    assert event.event_id not in database._event_claim_tokens
    async with database.sessions() as session:
        claim = await session.get(EventDedupRow, event.event_id)
        assert claim is not None
        assert claim.processing_status == "PROCESSED"
    await database.close()


def _pipeline_batch_events(
    prefix: str,
    now: datetime,
    *,
    count: int = 2,
) -> list[EventEnvelope]:
    return [
        EventEnvelope(
            source=EventSource.REPLAY,
            protocol=Protocol.PUMPSWAP,
            event_type=ChainEventType.SWAP_BUY,
            slot=10 + index,
            signature=f"{prefix}-{index}",
            instruction_index=0,
            block_time=now + timedelta(milliseconds=index),
            observed_at=now + timedelta(milliseconds=index),
            mint="TOKEN",
            pool_address="POOL",
            payload={"base_amount_out": "1", "quote_amount_in": "1"},
        )
        for index in range(count)
    ]


def _install_pipeline_batch_decoder(monkeypatch, pipeline, events) -> None:
    monkeypatch.setattr(
        pipeline._pumpswap,
        "decode_transaction",
        lambda _transaction, source: events,
    )
    monkeypatch.setattr(pipeline, "_event_state_filter_reason", lambda _event: None)


async def _run_pipeline_batch(pipeline: ConfirmationPipeline) -> None:
    await pipeline.process_transactions(
        [(Protocol.PUMPSWAP, {"signature": "batch"}, EventSource.REPLAY)]
    )


@pytest.mark.asyncio
async def test_pipeline_splits_decoded_events_into_bounded_ordered_chunks(
    tmp_path, monkeypatch
) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'pipeline-chunks.db'}")
    await database.create_schema_for_tests()
    events = _pipeline_batch_events(
        "pipeline-chunks",
        datetime(2026, 8, 25, tzinfo=timezone.utc),
        count=5,
    )
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(BotMetrics()),
        metrics=BotMetrics(),
        database=database,
        record_raw=False,
    )
    _install_pipeline_batch_decoder(monkeypatch, pipeline, events)
    monkeypatch.setattr("sniper_bot.pipeline.MAX_EVENT_BATCH_SIZE", 2)
    original_record_events = database.record_events
    durable_batches: list[list[str]] = []

    async def tracked_record_events(
        batch: list[EventEnvelope],
        *,
        reclaim: bool = False,
        resume_owned: bool = False,
    ) -> list[bool]:
        durable_batches.append([event.signature for event in batch])
        return await original_record_events(
            batch,
            reclaim=reclaim,
            resume_owned=resume_owned,
        )

    monkeypatch.setattr(database, "record_events", tracked_record_events)

    await _run_pipeline_batch(pipeline)

    assert durable_batches == [
        ["pipeline-chunks-0", "pipeline-chunks-1"],
        ["pipeline-chunks-2", "pipeline-chunks-3"],
        ["pipeline-chunks-4"],
    ]
    assert database._event_claim_tokens == {}
    await database.close()


@pytest.mark.asyncio
async def test_pipeline_commits_claimed_batch_in_one_state_transaction(
    tmp_path, monkeypatch
) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'pipeline-batch.db'}")
    await database.create_schema_for_tests()
    events = _pipeline_batch_events(
        "pipeline-batch", datetime(2026, 8, 25, tzinfo=timezone.utc)
    )
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(BotMetrics()),
        metrics=BotMetrics(),
        database=database,
        record_raw=False,
    )
    _install_pipeline_batch_decoder(monkeypatch, pipeline, events)
    transaction_count = 0
    original_transaction = database.event_state_batch_transaction

    @asynccontextmanager
    async def counted_transaction():
        nonlocal transaction_count
        transaction_count += 1
        async with original_transaction():
            yield

    monkeypatch.setattr(
        database,
        "event_state_batch_transaction",
        counted_transaction,
    )

    await _run_pipeline_batch(pipeline)

    assert transaction_count == 1
    assert database._event_claim_tokens == {}
    async with database.sessions() as session:
        claims = list(
            (
                await session.scalars(
                    select(EventDedupRow).order_by(EventDedupRow.event_id)
                )
            ).all()
        )
        raw_events = list(
            (
                await session.scalars(
                    select(RawChainEventRow).order_by(
                        RawChainEventRow.ingest_sequence
                    )
                )
            ).all()
        )
    assert [claim.processing_status for claim in claims] == ["PROCESSED", "PROCESSED"]
    assert [event.signature for event in raw_events] == [
        "pipeline-batch-0",
        "pipeline-batch-1",
    ]
    await database.close()


@pytest.mark.asyncio
async def test_pipeline_batch_archives_claims_once(tmp_path, monkeypatch) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'pipeline-archive.db'}")
    await database.create_schema_for_tests()
    events = _pipeline_batch_events(
        "pipeline-archive", datetime(2026, 8, 25, tzinfo=timezone.utc)
    )
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(BotMetrics()),
        metrics=BotMetrics(),
        database=database,
        record_raw=True,
    )
    _install_pipeline_batch_decoder(monkeypatch, pipeline, events)
    archived_batches: list[list[str]] = []

    async def capture_batch(batch: list[EventEnvelope]) -> None:
        archived_batches.append([event.event_id for event in batch])

    async def forbid_single_record(_event: EventEnvelope) -> None:
        raise AssertionError("batch pipeline must not archive events one at a time")

    monkeypatch.setattr(pipeline.recorder, "record_many", capture_batch)
    monkeypatch.setattr(pipeline.recorder, "record", forbid_single_record)

    try:
        await _run_pipeline_batch(pipeline)

        assert archived_batches == [[event.event_id for event in events]]
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_pipeline_batch_archive_failure_fails_claims_and_poison_pipeline(
    tmp_path,
    monkeypatch,
) -> None:
    database = Database(
        f"sqlite+aiosqlite:///{tmp_path / 'pipeline-archive-failure.db'}"
    )
    await database.create_schema_for_tests()
    events = _pipeline_batch_events(
        "pipeline-archive-failure", datetime(2026, 8, 25, tzinfo=timezone.utc)
    )
    fatal_errors: list[BaseException] = []
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(BotMetrics()),
        metrics=BotMetrics(),
        database=database,
        fatal_handler=fatal_errors.append,
        record_raw=True,
    )
    _install_pipeline_batch_decoder(monkeypatch, pipeline, events)

    async def fail_archive(_batch: list[EventEnvelope]) -> None:
        raise OSError("simulated archive failure")

    monkeypatch.setattr(pipeline.recorder, "record_many", fail_archive)

    try:
        with pytest.raises(OSError, match="simulated archive failure"):
            await _run_pipeline_batch(pipeline)

        assert len(fatal_errors) == 1
        assert database._event_claim_tokens == {}
        assert "event_processing_error" in pipeline.entry_gate.reasons
        async with database.sessions() as session:
            claims = list((await session.scalars(select(EventDedupRow))).all())
        assert len(claims) == len(events)
        assert all(claim.processing_status == "FAILED" for claim in claims)
        assert all(claim.processing_token is None for claim in claims)
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_pipeline_batch_failure_rolls_back_state_and_poison_pipeline(
    tmp_path, monkeypatch
) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'pipeline-batch-failure.db'}")
    await database.create_schema_for_tests()
    now = datetime(2026, 8, 25, tzinfo=timezone.utc)
    events = _pipeline_batch_events("pipeline-failure", now)
    fatal_errors: list[BaseException] = []
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(BotMetrics()),
        metrics=BotMetrics(),
        database=database,
        fatal_handler=fatal_errors.append,
        record_raw=False,
    )
    _install_pipeline_batch_decoder(monkeypatch, pipeline, events)
    applied = 0

    async def write_then_fail(
        _event,
        *,
        persist,
        observe,
        allow_candidate=True,
    ):
        nonlocal applied
        applied += 1
        if applied == 1:
            await database.upsert_token(
                TokenRecord(mint="BATCH-ROLLBACK", creation_time=now, updated_at=now)
            )
            return
        raise RuntimeError("batch state failure")

    monkeypatch.setattr(pipeline, "_apply_event", write_then_fail)

    with pytest.raises(RuntimeError, match="batch state failure"):
        await _run_pipeline_batch(pipeline)

    assert len(fatal_errors) == 1
    assert database._event_claim_tokens == {}
    assert "event_processing_error" in pipeline.entry_gate.reasons
    with pytest.raises(RuntimeError, match="restart required"):
        await _run_pipeline_batch(pipeline)
    async with database.sessions() as session:
        assert await session.get(TokenRow, "BATCH-ROLLBACK") is None
        claims = list(
            (
                await session.scalars(
                    select(EventDedupRow).order_by(EventDedupRow.event_id)
                )
            ).all()
        )
    assert [claim.processing_status for claim in claims] == ["FAILED", "FAILED"]
    assert [event.signature for event in await database.load_unprocessed_events()] == [
        "pipeline-failure-0",
        "pipeline-failure-1",
    ]
    await database.close()


@pytest.mark.asyncio
async def test_pipeline_batch_uncertain_commit_retains_tokens_and_poison_pipeline(
    tmp_path, monkeypatch
) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'pipeline-batch-commit.db'}")
    await database.create_schema_for_tests()
    events = _pipeline_batch_events(
        "pipeline-commit", datetime(2026, 8, 25, tzinfo=timezone.utc)
    )
    fatal_errors: list[BaseException] = []
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(BotMetrics()),
        metrics=BotMetrics(),
        database=database,
        fatal_handler=fatal_errors.append,
        record_raw=False,
    )
    _install_pipeline_batch_decoder(monkeypatch, pipeline, events)
    original_transaction = database.event_state_batch_transaction

    @asynccontextmanager
    async def fail_after_commit():
        async with original_transaction():
            yield
        raise RuntimeError("batch commit acknowledgement failure")

    monkeypatch.setattr(
        database,
        "event_state_batch_transaction",
        fail_after_commit,
    )

    with pytest.raises(RuntimeError, match="batch commit acknowledgement failure"):
        await _run_pipeline_batch(pipeline)

    assert len(fatal_errors) == 1
    assert set(database._event_claim_tokens) == {event.event_id for event in events}
    assert "event_processing_error" in pipeline.entry_gate.reasons
    async with database.sessions() as session:
        claims = list(
            (
                await session.scalars(
                    select(EventDedupRow).order_by(EventDedupRow.event_id)
                )
            ).all()
        )
    assert [claim.processing_status for claim in claims] == ["PROCESSED", "PROCESSED"]
    await database.close()


@pytest.mark.asyncio
async def test_pipeline_batch_cancellation_rolls_back_and_fails_claims(
    tmp_path, monkeypatch
) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'pipeline-batch-cancel.db'}")
    await database.create_schema_for_tests()
    now = datetime(2026, 8, 25, tzinfo=timezone.utc)
    events = _pipeline_batch_events("pipeline-cancel", now)
    fatal_errors: list[BaseException] = []
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(BotMetrics()),
        metrics=BotMetrics(),
        database=database,
        fatal_handler=fatal_errors.append,
        record_raw=False,
    )
    _install_pipeline_batch_decoder(monkeypatch, pipeline, events)
    applied = 0

    async def cancel_second_event(
        _event,
        *,
        persist,
        observe,
        allow_candidate=True,
    ):
        nonlocal applied
        applied += 1
        if applied == 1:
            await database.upsert_token(
                TokenRecord(mint="BATCH-CANCEL", creation_time=now, updated_at=now)
            )
            return
        raise asyncio.CancelledError

    monkeypatch.setattr(pipeline, "_apply_event", cancel_second_event)

    with pytest.raises(asyncio.CancelledError):
        await _run_pipeline_batch(pipeline)

    assert len(fatal_errors) == 1
    assert database._event_claim_tokens == {}
    assert "event_processing_error" in pipeline.entry_gate.reasons
    async with database.sessions() as session:
        assert await session.get(TokenRow, "BATCH-CANCEL") is None
        claims = list(
            (
                await session.scalars(
                    select(EventDedupRow).order_by(EventDedupRow.event_id)
                )
            ).all()
        )
    assert [claim.processing_status for claim in claims] == ["FAILED", "FAILED"]
    await database.close()

@pytest.mark.asyncio
async def test_startup_reclaims_owned_processing_and_quarantines_poison_events(
    tmp_path,
) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'quarantine.db'}")
    await database.create_schema_for_tests()
    now = datetime(2026, 8, 25, tzinfo=timezone.utc)
    event = EventEnvelope(
        source=EventSource.REPLAY,
        protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.SWAP_SELL,
        slot=5,
        signature="poison-event",
        instruction_index=0,
        block_time=now,
        observed_at=now,
        mint="TOKEN",
        pool_address="POOL",
        payload={"base_amount_in": "1", "quote_amount_out": "1"},
    )

    assert await database.record_event(event) is True
    assert await database.load_unprocessed_events() == []
    immediate = await database.load_unprocessed_events(include_owned_processing=True)
    assert [row.event_id for row in immediate] == [event.event_id]

    await database.mark_event_failed(event.event_id, RuntimeError("attempt 1"))
    assert await database.record_event(event, reclaim=True) is True
    await database.mark_event_failed(event.event_id, RuntimeError("attempt 2"))
    assert await database.record_event(event, reclaim=True) is True
    await database.mark_event_failed(event.event_id, RuntimeError("attempt 3"))

    assert (
        await database.load_unprocessed_events(include_owned_processing=True)
        == []
    )
    assert await database.load_quarantined_event_protocols() == {"pumpswap"}
    await database.close()


@pytest.mark.asyncio
async def test_pipeline_reclaims_fresh_owned_processing_event(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'owned-processing.db'}")
    await database.create_schema_for_tests()
    now = datetime(2026, 8, 25, tzinfo=timezone.utc)
    event = EventEnvelope(
        source=EventSource.REPLAY,
        protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.SWAP_BUY,
        slot=6,
        signature="owned-processing",
        instruction_index=0,
        block_time=now,
        observed_at=now,
        mint="TOKEN",
        pool_address="POOL",
        payload={"base_amount_out": "1", "quote_amount_in": "1"},
    )
    assert await database.record_event(event) is True
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(BotMetrics()),
        metrics=BotMetrics(),
        database=database,
        record_raw=False,
    )

    assert await pipeline.process_event(event, recovering=True) is True

    async with database.sessions() as session:
        claim = await session.get(EventDedupRow, event.event_id)
        assert claim is not None
        assert claim.processing_status == "PROCESSED"
        assert claim.processing_attempts == 2
    await database.close()


def _database_batch_events(
    prefix: str,
    now: datetime,
    count: int,
) -> list[EventEnvelope]:
    return [
        EventEnvelope(
            source=EventSource.REPLAY,
            protocol=Protocol.PUMPSWAP,
            event_type=ChainEventType.SWAP_BUY,
            signature=f"{prefix}-{index}",
            slot=100_000 + index,
            instruction_index=0,
            block_time=now,
            observed_at=now,
            mint="mint-batch",
            pool_address="pool-batch",
            payload={"index": index},
        )
        for index in range(count)
    ]


@pytest.mark.asyncio
async def test_mark_events_failed_chunks_sqlite_bound_parameters(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'failure-chunks.db'}")
    await database.create_schema_for_tests()

    events = _database_batch_events(
        "failure-chunks",
        datetime.now(tz=timezone.utc),
        600,
    )
    update_parameter_counts: list[int] = []

    def capture_updates(
        _connection,
        _cursor,
        statement,
        parameters,
        _context,
        _executemany,
    ) -> None:
        normalized = " ".join(statement.lower().split())
        if normalized.startswith("update event_dedup set"):
            update_parameter_counts.append(len(parameters))

    try:
        assert await database.record_events(events) == [True] * len(events)

        sqlalchemy_event.listen(
            database.engine.sync_engine,
            "before_cursor_execute",
            capture_updates,
        )
        try:
            await database.mark_events_failed(
                [event.event_id for event in events],
                RuntimeError("batch failed"),
            )
        finally:
            sqlalchemy_event.remove(
                database.engine.sync_engine,
                "before_cursor_execute",
                capture_updates,
            )

        assert len(update_parameter_counts) > 1
        assert max(update_parameter_counts) <= SQLITE_SAFE_BOUND_PARAMETER_BUDGET
        assert database._event_claim_tokens == {}

        async with database.sessions() as session:
            rows = list((await session.scalars(select(EventDedupRow))).all())

        assert len(rows) == len(events)
        assert all(row.processing_status == "FAILED" for row in rows)
        assert all(row.processing_token is None for row in rows)
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_mark_events_failed_mixed_reclaimed_claim_is_atomic(tmp_path) -> None:
    url = f"sqlite+aiosqlite:///{tmp_path / 'mixed-claims.db'}"
    owner = Database(url)
    competitor = Database(url)
    await owner.create_schema_for_tests()

    active, stale = _database_batch_events(
        "mixed-claims",
        datetime.now(tz=timezone.utc),
        2,
    )

    try:
        assert await owner.record_events([active, stale]) == [True, True]
        original_tokens = dict(owner._event_claim_tokens)

        async with owner.sessions.begin() as session:
            stale_row = await session.get(EventDedupRow, stale.event_id)
            assert stale_row is not None
            stale_row.last_attempt_at = (
                datetime.now(tz=timezone.utc) - timedelta(minutes=3)
            )

        assert await competitor.record_event(stale) is True
        competitor_token = competitor._event_claim_tokens[stale.event_id]
        assert competitor_token != original_tokens[stale.event_id]

        with pytest.raises(RuntimeError, match="claim"):
            await owner.mark_events_failed(
                [active.event_id, stale.event_id],
                RuntimeError("batch failed"),
            )

        assert owner._event_claim_tokens == original_tokens

        async with owner.sessions() as session:
            active_row = await session.get(EventDedupRow, active.event_id)
            stale_row = await session.get(EventDedupRow, stale.event_id)

        assert active_row is not None
        assert active_row.processing_status == "PROCESSING"
        assert active_row.processing_token == original_tokens[active.event_id]

        assert stale_row is not None
        assert stale_row.processing_status == "PROCESSING"
        assert stale_row.processing_token == competitor_token
    finally:
        await competitor.close()
        await owner.close()


@pytest.mark.asyncio
async def test_mark_events_failed_reversed_overlap_uses_canonical_order(
    tmp_path,
) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'failure-locking.db'}")
    await database.create_schema_for_tests()

    first, second = _database_batch_events(
        "reversed-overlap",
        datetime.now(tz=timezone.utc),
        2,
    )
    event_ids = [first.event_id, second.event_id]
    lock_orders: list[tuple[str, ...]] = []

    def capture_lock_select(
        _connection,
        _cursor,
        statement,
        parameters,
        _context,
        _executemany,
    ) -> None:
        normalized = " ".join(statement.lower().split())
        if (
            normalized.startswith("select event_dedup.event_id")
            and "order by event_dedup.event_id" in normalized
        ):
            lock_orders.append(tuple(str(value) for value in parameters))

    start = asyncio.Event()

    async def fail_in_order(order: list[str]) -> None:
        await start.wait()
        await database.mark_events_failed(order, RuntimeError("batch failed"))

    try:
        assert await database.record_events([first, second]) == [True, True]

        sqlalchemy_event.listen(
            database.engine.sync_engine,
            "before_cursor_execute",
            capture_lock_select,
        )
        try:
            tasks = [
                asyncio.create_task(fail_in_order(event_ids)),
                asyncio.create_task(fail_in_order(list(reversed(event_ids)))),
            ]
            start.set()
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=2,
            )
        finally:
            sqlalchemy_event.remove(
                database.engine.sync_engine,
                "before_cursor_execute",
                capture_lock_select,
            )

        assert sum(result is None for result in results) == 1
        assert sum(isinstance(result, RuntimeError) for result in results) == 1
        assert lock_orders == [tuple(sorted(event_ids))]
        assert database._event_claim_tokens == {}

        async with database.sessions() as session:
            rows = list((await session.scalars(select(EventDedupRow))).all())

        assert len(rows) == 2
        assert all(row.processing_status == "FAILED" for row in rows)
        assert all(row.processing_token is None for row in rows)
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_pipeline_batch_task_cancellation_rolls_back_and_poison_restarts(
    tmp_path,
    monkeypatch,
) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'cancel-batch.db'}")
    await database.create_schema_for_tests()
    now = datetime.now(tz=timezone.utc)
    events = _pipeline_batch_events("task-cancel", now)
    fatal_errors: list[BaseException] = []
    metrics = BotMetrics()
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(metrics),
        metrics=metrics,
        database=database,
        fatal_handler=fatal_errors.append,
        record_raw=False,
    )
    _install_pipeline_batch_decoder(monkeypatch, pipeline, events)

    second_started = asyncio.Event()
    never_complete = asyncio.Event()
    applied = 0

    async def blocking_apply(
        _event,
        *,
        persist,
        observe,
        allow_candidate=True,
    ) -> None:
        nonlocal applied
        applied += 1
        if applied == 1:
            await database.upsert_token(
                TokenRecord(
                    mint="cancelled-batch-token",
                    creation_time=now,
                    updated_at=now,
                )
            )
            return

        second_started.set()
        await never_complete.wait()

    monkeypatch.setattr(pipeline, "_apply_event", blocking_apply)
    task = asyncio.create_task(_run_pipeline_batch(pipeline))

    try:
        await asyncio.wait_for(second_started.wait(), timeout=1)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=2)

        assert len(fatal_errors) == 1
        assert isinstance(fatal_errors[0], asyncio.CancelledError)
        assert database._event_claim_tokens == {}
        assert "event_processing_error" in pipeline.entry_gate.reasons

        async with database.sessions() as session:
            token = await session.get(TokenRow, "cancelled-batch-token")
            claims = {
                row.event_id: row
                for row in (
                    await session.scalars(select(EventDedupRow))
                ).all()
            }

        assert token is None
        assert set(claims) == {event.event_id for event in events}
        assert all(row.processing_status == "FAILED" for row in claims.values())
        assert all(row.processing_token is None for row in claims.values())

        with pytest.raises(RuntimeError, match="restart required"):
            await _run_pipeline_batch(pipeline)
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await database.close()

@pytest.mark.asyncio
async def test_pipeline_untracked_swaps_remain_durable_raw_and_processed(
    tmp_path,
    monkeypatch,
) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'filtered-swaps.db'}")
    await database.create_schema_for_tests()
    events = _pipeline_batch_events(
        "filtered-swap", datetime(2026, 8, 25, tzinfo=timezone.utc)
    )
    metrics = BotMetrics()
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(metrics),
        metrics=metrics,
        database=database,
        record_raw=True,
    )
    monkeypatch.setattr(
        pipeline._pumpswap,
        "decode_transaction",
        lambda _transaction, source: events,
    )
    archived: list[str] = []
    applied: list[str] = []

    async def capture_raw(batch: list[EventEnvelope]) -> None:
        archived.extend(event.event_id for event in batch)

    async def capture_apply(
        event: EventEnvelope,
        *,
        persist: bool,
        observe: bool,
        allow_candidate: bool = True,
    ) -> None:
        applied.append(event.event_id)

    monkeypatch.setattr(pipeline.recorder, "record_many", capture_raw)
    monkeypatch.setattr(pipeline, "_apply_event", capture_apply)

    await _run_pipeline_batch(pipeline)

    assert archived == [event.event_id for event in events]
    assert applied == []
    assert (
        metrics.chain_event_state_filter_decisions.labels(
            reason="unknown_pool"
        )._value.get()
        == len(events)
    )
    async with database.sessions() as session:
        for event in events:
            claim = await session.get(EventDedupRow, event.event_id)
            assert claim is not None
            assert claim.processing_status == "PROCESSED"
    assert database._event_claim_tokens == {}
    await database.close()

@pytest.mark.asyncio
async def test_load_processed_pool_creation_events_is_targeted(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'pool-bootstrap.db'}")
    await database.create_schema_for_tests()
    now = datetime(2026, 8, 25, tzinfo=timezone.utc)
    events = [
        EventEnvelope(
            source=EventSource.REPLAY,
            protocol=Protocol.PUMPSWAP,
            event_type=ChainEventType.POOL_CREATED,
            slot=1,
            signature="pool-a-created",
            instruction_index=0,
            block_time=now,
            observed_at=now,
            mint="TOKEN-A",
            pool_address="POOL-A",
            payload={},
        ),
        EventEnvelope(
            source=EventSource.REPLAY,
            protocol=Protocol.PUMPSWAP,
            event_type=ChainEventType.SWAP_BUY,
            slot=2,
            signature="pool-a-swap",
            instruction_index=0,
            block_time=now + timedelta(seconds=1),
            observed_at=now + timedelta(seconds=1),
            mint="TOKEN-A",
            pool_address="POOL-A",
            payload={},
        ),
        EventEnvelope(
            source=EventSource.REPLAY,
            protocol=Protocol.PUMPSWAP,
            event_type=ChainEventType.POOL_CREATED,
            slot=3,
            signature="pool-b-created",
            instruction_index=0,
            block_time=now + timedelta(seconds=2),
            observed_at=now + timedelta(seconds=2),
            mint="TOKEN-B",
            pool_address="POOL-B",
            payload={},
        ),
    ]
    assert await database.record_events(events) == [True, True, True]
    async with database.event_state_batch_transaction():
        for event in events:
            await database.mark_event_processed(
                event.event_id,
                processed_at=event.observed_at,
            )
    for event in events:
        database.release_event_claim(event.event_id)

    loaded = await database.load_processed_pool_creation_events({"POOL-A"})

    assert [event.event_id for event in loaded] == [events[0].event_id]
    assert await database.load_processed_pool_creation_events(set()) == []
    await database.close()
