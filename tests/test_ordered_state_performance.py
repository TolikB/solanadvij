from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from sniper_bot.database import Database
from sniper_bot.db_models import EventDedupRow, StreamProtocolCheckpointRow
from sniper_bot.events import ChainEventType, EventEnvelope, EventSource, Protocol
from sniper_bot.metrics import BotMetrics
from sniper_bot.pipeline import ConfirmationPipeline
from sniper_bot.solana_rpc import SolanaRpcClient
from sniper_bot.stream import EntryGate, HeliusStreamGateway, _TransactionNotification


def _event(
    signature: str,
    instruction_index: int = 0,
    *,
    slot: int = 100,
    protocol: Protocol = Protocol.PUMPSWAP,
) -> EventEnvelope:
    now = datetime(2026, 8, 25, tzinfo=timezone.utc)
    return EventEnvelope(
        source=EventSource.HELIUS_WSS,
        protocol=protocol,
        event_type=ChainEventType.SWAP_BUY,
        slot=slot,
        signature=signature,
        instruction_index=instruction_index,
        block_time=now,
        observed_at=now,
        mint="TOKEN",
        pool_address="POOL",
        payload={"base_amount_out": "1", "quote_amount_in": "1"},
    )


@pytest.mark.asyncio
async def test_state_worker_coalesces_backlog_batches(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'state_coalesce.db'}")
    await database.create_schema_for_tests()
    metrics = BotMetrics()
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(metrics),
        metrics=metrics,
        database=database,
        record_raw=False,
    )

    batch_calls: list[int] = []
    original_process = pipeline._process_claimed_event_batch

    async def tracked_process(events, results, archive_raw=False):
        batch_calls.append(len(events))
        return await original_process(events, results, archive_raw=archive_raw)

    pipeline._process_claimed_event_batch = tracked_process

    # Start background workers
    await pipeline.start_background_workers()

    # Pre-claim 5 separate small batches in database and directly enqueue into state queue
    all_events = []
    for b in range(5):
        events = [_event(f"coalesce-sig-{b}-{i}", i, slot=200 + b) for i in range(4)]
        all_events.extend(events)
        await database.record_events(events)

    # Put batches into state queue
    for b in range(5):
        batch_events = all_events[b * 4 : (b + 1) * 4]
        await pipeline._enqueue_stage("state", pipeline._state_queue, batch_events)

    # Allow worker to process
    await pipeline._state_queue.join()
    await pipeline.stop_background_workers(timeout_seconds=5.0)

    # Should have coalesced 5 batches of 4 events (20 total) into fewer than 5 calls
    assert sum(batch_calls) == 20
    assert len(batch_calls) < 5

    # Check that all events are marked PROCESSED
    async with database.sessions() as session:
        claims = (
            await session.scalars(
                select(EventDedupRow).where(
                    EventDedupRow.event_id.in_([e.event_id for e in all_events])
                )
            )
        ).all()
    assert len(claims) == 20
    assert all(c.processing_status == "PROCESSED" for c in claims)

    # Check that stage metrics / FIFO tracking is clean
    assert len(pipeline._stage_pending["state"]) == 0
    assert "stage_tracking_error" not in pipeline.entry_gate.reasons

    await database.close()


@pytest.mark.asyncio
async def test_durable_worker_coalesces_backlog_batches(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'durable_coalesce.db'}")
    await database.create_schema_for_tests()
    metrics = BotMetrics()
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(metrics),
        metrics=metrics,
        database=database,
        record_raw=False,
    )

    record_calls: list[int] = []
    original_record = database.record_events

    async def tracked_record(events, **kwargs):
        record_calls.append(len(events))
        return await original_record(events, **kwargs)

    database.record_events = tracked_record

    await pipeline.start_background_workers()

    # Enqueue multiple batches into durable queue
    all_events = []
    for b in range(4):
        events = [_event(f"durable-sig-{b}-{i}", i, slot=300 + b) for i in range(5)]
        all_events.extend(events)
        await pipeline._enqueue_stage("durable", pipeline._durable_queue, events)

    await pipeline._durable_queue.join()
    await pipeline._state_queue.join()
    await pipeline.stop_background_workers(timeout_seconds=5.0)

    # Durable worker should have coalesced into fewer calls to record_events
    assert sum(record_calls) == 20
    assert len(record_calls) < 4
    assert len(pipeline._stage_pending["durable"]) == 0
    assert "stage_tracking_error" not in pipeline.entry_gate.reasons

    await database.close()


@pytest.mark.asyncio
async def test_state_checkpoint_is_atomic_with_event_state_batch(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'atomic_ckpt.db'}")
    await database.create_schema_for_tests()
    metrics = BotMetrics()
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(metrics),
        metrics=metrics,
        database=database,
        record_raw=False,
    )

    events = [_event(f"atomic-ckpt-{i}", i, slot=400) for i in range(2)]
    results = await database.record_events(events)
    assert all(r.accepted for r in results)

    # Process claimed batch with archive_raw=False (state stage)
    await pipeline._process_claimed_event_batch(events, results, archive_raw=False)

    # Check that state checkpoint was committed together with processed status
    async with database.sessions() as session:
        checkpoint = await session.get(
            StreamProtocolCheckpointRow,
            Protocol.PUMPSWAP.value,
        )
        assert checkpoint is not None
        assert checkpoint.state_sequence == 2

        claims = (
            await session.scalars(
                select(EventDedupRow).where(
                    EventDedupRow.event_id.in_([e.event_id for e in events])
                )
            )
        ).all()
        assert all(c.processing_status == "PROCESSED" for c in claims)

    for event in events:
        database.release_event_claim(event.event_id)
    await database.close()


@pytest.mark.asyncio
async def test_stop_background_workers_cancels_cleanly_on_timeout(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'timeout_clean.db'}")
    await database.create_schema_for_tests()
    metrics = BotMetrics()
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(metrics),
        metrics=metrics,
        database=database,
        record_raw=False,
    )

    await pipeline.start_background_workers()
    stage_tasks = list(pipeline._stage_tasks)
    assert len(stage_tasks) >= 2

    # Block state worker indefinitely
    hang_event = asyncio.Event()

    async def hanging_process(*args, **kwargs):
        await hang_event.wait()

    pipeline._process_claimed_event_batch = hanging_process

    event = _event("hang-sig", 0)
    await database.record_events([event])
    await pipeline._enqueue_stage("state", pipeline._state_queue, [event])

    with pytest.raises(RuntimeError, match="ordered ingestion queues did not drain within"):
        await pipeline.stop_background_workers(timeout_seconds=0.1)

    # Verify tasks were cancelled and cleaned up
    assert all(task.done() for task in stage_tasks)
    assert pipeline._stage_tasks == []
    assert pipeline._background_workers_started is False

    await database.close()


@pytest.mark.asyncio
async def test_stream_stop_cleans_up_even_if_notification_queue_times_out() -> None:
    entry_gate = EntryGate(BotMetrics())
    metrics = BotMetrics()
    gateway = HeliusStreamGateway(
        websocket_url="wss://example.invalid",
        rpc=SolanaRpcClient("https://example.invalid"),
        handler=AsyncMock(),
        entry_gate=entry_gate,
        metrics=metrics,
        notification_queue_size=1,
    )
    gateway.SHUTDOWN_DRAIN_TIMEOUT_SECONDS = 0.1

    # Place item in notification queue and don't dispatch it
    gateway._notification_queue.put_nowait(
        _TransactionNotification(
            transaction={"slot": 1},
            received_at=datetime.now(tz=timezone.utc),
            generation=1,
        )
    )

    # Do not start dispatch task so notification queue will never drain
    with pytest.raises(RuntimeError, match="Solana ingress queues did not drain within"):
        await gateway.stop()

    # Verify worker task and fetch tasks are cancelled and cleaned up
    assert gateway._worker_task is None
    assert gateway._notification_dispatch_task is None
    assert len(gateway._log_fetch_tasks) == 0
