"""Fault injection at every ordered-ingestion boundary.

Each case fails one boundary, lets the release path recover, and then asserts
that the durable sequence, the archive sequence, and applied state reconcile
without a gap and without duplicate effects.
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func, select

from sniper_bot.database import Database
from sniper_bot.db_models import (
    EventDedupRow,
    RawArchiveSegmentRow,
    RawChainEventRow,
    StreamProtocolCheckpointRow,
)
from sniper_bot.events import (
    ChainEventType,
    EventEnvelope,
    EventSource,
    Protocol,
)
from sniper_bot.metrics import BotMetrics
from sniper_bot.pipeline import ConfirmationPipeline
from sniper_bot.stream import EntryGate


class InjectedFailure(RuntimeError):
    pass


def _event(index: int) -> EventEnvelope:
    now = datetime(2026, 9, 1, tzinfo=timezone.utc)
    return EventEnvelope(
        source=EventSource.REPLAY,
        protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.SWAP_BUY,
        slot=500 + index,
        signature=f"fault-{index:04d}",
        instruction_index=0,
        block_time=now,
        observed_at=now,
        pool_address="UNKNOWN_POOL",
        payload={"quote_amount_in": "1"},
    )


def _pipeline(
    database: Database,
    tmp_path: Path,
    *,
    record_raw: bool = True,
) -> ConfirmationPipeline:
    metrics = BotMetrics()
    return ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(metrics),
        metrics=metrics,
        database=database,
        record_raw=record_raw,
    )


@dataclass(frozen=True)
class Reconciliation:
    raw_rows: int
    dedup_rows: int
    processed_rows: int
    max_raw_sequence: int
    max_archived_sequence: int
    archive_segments: int
    durable_checkpoint: int
    state_checkpoint: int


async def _reconcile(database: Database) -> Reconciliation:
    async with database.sessions() as session:
        raw_rows = int(
            await session.scalar(
                select(func.count()).select_from(RawChainEventRow)
            )
            or 0
        )
        dedup_rows = int(
            await session.scalar(
                select(func.count()).select_from(EventDedupRow)
            )
            or 0
        )
        processed_rows = int(
            await session.scalar(
                select(func.count())
                .select_from(EventDedupRow)
                .where(EventDedupRow.processing_status == "PROCESSED")
            )
            or 0
        )
        max_raw_sequence = int(
            await session.scalar(
                select(func.max(RawChainEventRow.ingest_sequence))
            )
            or 0
        )
        max_archived_sequence = int(
            await session.scalar(
                select(func.max(RawArchiveSegmentRow.end_sequence))
            )
            or 0
        )
        archive_segments = int(
            await session.scalar(
                select(func.count()).select_from(RawArchiveSegmentRow)
            )
            or 0
        )
        checkpoint = await session.get(
            StreamProtocolCheckpointRow, Protocol.PUMPSWAP.value
        )
    return Reconciliation(
        raw_rows=raw_rows,
        dedup_rows=dedup_rows,
        processed_rows=processed_rows,
        max_raw_sequence=max_raw_sequence,
        max_archived_sequence=max_archived_sequence,
        archive_segments=archive_segments,
        durable_checkpoint=(
            checkpoint.durable_ingest_sequence if checkpoint else 0
        ),
        state_checkpoint=checkpoint.state_sequence if checkpoint else 0,
    )


async def _database(tmp_path: Path, name: str) -> Database:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / name}")
    await database.create_schema_for_tests()
    return database


@pytest.mark.asyncio
async def test_failure_before_the_raw_commit_recovers_without_duplicates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = await _database(tmp_path, "before_raw.db")
    pipeline = _pipeline(database, tmp_path)
    events = [_event(index) for index in range(4)]
    original = database.record_events
    attempts = 0

    async def failing_record(
        batch: list[EventEnvelope], **kwargs: Any
    ) -> list[Any]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise InjectedFailure("durable commit failed before any raw write")
        return await original(batch, **kwargs)

    monkeypatch.setattr(database, "record_events", failing_record)

    try:
        await pipeline.start_background_workers()
        await pipeline._process_decoded_event_batch(events)
        await pipeline.stop_background_workers(timeout_seconds=5)

        state = await _reconcile(database)
        assert attempts == 2
        assert state.raw_rows == len(events)
        assert state.dedup_rows == len(events)
        assert state.processed_rows == len(events)
        assert state.max_raw_sequence == len(events)
        assert state.max_archived_sequence == len(events)
        assert state.durable_checkpoint == len(events)
        assert state.state_checkpoint == len(events)
        assert "durable_ingest_error" not in pipeline.entry_gate.reasons
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_failure_after_the_raw_commit_reuses_the_same_sequence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = await _database(tmp_path, "after_raw.db")
    pipeline = _pipeline(database, tmp_path)
    events = [_event(index) for index in range(3)]
    original = database.save_stream_protocol_checkpoints
    attempts = 0

    async def failing_checkpoint(
        batch: list[EventEnvelope], *, stage: str
    ) -> None:
        nonlocal attempts
        if stage == "durable":
            attempts += 1
            if attempts == 1:
                raise InjectedFailure(
                    "checkpoint failed after the raw commit landed"
                )
        await original(batch, stage=stage)

    monkeypatch.setattr(
        database, "save_stream_protocol_checkpoints", failing_checkpoint
    )

    try:
        await pipeline.start_background_workers()
        await pipeline._process_decoded_event_batch(events)
        await pipeline.stop_background_workers(timeout_seconds=5)

        state = await _reconcile(database)
        assert attempts == 2
        assert state.raw_rows == len(events)
        assert state.dedup_rows == len(events)
        assert state.processed_rows == len(events)
        assert state.max_raw_sequence == len(events)
        assert state.durable_checkpoint == len(events)
        assert state.state_checkpoint == len(events)
        assert [event.ingest_sequence for event in events] == [1, 2, 3]
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_archive_segment_record_failure_leaves_one_segment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = await _database(tmp_path, "archive_record.db")
    pipeline = _pipeline(database, tmp_path)
    events = [_event(index) for index in range(2)]
    original = database.record_raw_archive_segments
    attempts = 0

    async def failing_record_segments(segments: list[Any]) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise InjectedFailure(
                "segment metadata failed after the file was published"
            )
        await original(segments)

    monkeypatch.setattr(
        database, "record_raw_archive_segments", failing_record_segments
    )

    try:
        await pipeline.start_background_workers()
        await pipeline._process_decoded_event_batch(events)
        await pipeline.stop_background_workers(timeout_seconds=5)

        state = await _reconcile(database)
        assert attempts == 2
        assert state.archive_segments == 1
        assert state.max_archived_sequence == len(events)
        assert state.max_raw_sequence == len(events)
        assert state.processed_rows == len(events)
        assert len(list((tmp_path / "raw").rglob("*.ndjson.zst"))) == 1
        assert list((tmp_path / "raw").rglob("*.tmp")) == []
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_state_commit_failure_fails_closed_and_restart_applies_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = await _database(tmp_path, "state_commit.db")
    pipeline = _pipeline(database, tmp_path, record_raw=False)
    events = [_event(index) for index in range(2)]

    async def failing_state_batch(*_args: Any, **_kwargs: Any) -> None:
        raise InjectedFailure("state commit failed")

    monkeypatch.setattr(
        pipeline, "_process_claimed_event_batch", failing_state_batch
    )

    try:
        await pipeline.start_background_workers()
        await pipeline._process_decoded_event_batch(events)
        with pytest.raises(InjectedFailure):
            await pipeline.stop_background_workers(timeout_seconds=5)

        crashed = await _reconcile(database)
        assert crashed.raw_rows == len(events)
        assert crashed.dedup_rows == len(events)
        assert crashed.processed_rows == 0
        assert crashed.durable_checkpoint == len(events)
        assert crashed.state_checkpoint == 0
        assert "state_apply_error" in pipeline.entry_gate.reasons

        for event in events:
            database.release_event_claim(event.event_id)

        restarted = _pipeline(database, tmp_path, record_raw=False)
        pending = await database.load_unprocessed_events(
            include_owned_processing=True
        )
        assert {event.event_id for event in pending} == {
            event.event_id for event in events
        }
        for event in pending:
            await restarted.process_event(event, recovering=True)

        recovered = await _reconcile(database)
        assert recovered.raw_rows == len(events)
        assert recovered.dedup_rows == len(events)
        assert recovered.processed_rows == len(events)
        assert recovered.max_raw_sequence == len(events)
        assert recovered.durable_checkpoint == len(events)
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_unfinished_archive_segment_is_rebuilt_on_restart(
    tmp_path: Path,
) -> None:
    database = await _database(tmp_path, "archive_rebuild.db")
    pipeline = _pipeline(database, tmp_path, record_raw=False)
    events = [_event(index) for index in range(3)]

    try:
        await pipeline.start_background_workers()
        await pipeline._process_decoded_event_batch(events)
        await pipeline.stop_background_workers(timeout_seconds=5)

        without_archive = await _reconcile(database)
        assert without_archive.archive_segments == 0
        assert without_archive.max_raw_sequence == len(events)

        restarted = _pipeline(database, tmp_path, record_raw=True)
        await restarted.start_background_workers()
        await restarted.stop_background_workers(timeout_seconds=5)

        rebuilt = await _reconcile(database)
        assert rebuilt.archive_segments == 1
        assert rebuilt.max_archived_sequence == rebuilt.max_raw_sequence
        assert rebuilt.raw_rows == len(events)
        assert rebuilt.dedup_rows == len(events)
        assert rebuilt.processed_rows == len(events)
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_state_failure_releases_a_batch_pulled_ahead_of_the_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("sniper_bot.pipeline.MAX_EVENT_BATCH_SIZE", 3)
    database = await _database(tmp_path, "carried_state.db")
    pipeline = _pipeline(database, tmp_path, record_raw=False)
    first_batch = [_event(index) for index in range(2)]
    second_batch = [_event(index) for index in range(2, 4)]

    async def failing_state_batch(*_args: Any, **_kwargs: Any) -> None:
        raise InjectedFailure("state commit failed mid-backlog")

    try:
        await database.record_events(first_batch)
        await database.record_events(second_batch)
        await pipeline.start_background_workers()
        monkeypatch.setattr(
            pipeline, "_process_claimed_event_batch", failing_state_batch
        )
        await pipeline._enqueue_stage(
            "state", pipeline._state_queue, first_batch
        )
        await pipeline._enqueue_stage(
            "state", pipeline._state_queue, second_batch
        )

        with pytest.raises(InjectedFailure):
            await pipeline.stop_background_workers(timeout_seconds=5)

        assert pipeline._stage_pending["state"] == deque()
        assert "stage_tracking_error" not in pipeline.entry_gate.reasons
        assert "state_apply_error" in pipeline.entry_gate.reasons

        crashed = await _reconcile(database)
        assert crashed.processed_rows == 0
        assert crashed.raw_rows == len(first_batch) + len(second_batch)
    finally:
        for event in first_batch + second_batch:
            database.release_event_claim(event.event_id)
        await database.close()

@pytest.mark.asyncio
@pytest.mark.parametrize("coalesced", [False, True])
async def test_inflight_notification_duplicate_is_applied_and_archived_once(
    tmp_path: Path, coalesced: bool
) -> None:
    database = await _database(tmp_path, "inflight_duplicate.db")
    pipeline = _pipeline(database, tmp_path)
    events = [_event(0), _event(1)]

    try:
        await pipeline._apply_durable_batch(events)
        # State is deliberately still queued when Helius repeats an event.
        await pipeline._apply_durable_batch([_event(0)])
        queued_state = []
        while not pipeline._state_queue.empty():
            item = pipeline._state_queue.get_nowait()
            assert item is not None
            queued_state.append(item)

        assert [event.event_id for item in queued_state for event in item.events] == [
            event.event_id for event in events
        ]
        if coalesced:
            await pipeline._apply_state_batch(
                [event for item in queued_state for event in item.events]
            )
        else:
            for item in queued_state:
                await pipeline._apply_state_batch(item.events)
        for item in queued_state:
            pipeline._state_queue.task_done()
            pipeline._complete_stage("state", item)

        archived_ids = []
        while not pipeline._archive_queue.empty():
            item = pipeline._archive_queue.get_nowait()
            assert item is not None
            archived_ids.extend(event.event_id for event in item.events)
            await pipeline._apply_archive_batch(item.events)
            pipeline._archive_queue.task_done()
            pipeline._complete_stage("archive", item)

        assert archived_ids == [event.event_id for event in events]
        state = await _reconcile(database)
        assert state.raw_rows == state.dedup_rows == state.processed_rows == 2
        assert state.max_raw_sequence == state.max_archived_sequence == 2
        assert state.durable_checkpoint == state.state_checkpoint == 2
        assert database._event_claim_tokens == {}
        assert pipeline._state_poisoned is False
    finally:
        await database.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "boundary", ["raw_before", "raw_after", "checkpoint_after", "archive_before"]
)
async def test_mixed_inflight_batch_retry_has_one_handoff_per_stage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, boundary: str
) -> None:
    database = await _database(tmp_path, "mixed_retry.db")
    pipeline = _pipeline(database, tmp_path)
    first, second = _event(0), _event(1)
    await pipeline._apply_durable_batch([first])
    original_record = database.record_events
    original_checkpoint = database.save_stream_protocol_checkpoints
    original_enqueue = pipeline._enqueue_stage
    attempts = 0
    claim_batches: list[list[str]] = []

    async def failing_record(
        batch: list[EventEnvelope], **kwargs: Any
    ) -> list[Any]:
        nonlocal attempts
        claim_batches.append([event.event_id for event in batch])
        if boundary == "raw_before":
            attempts += 1
            if attempts == 1:
                raise InjectedFailure("raw commit did not start")
        result = await original_record(batch, **kwargs)
        if boundary == "raw_after":
            attempts += 1
            if attempts == 1:
                raise InjectedFailure("raw commit outcome is ambiguous")
        return result

    async def failing_checkpoint(
        batch: list[EventEnvelope], *, stage: str
    ) -> None:
        nonlocal attempts
        await original_checkpoint(batch, stage=stage)
        if boundary == "checkpoint_after" and stage == "durable":
            attempts += 1
            if attempts == 1:
                raise InjectedFailure("durable checkpoint commit outcome is ambiguous")

    async def failing_enqueue(
        stage: str, queue: Any, batch: list[EventEnvelope]
    ) -> None:
        nonlocal attempts
        if boundary == "archive_before" and stage == "archive":
            attempts += 1
            if attempts == 1:
                raise InjectedFailure("archive handoff failed after state handoff")
        await original_enqueue(stage, queue, batch)

    monkeypatch.setattr(database, "record_events", failing_record)
    monkeypatch.setattr(database, "save_stream_protocol_checkpoints", failing_checkpoint)
    monkeypatch.setattr(pipeline, "_enqueue_stage", failing_enqueue)

    try:
        await pipeline._apply_durable_batch([_event(0), second])
        assert claim_batches
        assert all(batch == [second.event_id] for batch in claim_batches)
        assert attempts == 2

        state_ids = []
        while not pipeline._state_queue.empty():
            item = pipeline._state_queue.get_nowait()
            assert item is not None
            state_ids.extend(event.event_id for event in item.events)
            await pipeline._apply_state_batch(item.events)
            pipeline._state_queue.task_done()
            pipeline._complete_stage("state", item)
        archived_ids = []
        while not pipeline._archive_queue.empty():
            item = pipeline._archive_queue.get_nowait()
            assert item is not None
            archived_ids.extend(event.event_id for event in item.events)
            await pipeline._apply_archive_batch(item.events)
            pipeline._archive_queue.task_done()
            pipeline._complete_stage("archive", item)

        expected_ids = [first.event_id, second.event_id]
        assert state_ids == archived_ids == expected_ids
        state = await _reconcile(database)
        assert state.raw_rows == state.dedup_rows == state.processed_rows == 2
        assert state.max_raw_sequence == state.max_archived_sequence == 2
        assert state.durable_checkpoint == state.state_checkpoint == 2
        assert database._event_claim_tokens == {}
        assert pipeline._state_poisoned is False
        assert "durable_ingest_error" not in pipeline.entry_gate.reasons
    finally:
        await database.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "boundary", ["raw_after", "checkpoint_after", "state_before", "archive_before"]
)
async def test_cancelled_durable_handoff_reconciles_after_process_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, boundary: str
) -> None:
    database = await _database(tmp_path, "cancelled_handoff.db")
    pipeline = _pipeline(database, tmp_path)
    events = [_event(0), _event(1)]
    original_record = database.record_events
    original_checkpoint = database.save_stream_protocol_checkpoints
    original_enqueue = pipeline._enqueue_stage

    async def cancelled_record(
        batch: list[EventEnvelope], **kwargs: Any
    ) -> list[Any]:
        result = await original_record(batch, **kwargs)
        if boundary == "raw_after":
            raise asyncio.CancelledError
        return result

    async def cancelled_checkpoint(
        batch: list[EventEnvelope], *, stage: str
    ) -> None:
        await original_checkpoint(batch, stage=stage)
        if boundary == "checkpoint_after" and stage == "durable":
            raise asyncio.CancelledError

    async def cancelled_enqueue(
        stage: str, queue: Any, batch: list[EventEnvelope]
    ) -> None:
        if boundary == f"{stage}_before":
            raise asyncio.CancelledError
        await original_enqueue(stage, queue, batch)

    monkeypatch.setattr(database, "record_events", cancelled_record)
    monkeypatch.setattr(database, "save_stream_protocol_checkpoints", cancelled_checkpoint)
    monkeypatch.setattr(pipeline, "_enqueue_stage", cancelled_enqueue)
    try:
        with pytest.raises(asyncio.CancelledError):
            await pipeline._apply_durable_batch(events)
        assert (await _reconcile(database)).raw_rows == len(events)
    finally:
        await database.close()

    # A new Database instance has no process-local tokens, as on a real restart.
    recovered_database = Database(
        f"sqlite+aiosqlite:///{tmp_path / 'cancelled_handoff.db'}"
    )
    restarted = _pipeline(recovered_database, tmp_path)
    try:
        pending = await recovered_database.load_unprocessed_events(
            include_owned_processing=True
        )
        assert [event.event_id for event in pending] == [
            event.event_id for event in events
        ]
        for event in pending:
            await restarted.process_event(event, recovering=True)
        await restarted.start_background_workers()
        await restarted.stop_background_workers(timeout_seconds=5)

        recovered = await _reconcile(recovered_database)
        assert recovered.raw_rows == recovered.dedup_rows == recovered.processed_rows == 2
        assert recovered.max_raw_sequence == recovered.max_archived_sequence == 2
        assert recovered_database._event_claim_tokens == {}
        assert restarted._state_poisoned is False
    finally:
        await recovered_database.close()
