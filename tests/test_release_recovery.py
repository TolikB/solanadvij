from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func, select

from sniper_bot.database import Database
from sniper_bot.db_models import RawArchiveSegmentRow, StreamRecoveryGapRow
from sniper_bot.events import (
    ChainEventType,
    EventEnvelope,
    EventSource,
    Protocol,
    RawEventRecorder,
)
from sniper_bot.metrics import BotMetrics
from sniper_bot.pipeline import ConfirmationPipeline
from sniper_bot.stream import EntryGate, HeliusStreamGateway


def _event(signature: str, *, sequence: int | None = None) -> EventEnvelope:
    now = datetime(2026, 9, 1, tzinfo=timezone.utc)
    return EventEnvelope(
        ingest_sequence=sequence,
        source=EventSource.REPLAY,
        protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.SWAP_BUY,
        slot=100,
        signature=signature,
        instruction_index=0,
        block_time=now,
        observed_at=now,
        pool_address="POOL",
        payload={"quote_amount_in": "1"},
    )


def _metric(rendered: str, name: str, stage: str) -> float:
    match = re.search(
        rf'^{re.escape(name)}\{{stage="{stage}"\}} ([0-9.eE+-]+)$',
        rendered,
        flags=re.MULTILINE,
    )
    assert match is not None
    return float(match.group(1))


@pytest.mark.asyncio
async def test_stage_backlog_counts_events_and_oldest_age() -> None:
    metrics = BotMetrics()
    pipeline = ConfirmationPipeline(
        data_dir=".",
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(metrics),
        metrics=metrics,
        record_raw=False,
    )
    pipeline._background_workers_started = True
    events = [_event("one"), _event("two"), _event("three")]

    await pipeline._process_decoded_event_batch(events)
    await asyncio.sleep(0.01)
    pipeline._sync_stage_metrics()
    rendered = metrics.render().decode("utf-8")

    assert _metric(rendered, "ingestion_backlog_events", "durable") == 3
    assert _metric(
        rendered,
        "ingestion_oldest_event_age_seconds",
        "durable",
    ) > 0

    item = pipeline._durable_queue.get_nowait()
    assert item is not None
    pipeline._durable_queue.task_done()
    pipeline._complete_stage("durable", item)
    pipeline._background_workers_started = False
    rendered = metrics.render().decode("utf-8")
    assert _metric(rendered, "ingestion_backlog_events", "durable") == 0


@pytest.mark.asyncio
async def test_recovery_gap_retries_update_one_pending_row_per_protocol(
    tmp_path: Path,
) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'gaps.db'}")
    await database.create_schema_for_tests()
    try:
        await database.record_stream_recovery_gap("timeout")
        await database.record_stream_recovery_gap("retry")

        async with database.sessions() as session:
            rows = list(
                (
                    await session.scalars(
                        select(StreamRecoveryGapRow).order_by(
                            StreamRecoveryGapRow.protocol
                        )
                    )
                ).all()
            )
        assert len(rows) == 2
        assert {row.protocol for row in rows} == {"pump", "pumpswap"}
        assert all(row.status == "PENDING" for row in rows)
        assert all(row.attempts == 2 for row in rows)
        assert all(row.reason == "retry" for row in rows)

        await database.resolve_stream_recovery_gaps()
        async with database.sessions() as session:
            statuses = list(
                (
                    await session.scalars(
                        select(StreamRecoveryGapRow.status)
                    )
                ).all()
            )
        assert statuses == ["RESOLVED", "RESOLVED"]
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_atomic_archive_rename_failure_leaves_no_segment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = RawEventRecorder(tmp_path)

    def fail_replace(_source: object, _target: object) -> None:
        raise OSError("fault injection: rename")

    monkeypatch.setattr("sniper_bot.events.os.replace", fail_replace)
    with pytest.raises(OSError, match="fault injection"):
        await recorder.write_segments([_event("rename", sequence=1)])

    assert list(tmp_path.rglob("*.ndjson.zst")) == []
    assert list(tmp_path.rglob("*.tmp")) == []


@pytest.mark.asyncio
async def test_archive_checkpoint_is_rebuilt_from_postgres_after_crash(
    tmp_path: Path,
) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'archive.db'}")
    await database.create_schema_for_tests()
    event = _event("archive-rebuild")
    result = (await database.record_events([event]))[0]
    assert result.accepted is True
    recorder = RawEventRecorder(tmp_path / "raw")
    await recorder.write_segments([event])
    metrics = BotMetrics()
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(metrics),
        metrics=metrics,
        database=database,
    )
    try:
        await pipeline.start_background_workers()
        await pipeline.stop_background_workers(timeout_seconds=5)
        async with database.sessions() as session:
            count = int(
                await session.scalar(
                    select(func.count()).select_from(RawArchiveSegmentRow)
                )
                or 0
            )
        assert count == 1
        rendered = metrics.render().decode("utf-8")
        assert _metric(rendered, "ingestion_backlog_events", "durable") == 0
        assert _metric(rendered, "ingestion_backlog_events", "state") == 0
        assert _metric(rendered, "ingestion_backlog_events", "archive") == 0
    finally:
        database.release_event_claim(event.event_id)
        await database.close()


class _RecoveryRpc:
    def __init__(self) -> None:
        self.active = 0
        self.max_active = 0
        self.pages = 0

    async def get_signatures_for_address(
        self,
        _address: str,
        *,
        until: str | None,
        before: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        assert limit == 1000
        if until != "checkpoint":
            return []
        self.pages += 1
        if before is None:
            return [
                {
                    "signature": f"signature-{index}",
                    "slot": 2_001 - index,
                    "err": None,
                }
                for index in range(1000)
            ]
        return [{"signature": "oldest", "slot": 1_000, "err": None}]

    async def get_transaction(self, signature: str) -> dict[str, Any]:
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(0)
            slot = (
                1_000
                if signature == "oldest"
                else 2_001 - int(signature.split("-")[1])
            )
            return {"signature": signature, "slot": slot}
        finally:
            self.active -= 1


@pytest.mark.asyncio
async def test_paginated_gap_recovery_is_ordered_and_bounded_concurrent() -> None:
    rpc = _RecoveryRpc()

    async def handle(
        _transaction: dict[str, Any],
        _source: EventSource,
    ) -> None:
        return None

    metrics = BotMetrics()
    gateway = HeliusStreamGateway(
        websocket_url="wss://example.invalid",
        rpc=rpc,  # type: ignore[arg-type]
        handler=handle,
        entry_gate=EntryGate(metrics),
        metrics=metrics,
        log_fetch_concurrency=20,
    )
    gateway.restore_protocol_checkpoint(Protocol.PUMP, "checkpoint")

    recovered = await gateway._recover_gap()

    assert len(recovered) == 1001
    slots = [int(item[1]["slot"]) for item in recovered]
    assert slots == sorted(slots)
    assert rpc.pages == 2
    assert 1 < rpc.max_active <= 20
    assert all(item[2] == EventSource.RPC_RECOVERY for item in recovered)