from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError
from sqlalchemy import func, select

from sniper_bot.config import AppConfig
from sniper_bot.database import Database, EventRecordResult
from sniper_bot.db_models import (
    RawArchiveSegmentRow,
    StreamProtocolCheckpointRow,
)
from sniper_bot.events import (
    ChainEventType,
    EventEnvelope,
    EventSource,
    Protocol,
    RawEventReader,
    RawEventRecorder,
)
from sniper_bot.metrics import BotMetrics
from sniper_bot.pipeline import ConfirmationPipeline
from sniper_bot.runtime import SniperRuntime
from sniper_bot.stream import EntryGate
from sniper_bot.telegram import NoopTelegramNotifier


def _event(
    signature: str,
    sequence: int | None = None,
) -> EventEnvelope:
    now = datetime(2026, 9, 1, tzinfo=timezone.utc)
    return EventEnvelope(
        ingest_sequence=sequence,
        source=EventSource.REPLAY,
        protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.SWAP_BUY,
        slot=100 + (sequence or 0),
        signature=signature,
        instruction_index=0,
        block_time=now,
        observed_at=now,
        pool_address="UNKNOWN_POOL",
        payload={"quote_amount_in": "1"},
    )


def _config(**overrides: object) -> AppConfig:
    values: dict[str, object] = {
        "APP_MODE": "paper",
        "HELIUS_API_KEY": "helius-key",
        "JUPITER_API_KEY": "jupiter-key",
        "POSTGRES_DSN": (
            "postgresql://user:pass@localhost:5432/db"
        ),
        "TELEGRAM_BOT_TOKEN": "telegram-token",
        "TELEGRAM_ADMIN_CHAT_ID": 123,
        "STARTING_EQUITY_USD": Decimal("500"),
    }
    values.update(overrides)
    return AppConfig(**values)


def test_ingest_sequence_is_not_part_of_event_identity() -> None:
    first = _event("same-signature", 1)
    second = _event("same-signature", 99)

    assert first.event_id == second.event_id


@pytest.mark.asyncio
async def test_raw_archive_segment_is_atomic_ordered_and_idempotent(
    tmp_path,
) -> None:
    recorder = RawEventRecorder(tmp_path)
    events = [_event("second", 2), _event("first", 1)]

    first = await recorder.write_segments(events)
    second = await recorder.write_segments(events)

    assert len(first) == 1
    assert first == second
    assert first[0].path.name == (
        "00000000000000000001-"
        "00000000000000000002.ndjson.zst"
    )
    assert first[0].start_sequence == 1
    assert first[0].end_sequence == 2
    assert [event.ingest_sequence for event in RawEventReader(tmp_path).iter_events()] == [
        1,
        2,
    ]
    assert list(tmp_path.rglob("*.tmp")) == []


@pytest.mark.asyncio
async def test_record_events_returns_typed_sequence_and_checkpoints(
    tmp_path,
) -> None:
    database = Database(
        f"sqlite+aiosqlite:///{tmp_path / 'durable.db'}"
    )
    await database.create_schema_for_tests()
    event = _event("typed-result")

    try:
        result = (await database.record_events([event]))[0]

        assert isinstance(result, EventRecordResult)
        assert result.accepted is True
        assert result.event_id == event.event_id
        assert result.ingest_sequence == 1
        assert event.ingest_sequence == 1
        assert bool(result) is True
        await database.save_stream_protocol_checkpoints(
            [event],
            stage="durable",
        )
        await database.mark_event_processed(
            event.event_id,
            processed_at=event.observed_at,
        )
        database.release_event_claim(event.event_id)
        await database.save_stream_protocol_checkpoints(
            [event],
            stage="state",
        )

        duplicate = (await database.record_events([event]))[0]
        assert duplicate.accepted is False
        assert duplicate.ingest_sequence == 0

        async with database.sessions() as session:
            checkpoint = await session.get(
                StreamProtocolCheckpointRow,
                Protocol.PUMPSWAP.value,
            )
        assert checkpoint is not None
        assert checkpoint.durable_ingest_sequence == 1
        assert checkpoint.state_sequence == 1
    finally:
        database.release_event_claim(event.event_id)
        await database.close()


@pytest.mark.asyncio
async def test_background_stages_drain_to_state_and_archive(
    tmp_path,
) -> None:
    database = Database(
        f"sqlite+aiosqlite:///{tmp_path / 'pipeline.db'}"
    )
    await database.create_schema_for_tests()
    metrics = BotMetrics()
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(metrics),
        metrics=metrics,
        database=database,
    )
    event = _event("background")

    try:
        await pipeline.start_background_workers()
        await pipeline._process_decoded_event_batch([event])
        await pipeline.stop_background_workers(
            timeout_seconds=5,
        )

        async with database.sessions() as session:
            archived = int(
                await session.scalar(
                    select(func.count())
                    .select_from(RawArchiveSegmentRow)
                )
                or 0
            )
            checkpoint = await session.get(
                StreamProtocolCheckpointRow,
                Protocol.PUMPSWAP.value,
            )
        assert archived == 1
        assert checkpoint is not None
        assert checkpoint.durable_ingest_sequence == 1
        assert checkpoint.state_sequence == 1
    finally:
        await database.close()


def test_telegram_disabled_uses_noop_and_no_outbox_worker(
    tmp_path,
) -> None:
    runtime = SniperRuntime(
        _config(
            TELEGRAM={
                "enabled": False,
                "daily_report_time": "00:00",
            }
        ),
        data_dir=tmp_path,
    )

    assert isinstance(runtime.notifier, NoopTelegramNotifier)
    assert runtime.outbox_worker is None


def test_image_revision_mismatch_fails_closed(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    revision_file = tmp_path / "REVISION"
    revision_file.write_text("a" * 40, encoding="ascii")
    monkeypatch.setenv("APP_REVISION_FILE", str(revision_file))

    with pytest.raises(
        ValidationError,
        match="does not match immutable image revision",
    ):
        _config(APP_REVISION="b" * 40)


def test_release_capacity_metrics_are_exported() -> None:
    metrics = BotMetrics()
    metrics.ingestion_backlog_events.labels(
        stage="durable"
    ).set(3)
    metrics.stream_recovery_gap_active.set(1)
    metrics.ingestion_events_dropped.labels(
        stage="notification"
    ).inc()
    metrics.shutdown_drain_seconds.labels(
        stage="pipeline"
    ).observe(0.5)

    rendered = metrics.render().decode("utf-8")
    assert 'ingestion_backlog_events{stage="durable"} 3.0' in rendered
    assert "stream_recovery_gap_active 1.0" in rendered
    assert (
        'ingestion_events_dropped_total{stage="notification"} 1.0'
        in rendered
    )
    assert (
        'shutdown_drain_seconds_count{stage="pipeline"} 1.0'
        in rendered
    )
