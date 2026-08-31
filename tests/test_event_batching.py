from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import event as sqlalchemy_event
from sqlalchemy import func, select, update

from sniper_bot.database import (
    MAX_EVENT_BATCH_SIZE,
    SQLITE_SAFE_BOUND_PARAMETER_BUDGET,
    Database,
)
from sniper_bot.db_models import EventDedupRow, RawChainEventRow, TokenRow
from sniper_bot.events import ChainEventType, EventEnvelope, EventSource, Protocol
from sniper_bot.metrics import BotMetrics
from sniper_bot.pipeline import EVENT_LOOP_YIELD_INTERVAL, ConfirmationPipeline
from sniper_bot.registry import TokenRecord
from sniper_bot.solana_rpc import SolanaRpcClient
from sniper_bot.stream import EntryGate, HeliusStreamGateway, TransactionItem


def _event(
    signature: str, instruction_index: int, *, slot: int | None = None
) -> EventEnvelope:
    now = datetime(2026, 8, 25, tzinfo=timezone.utc)
    return EventEnvelope(
        source=EventSource.REPLAY,
        protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.SWAP_BUY,
        slot=slot if slot is not None else 100 + instruction_index,
        signature=signature,
        instruction_index=instruction_index,
        block_time=now,
        observed_at=now,
        mint="TOKEN",
        pool_address="POOL",
        payload={"base_amount_out": "1", "quote_amount_in": "1"},
    )


@pytest.mark.asyncio
async def test_pipeline_large_decode_batch_yields_control(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(BotMetrics()),
        metrics=BotMetrics(),
        database=None,
        record_raw=False,
    )
    monkeypatch.setattr(
        pipeline._pumpswap,
        "decode_transaction",
        lambda _transaction, source: [],
    )
    yield_delays: list[float] = []

    async def tracked_sleep(delay: float) -> None:
        yield_delays.append(delay)

    monkeypatch.setattr("sniper_bot.pipeline.asyncio.sleep", tracked_sleep)
    transaction_count = EVENT_LOOP_YIELD_INTERVAL * 2 + 1
    await pipeline.process_transactions(
        [
            (
                Protocol.PUMPSWAP,
                {"signature": f"yield-{index}"},
                EventSource.REPLAY,
            )
            for index in range(transaction_count)
        ]
    )

    assert yield_delays == [0, 0]


@pytest.mark.asyncio
async def test_record_events_batches_claims_and_resumes_owned_suffix(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'batch.db'}")
    await database.create_schema_for_tests()
    events = [_event("first", 0), _event("second", 1)]

    assert await database.record_events(events) == [True, True]
    async with database.event_state_transaction():
        await database.mark_event_processed(events[0].event_id, processed_at=events[0].block_time)
    database.release_event_claim(events[0].event_id)

    assert await database.record_events(events, resume_owned=True) == [False, True]
    async with database.event_state_transaction():
        await database.mark_event_processed(events[1].event_id, processed_at=events[1].block_time)
    database.release_event_claim(events[1].event_id)

    assert await database.load_unprocessed_events(include_owned_processing=True) == []
    await database.close()


@pytest.mark.asyncio
async def test_record_events_reuses_stable_token_across_claim_retry(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'stable-token.db'}")
    await database.create_schema_for_tests()
    event = _event("ambiguous-commit", 0)
    database._event_claim_tokens[event.event_id] = "stable-claim-token"

    assert await database.record_events([event]) == [True]
    assert database._event_claim_tokens[event.event_id] == "stable-claim-token"
    assert await database.record_events([event], resume_owned=True) == [True]

    await database.close()


@pytest.mark.asyncio
async def test_stream_worker_dispatches_one_ordered_micro_batch() -> None:
    received: list[str] = []

    async def single_handler(*_args: Any) -> None:
        raise AssertionError("single handler must not run when batch handler is configured")

    async def batch_handler(items: list[TransactionItem]) -> None:
        received.extend(str(transaction["signature"]) for _, transaction, _ in items)

    gateway = HeliusStreamGateway(
        websocket_url="wss://example.invalid",
        rpc=SolanaRpcClient("https://example.invalid"),
        handler=single_handler,
        batch_handler=batch_handler,
        entry_gate=EntryGate(BotMetrics()),
        metrics=BotMetrics(),
    )
    block_time = int(datetime(2026, 8, 25, tzinfo=timezone.utc).timestamp())
    for signature, slot in (("first", 1), ("second", 2), ("third", 3)):
        gateway._queue.put_nowait(
            (
                Protocol.PUMPSWAP,
                {"signature": signature, "slot": slot, "blockTime": block_time},
                EventSource.HELIUS_WSS,
            )
        )
    gateway._queue.put_nowait(None)

    await gateway._worker()

    assert received == ["first", "second", "third"]
    assert gateway.last_processed_block_time == datetime.fromtimestamp(
        block_time, tz=timezone.utc
    )

@pytest.mark.asyncio
async def test_stream_worker_exhaustion_is_bounded_and_requests_restart() -> None:
    attempts = 0
    fatal_errors: list[BaseException] = []

    async def single_handler(*_args: Any) -> None:
        return None

    async def failing_batch_handler(_items: list[TransactionItem]) -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("persistent batch failure")

    gateway = HeliusStreamGateway(
        websocket_url="wss://example.invalid",
        rpc=SolanaRpcClient("https://example.invalid"),
        handler=single_handler,
        batch_handler=failing_batch_handler,
        fatal_handler=fatal_errors.append,
        entry_gate=EntryGate(BotMetrics()),
        metrics=BotMetrics(),
    )
    gateway.PROCESSING_BATCH_WINDOW_SECONDS = 0
    gateway.PROCESSING_RETRY_DELAYS = (0, 0)
    gateway._queue.put_nowait(
        (
            Protocol.PUMPSWAP,
            {"signature": "failure", "slot": 1},
            EventSource.HELIUS_WSS,
        )
    )

    with pytest.raises(RuntimeError, match="persistent batch failure"):
        await gateway._worker()

    assert attempts == gateway.PROCESSING_RETRY_LIMIT
    assert len(fatal_errors) == 1
    assert gateway.entry_gate.enabled is False
    assert "event_processing_error" in gateway.entry_gate.reasons

@pytest.mark.asyncio
async def test_same_slot_recovery_preserves_durable_ingest_order(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'same-slot.db'}")
    await database.create_schema_for_tests()
    events = [
        _event("z-received-first", 0, slot=500),
        _event("a-received-second", 0, slot=500),
    ]

    assert await database.record_events(events) == [True, True]
    recovered = await database.load_unprocessed_events(include_owned_processing=True)

    assert [event.signature for event in recovered] == [
        "z-received-first",
        "a-received-second",
    ]
    await database.close()


@pytest.mark.asyncio
async def test_stop_drains_full_queue_before_cancelling_worker() -> None:
    batch_started = asyncio.Event()
    release_batch = asyncio.Event()
    received: list[str] = []

    async def single_handler(*_args: Any) -> None:
        return None

    async def blocked_batch_handler(items: list[TransactionItem]) -> None:
        received.extend(str(transaction["signature"]) for _, transaction, _ in items)
        if received == ["active"]:
            batch_started.set()
            await release_batch.wait()

    gateway = HeliusStreamGateway(
        websocket_url="wss://example.invalid",
        rpc=SolanaRpcClient("https://example.invalid"),
        handler=single_handler,
        batch_handler=blocked_batch_handler,
        entry_gate=EntryGate(BotMetrics()),
        metrics=BotMetrics(),
        queue_size=1,
    )
    gateway.PROCESSING_BATCH_WINDOW_SECONDS = 0
    gateway._worker_task = asyncio.create_task(gateway._worker())
    gateway._queue.put_nowait(
        (Protocol.PUMPSWAP, {"signature": "active", "slot": 1}, EventSource.HELIUS_WSS)
    )
    await asyncio.wait_for(batch_started.wait(), timeout=1)
    gateway._queue.put_nowait(
        (Protocol.PUMPSWAP, {"signature": "queued", "slot": 2}, EventSource.HELIUS_WSS)
    )

    stop_task = asyncio.create_task(gateway.stop())
    await asyncio.sleep(0)
    assert stop_task.done() is False
    release_batch.set()
    await asyncio.wait_for(stop_task, timeout=1)

    assert received == ["active", "queued"]
    assert gateway._worker_task is None

@pytest.mark.asyncio
async def test_checkpoints_stop_before_processing_order_hole(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'prefix-hole.db'}")
    await database.create_schema_for_tests()
    events = [
        _event("unresolved-first", 0, slot=500),
        _event("processed-second", 0, slot=500),
    ]
    assert await database.record_events(events) == [True, True]
    async with database.event_state_transaction():
        await database.mark_event_processed(
            events[1].event_id, processed_at=events[1].block_time
        )
    database.release_event_claim(events[1].event_id)

    since = datetime(2026, 8, 24, tzinfo=timezone.utc)
    assert await database.load_processed_events_since(since) == []
    assert await database.load_stream_checkpoint() == (0, None, None)
    assert await database.load_protocol_checkpoints() == {}
    assert await database.load_quarantined_event_protocols() == {
        Protocol.PUMPSWAP.value
    }

    async with database.event_state_transaction():
        await database.mark_event_processed(
            events[0].event_id, processed_at=events[0].block_time
        )
    database.release_event_claim(events[0].event_id)

    assert [
        event.signature for event in await database.load_processed_events_since(since)
    ] == ["unresolved-first", "processed-second"]
    assert await database.load_stream_checkpoint() == (
        500,
        "processed-second",
        events[1].block_time,
    )
    assert await database.load_protocol_checkpoints() == {
        Protocol.PUMPSWAP.value: "processed-second"
    }
    assert await database.load_quarantined_event_protocols() == set()
    await database.close()

@pytest.mark.asyncio
async def test_record_events_uses_bounded_multirow_sql_and_preserves_order(
    tmp_path,
) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'bulk-shape.db'}")
    await database.create_schema_for_tests()
    statements: list[tuple[str, bool]] = []

    def capture_sql(
        _connection: Any,
        _cursor: Any,
        statement: str,
        _parameters: Any,
        _context: Any,
        executemany: bool,
    ) -> None:
        statements.append((" ".join(statement.lower().split()), executemany))

    sqlalchemy_event.listen(
        database.engine.sync_engine, "before_cursor_execute", capture_sql
    )
    events = [
        _event(f"bulk-{index:02d}", 0, slot=1_000 + index)
        for index in range(64)
    ]
    try:
        assert await database.record_events(events) == [True] * len(events)
    finally:
        sqlalchemy_event.remove(
            database.engine.sync_engine, "before_cursor_execute", capture_sql
        )

    dedup_inserts = [
        item for item in statements if item[0].startswith("insert into event_dedup")
    ]
    raw_inserts = [
        item
        for item in statements
        if item[0].startswith("insert into raw_chain_events")
    ]
    sequence_reads = [
        item
        for item in statements
        if "max(raw_chain_events.ingest_sequence)" in item[0]
    ]
    assert len(dedup_inserts) == 1
    assert " on conflict " in dedup_inserts[0][0]
    assert " returning event_id" in dedup_inserts[0][0]
    assert dedup_inserts[0][1] is False
    assert 1 <= len(raw_inserts) <= 2
    assert all(executemany is False for _, executemany in raw_inserts)
    assert len(sequence_reads) == 1
    assert len(statements) == (
        len(dedup_inserts) + len(raw_inserts) + len(sequence_reads)
    )

    async with database.sessions() as session:
        rows = list(
            (
                await session.scalars(
                    select(RawChainEventRow).order_by(
                        RawChainEventRow.ingest_sequence
                    )
                )
            ).all()
        )
    assert [row.ingest_sequence for row in rows] == list(range(1, 65))
    assert [row.signature for row in rows] == [
        event.signature for event in events
    ]
    await database.close()


@pytest.mark.asyncio
async def test_record_events_recovers_from_real_post_commit_fault(tmp_path) -> None:
    database = Database(
        f"sqlite+aiosqlite:///{tmp_path / 'post-commit-fault.db'}"
    )
    await database.create_schema_for_tests()
    event = _event("post-commit-fault", 0)
    injected = False

    def fail_after_commit(_session: Any) -> None:
        nonlocal injected
        if not injected:
            injected = True
            raise RuntimeError("injected post-commit acknowledgement loss")

    sync_session_class = database.sessions.class_.sync_session_class
    sqlalchemy_event.listen(
        sync_session_class, "after_commit", fail_after_commit
    )
    try:
        with pytest.raises(RuntimeError, match="acknowledgement loss"):
            await database.record_events([event])
    finally:
        sqlalchemy_event.remove(
            sync_session_class, "after_commit", fail_after_commit
        )

    assert injected is True
    stable_token = database._event_claim_tokens[event.event_id]
    assert await database.record_events([event], resume_owned=True) == [True]
    assert database._event_claim_tokens[event.event_id] == stable_token
    async with database.sessions() as session:
        assert (
            await session.scalar(
                select(func.count()).select_from(RawChainEventRow)
            )
            == 1
        )
    await database.close()


@pytest.mark.asyncio
async def test_record_events_canonical_locking_handles_reversed_overlap(
    tmp_path,
) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'canonical-lock.db'}")
    await database.create_schema_for_tests()
    forward = [_event("overlap-a", 0), _event("overlap-b", 0)]
    reverse = list(reversed(forward))

    results = await asyncio.wait_for(
        asyncio.gather(
            database.record_events(forward),
            database.record_events(reverse),
        ),
        timeout=1,
    )

    assert sorted(results) == [[False, False], [True, True]]
    winner = forward if results[0] == [True, True] else reverse
    async with database.sessions() as session:
        rows = list(
            (
                await session.scalars(
                    select(RawChainEventRow).order_by(
                        RawChainEventRow.ingest_sequence
                    )
                )
            ).all()
        )
    assert [row.signature for row in rows] == [
        event.signature for event in winner
    ]
    assert [row.ingest_sequence for row in rows] == [1, 2]
    await database.close()


@pytest.mark.asyncio
async def test_record_events_bulk_conflicts_cover_mixed_claim_states(
    tmp_path,
) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'mixed-claims.db'}")
    await database.create_schema_for_tests()
    processed = _event("processed", 0)
    active = _event("active", 0)
    stale = _event("stale", 0)
    assert await database.record_events([processed, active, stale]) == [
        True,
        True,
        True,
    ]
    async with database.event_state_transaction():
        await database.mark_event_processed(
            processed.event_id, processed_at=processed.block_time
        )
    database.release_event_claim(processed.event_id)
    async with database.sessions.begin() as session:
        await session.execute(
            update(EventDedupRow)
            .where(EventDedupRow.event_id == stale.event_id)
            .values(
                last_attempt_at=(
                    datetime.now(tz=timezone.utc) - timedelta(minutes=3)
                )
            )
        )

    new = _event("new", 0)
    assert await database.record_events(
        [new, stale, active, processed]
    ) == [True, True, False, False]
    async with database.sessions() as session:
        rows = {
            row.event_id: row
            for row in (
                await session.scalars(
                    select(EventDedupRow).where(
                        EventDedupRow.event_id.in_(
                            [
                                processed.event_id,
                                active.event_id,
                                stale.event_id,
                                new.event_id,
                            ]
                        )
                    )
                )
            ).all()
        }
        assert (
            await session.scalar(
                select(func.count()).select_from(RawChainEventRow)
            )
            == 4
        )
    assert rows[processed.event_id].processing_status == "PROCESSED"
    assert rows[active.event_id].processing_attempts == 1
    assert rows[stale.event_id].processing_attempts == 2
    assert rows[new.event_id].processing_attempts == 1
    await database.close()

@pytest.mark.asyncio
async def test_record_events_cleans_token_after_precommit_failure(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'rollback-token.db'}")
    await database.create_schema_for_tests()
    event = _event("precommit-fault", 0)
    injected = False

    def fail_before_insert(
        _connection: Any,
        _cursor: Any,
        statement: str,
        _parameters: Any,
        _context: Any,
        _executemany: bool,
    ) -> None:
        nonlocal injected
        if not injected and statement.lower().startswith("insert into event_dedup"):
            injected = True
            raise RuntimeError("injected pre-commit failure")

    sqlalchemy_event.listen(
        database.engine.sync_engine,
        "before_cursor_execute",
        fail_before_insert,
    )
    try:
        with pytest.raises(RuntimeError, match="pre-commit failure"):
            await database.record_events([event])
    finally:
        sqlalchemy_event.remove(
            database.engine.sync_engine,
            "before_cursor_execute",
            fail_before_insert,
        )

    assert injected is True
    assert event.event_id not in database._event_claim_tokens
    assert await database.record_events([event]) == [True]
    await database.close()


@pytest.mark.asyncio
async def test_mark_event_failed_serializes_with_batch_claims(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'failed-race.db'}")
    await database.create_schema_for_tests()
    event = _event("failed-race", 0)
    assert await database.record_event(event) is True

    await database._event_batch_lock.acquire()
    task = asyncio.create_task(
        database.mark_event_failed(event.event_id, RuntimeError("processing failed"))
    )
    try:
        await asyncio.sleep(0)
        assert task.done() is False
    finally:
        database._event_batch_lock.release()
    await asyncio.wait_for(task, timeout=1)

    assert event.event_id not in database._event_claim_tokens
    assert await database.record_events([event]) == [True]
    await database.close()


@pytest.mark.asyncio
async def test_sqlite_conflict_locking_chunks_large_batches(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'conflict-chunks.db'}")
    await database.create_schema_for_tests()
    events = [
        _event(f"conflict-{index:04d}", 0, slot=20_000 + index)
        for index in range(901)
    ]
    assert await database.record_events(events) == [True] * len(events)
    conflict_select_parameter_counts: list[int] = []

    def capture_conflict_select(
        _connection: Any,
        _cursor: Any,
        statement: str,
        parameters: Any,
        _context: Any,
        _executemany: bool,
    ) -> None:
        normalized = " ".join(statement.lower().split())
        if (
            normalized.startswith("select event_dedup.")
            and " from event_dedup " in normalized
        ):
            conflict_select_parameter_counts.append(len(parameters))

    sqlalchemy_event.listen(
        database.engine.sync_engine,
        "before_cursor_execute",
        capture_conflict_select,
    )
    try:
        assert await database.record_events(events) == [False] * len(events)
    finally:
        sqlalchemy_event.remove(
            database.engine.sync_engine,
            "before_cursor_execute",
            capture_conflict_select,
        )

    assert conflict_select_parameter_counts == [900, 1]
    assert all(
        event.event_id in database._event_claim_tokens
        for event in events
    )
    await database.close()


@pytest.mark.asyncio
async def test_record_events_rejects_oversized_atomic_batch(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'oversized.db'}")
    await database.create_schema_for_tests()
    event = _event("oversized", 0)

    with pytest.raises(ValueError, match="at most"):
        await database.record_events([event] * (MAX_EVENT_BATCH_SIZE + 1))

    assert database._event_claim_tokens == {}
    await database.close()

@pytest.mark.asyncio
async def test_record_events_preserves_token_for_unknown_commit_phase(
    tmp_path,
) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'unknown-commit.db'}")
    await database.create_schema_for_tests()
    event = _event("unknown-commit", 0)
    injected = False

    def fail_before_commit(_session: Any) -> None:
        nonlocal injected
        if not injected:
            injected = True
            raise RuntimeError("injected commit-phase acknowledgement loss")

    sync_session_class = database.sessions.class_.sync_session_class
    sqlalchemy_event.listen(
        sync_session_class,
        "before_commit",
        fail_before_commit,
    )
    try:
        with pytest.raises(RuntimeError, match="acknowledgement loss"):
            await database.record_events([event])
    finally:
        sqlalchemy_event.remove(
            sync_session_class,
            "before_commit",
            fail_before_commit,
        )

    assert injected is True
    stable_token = database._event_claim_tokens[event.event_id]
    assert await database.record_events([event]) == [True]
    assert database._event_claim_tokens[event.event_id] == stable_token
    async with database.sessions() as session:
        assert (
            await session.scalar(
                select(func.count()).select_from(RawChainEventRow)
            )
            == 1
        )
    await database.close()

@pytest.mark.asyncio
async def test_event_state_batch_coalesces_upserts_and_marks_processed(
    tmp_path,
) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'state-batch.db'}")
    await database.create_schema_for_tests()
    events = [
        _event(f"state-batch-{index:03d}", 0, slot=20_000 + index)
        for index in range(64)
    ]
    statements: list[tuple[str, bool]] = []

    def capture_sql(
        _connection: Any,
        _cursor: Any,
        statement: str,
        _parameters: Any,
        _context: Any,
        executemany: bool,
    ) -> None:
        statements.append((" ".join(statement.lower().split()), executemany))

    try:
        assert await database.record_events(events) == [True] * len(events)
        sqlalchemy_event.listen(
            database.engine.sync_engine,
            "before_cursor_execute",
            capture_sql,
        )
        try:
            async with database.event_state_batch_transaction():
                for index, event in enumerate(events):
                    await database.upsert_token(
                        TokenRecord(
                            mint="STATE-BATCH-TOKEN",
                            creation_time=events[0].block_time,
                            updated_at=event.observed_at + timedelta(milliseconds=index),
                        )
                    )
                    await database.mark_event_processed(
                        event.event_id,
                        processed_at=event.observed_at,
                    )
        finally:
            sqlalchemy_event.remove(
                database.engine.sync_engine,
                "before_cursor_execute",
                capture_sql,
            )

        token_inserts = [
            statement
            for statement, _ in statements
            if statement.startswith("insert into tokens")
        ]
        claim_locks = [
            statement
            for statement, _ in statements
            if statement.startswith("select event_dedup.event_id")
            and "order by event_dedup.event_id" in statement
        ]
        claim_updates = [
            statement
            for statement, _ in statements
            if statement.startswith("update event_dedup set")
        ]
        assert len(statements) == 3
        assert len(token_inserts) == 1
        assert len(claim_locks) == 1
        assert len(claim_updates) == 1
        assert all(executemany is False for _, executemany in statements)

        async with database.sessions() as session:
            token = await session.get(TokenRow, "STATE-BATCH-TOKEN")
            claims = list(
                (
                    await session.scalars(
                        select(EventDedupRow).where(
                            EventDedupRow.event_id.in_(
                                [event.event_id for event in events]
                            )
                        )
                    )
                ).all()
            )
        assert token is not None
        assert len(claims) == len(events)
        assert all(claim.processing_status == "PROCESSED" for claim in claims)
        assert all(claim.processing_token is None for claim in claims)
    finally:
        for event in events:
            database.release_event_claim(event.event_id)
        await database.close()


@pytest.mark.asyncio
async def test_event_state_batch_chunks_processed_claims_under_sqlite_budget(
    tmp_path,
) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'state-chunks.db'}")
    await database.create_schema_for_tests()
    events = [
        _event(f"state-chunks-{index:04d}", 0, slot=30_000 + index)
        for index in range(600)
    ]
    lock_parameter_counts: list[int] = []
    update_parameter_counts: list[int] = []

    def capture_sql(
        _connection: Any,
        _cursor: Any,
        statement: str,
        parameters: Any,
        _context: Any,
        _executemany: bool,
    ) -> None:
        normalized = " ".join(statement.lower().split())
        if (
            normalized.startswith("select event_dedup.event_id")
            and "order by event_dedup.event_id" in normalized
        ):
            lock_parameter_counts.append(len(parameters))
        elif normalized.startswith("update event_dedup set"):
            update_parameter_counts.append(len(parameters))

    try:
        assert await database.record_events(events) == [True] * len(events)
        sqlalchemy_event.listen(
            database.engine.sync_engine,
            "before_cursor_execute",
            capture_sql,
        )
        try:
            async with database.event_state_batch_transaction():
                for event in events:
                    await database.mark_event_processed(
                        event.event_id,
                        processed_at=event.observed_at,
                    )
        finally:
            sqlalchemy_event.remove(
                database.engine.sync_engine,
                "before_cursor_execute",
                capture_sql,
            )

        assert len(lock_parameter_counts) > 1
        assert len(update_parameter_counts) > 1
        assert max(lock_parameter_counts) <= SQLITE_SAFE_BOUND_PARAMETER_BUDGET
        assert max(update_parameter_counts) <= SQLITE_SAFE_BOUND_PARAMETER_BUDGET

        async with database.sessions() as session:
            processed_count = int(
                (
                    await session.scalar(
                        select(func.count())
                        .select_from(EventDedupRow)
                        .where(EventDedupRow.processing_status == "PROCESSED")
                    )
                )
                or 0
            )
        assert processed_count == len(events)
    finally:
        for event in events:
            database.release_event_claim(event.event_id)
        await database.close()


@pytest.mark.asyncio
async def test_event_state_batch_superseded_claim_is_atomic(tmp_path) -> None:
    url = f"sqlite+aiosqlite:///{tmp_path / 'state-superseded.db'}"
    owner = Database(url)
    competitor = Database(url)
    await owner.create_schema_for_tests()
    active = _event("state-active", 0, slot=40_000)
    stale = _event("state-stale", 0, slot=40_001)
    events = [active, stale]

    try:
        assert await owner.record_events(events) == [True, True]
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
            async with owner.event_state_batch_transaction():
                for event in events:
                    await owner.mark_event_processed(
                        event.event_id,
                        processed_at=event.observed_at,
                    )

        assert owner._event_claim_tokens == original_tokens
        async with owner.sessions() as session:
            rows = {
                row.event_id: row
                for row in (
                    await session.scalars(
                        select(EventDedupRow).where(
                            EventDedupRow.event_id.in_(
                                [event.event_id for event in events]
                            )
                        )
                    )
                ).all()
            }
        assert rows[active.event_id].processing_status == "PROCESSING"
        assert rows[active.event_id].processing_token == original_tokens[active.event_id]
        assert rows[stale.event_id].processing_status == "PROCESSING"
        assert rows[stale.event_id].processing_token == competitor_token
    finally:
        for event in events:
            owner.release_event_claim(event.event_id)
            competitor.release_event_claim(event.event_id)
        await competitor.close()
        await owner.close()
