from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import pytest

from sniper_bot.database import Database
from sniper_bot.events import ChainEventType, EventEnvelope, EventSource, Protocol
from sniper_bot.metrics import BotMetrics
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