from __future__ import annotations

import asyncio
import base64
import json
import struct
import threading
from datetime import datetime, timezone
from pathlib import Path

import pytest

import sniper_bot.events as events_module
from sniper_bot.events import (
    ChainEventType,
    EventDeduplicator,
    EventEnvelope,
    EventSource,
    Protocol,
    RawEventReader,
    RawEventRecorder,
)
from sniper_bot.protocols import UnknownDiscriminatorError
from sniper_bot.protocols.anchor import _base58_encode
from sniper_bot.protocols.pump import PUMP_PROGRAM_ID, PumpDecoder
from sniper_bot.protocols.pumpswap import PumpSwapDecoder


def _complete_event_transaction(discriminator: bytes | None = None) -> dict:
    user = bytes([1]) * 32
    mint = bytes([2]) * 32
    curve = bytes([3]) * 32
    quote = bytes([4]) * 32
    disc = discriminator or bytes([95, 114, 97, 156, 212, 46, 152, 8])
    payload = disc + user + mint + curve + struct.pack("<q", 1_776_700_000) + quote
    return {
        "slot": 123,
        "blockTime": 1_776_700_000,
        "transaction": {"signatures": ["signature-1"]},
        "meta": {
            "logMessages": [
                f"Program {PUMP_PROGRAM_ID} invoke [1]",
                f"Program data: {base64.b64encode(payload).decode()}",
                f"Program {PUMP_PROGRAM_ID} success",
            ]
        },
    }


def _event() -> EventEnvelope:
    observed = datetime(2026, 8, 24, 12, 0, 1, tzinfo=timezone.utc)
    return EventEnvelope(
        source=EventSource.REPLAY,
        protocol=Protocol.PUMP,
        event_type=ChainEventType.BONDING_CURVE_COMPLETED,
        slot=123,
        signature="signature-1",
        instruction_index=1,
        block_time=observed,
        observed_at=observed,
        mint="mint",
        payload={"value": "1"},
    )


def test_pump_decoder_uses_official_event_discriminator() -> None:
    decoded = PumpDecoder().decode_transaction(_complete_event_transaction())

    assert len(decoded) == 1
    event = decoded[0]
    assert event.event_type == ChainEventType.BONDING_CURVE_COMPLETED
    assert event.mint == _base58_encode(bytes([2]) * 32)
    assert event.payload["bonding_curve"] == _base58_encode(bytes([3]) * 32)
    assert event.payload["quote_mint"] == _base58_encode(bytes([4]) * 32)


def test_pumpswap_decoder_uses_real_reversed_create_pool_event() -> None:
    fixture_path = (
        Path(__file__).parent / "fixtures" / "pumpswap_create_pool_reversed.json"
    )
    transaction = json.loads(fixture_path.read_text(encoding="utf-8"))

    decoded = PumpSwapDecoder().decode_transaction(transaction)

    assert len(decoded) == 1
    event = decoded[0]
    assert event.event_type == ChainEventType.POOL_CREATED
    assert event.mint == "2r8hyN4p3uTtTjGkkyzhNt4Ygxe3J1JnQPqCoi3pyyvu"
    assert event.payload["base_mint"] == "So11111111111111111111111111111111111111112"
    assert event.pool_address == "3NPBqdz22Xz4xhomduRcC8QTCcHnqBGVgLXB8sWWPRpp"


def test_unknown_anchor_discriminator_fails_closed() -> None:
    with pytest.raises(UnknownDiscriminatorError):
        PumpDecoder().decode_transaction(_complete_event_transaction(bytes([255]) * 8))


@pytest.mark.asyncio
async def test_event_deduplication_and_raw_zstd_round_trip(tmp_path) -> None:
    event = _event()
    dedupe = EventDeduplicator(capacity=2)
    recorder = RawEventRecorder(tmp_path)

    assert await dedupe.accept(event.event_id) is True
    assert await dedupe.accept(event.event_id) is False
    assert dedupe.duplicates_total == 1
    path = await recorder.record(event)
    assert path.name == "pump-events-12.ndjson.zst"

    restored = list(RawEventReader(tmp_path).iter_events())
    assert restored == [event]


def test_event_id_rejects_noncanonical_value() -> None:
    payload = _event().model_dump()
    payload["event_id"] = "wrong"
    with pytest.raises(ValueError, match="canonical deduplication key"):
        EventEnvelope.model_validate(payload)


@pytest.mark.asyncio
async def test_raw_event_batch_writes_once_per_partition_and_round_trips(
    tmp_path,
    monkeypatch,
) -> None:
    first = _event()
    second_payload = first.model_dump(exclude={"event_id"})
    second_payload.update(
        {
            "signature": "signature-2",
            "instruction_index": 2,
            "block_time": first.block_time.replace(minute=1),
            "observed_at": first.observed_at.replace(minute=1),
        }
    )
    third_payload = first.model_dump(exclude={"event_id"})
    third_payload.update(
        {
            "signature": "signature-3",
            "instruction_index": 3,
            "block_time": first.block_time.replace(hour=13),
            "observed_at": first.observed_at.replace(hour=13),
        }
    )
    events = [
        first,
        EventEnvelope.model_validate(second_payload),
        EventEnvelope.model_validate(third_payload),
    ]
    append_calls: list[Path] = []
    thread_calls = 0
    original_append = events_module._append_bytes
    original_to_thread = events_module.asyncio.to_thread

    def counted_append(path: Path, payload: bytes) -> None:
        append_calls.append(path)
        original_append(path, payload)

    async def counted_to_thread(function, *args):
        nonlocal thread_calls
        thread_calls += 1
        return await original_to_thread(function, *args)

    monkeypatch.setattr(events_module, "_append_bytes", counted_append)
    monkeypatch.setattr(
        events_module.asyncio,
        "to_thread",
        counted_to_thread,
    )

    paths = await RawEventRecorder(tmp_path).record_many(events)

    assert thread_calls == 1
    assert len(append_calls) == 2
    assert paths == [
        tmp_path / "2026-08-24" / "pump-events-12.ndjson.zst",
        tmp_path / "2026-08-24" / "pump-events-12.ndjson.zst",
        tmp_path / "2026-08-24" / "pump-events-13.ndjson.zst",
    ]
    assert list(RawEventReader(tmp_path).iter_events()) == events

@pytest.mark.asyncio
async def test_raw_event_recorder_holds_lock_until_cancelled_write_finishes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 24, 12, tzinfo=timezone.utc)

    def raw_event(signature: str) -> EventEnvelope:
        return EventEnvelope(
            source=EventSource.REPLAY,
            protocol=Protocol.PUMP,
            event_type=ChainEventType.SWAP_BUY,
            slot=123,
            signature=signature,
            instruction_index=0,
            block_time=now,
            observed_at=now,
            mint="TOKEN",
            pool_address="POOL",
            payload={"quote_amount_in": "1"},
        )

    entered = threading.Event()
    release = threading.Event()
    state_lock = threading.Lock()
    calls: list[Path] = []
    active_writers = 0
    max_active_writers = 0

    def blocked_append(path: Path, _payload: bytes) -> None:
        nonlocal active_writers, max_active_writers
        with state_lock:
            calls.append(path)
            active_writers += 1
            max_active_writers = max(max_active_writers, active_writers)
            call_number = len(calls)
        try:
            if call_number == 1:
                entered.set()
                if not release.wait(timeout=2):
                    raise TimeoutError("raw archive test release timed out")
        finally:
            with state_lock:
                active_writers -= 1

    monkeypatch.setattr(events_module, "_append_bytes", blocked_append)
    recorder = RawEventRecorder(tmp_path)
    first_task = asyncio.create_task(recorder.record_many([raw_event("first")]))
    second_task: asyncio.Task[list[Path]] | None = None
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        first_task.cancel()
        await asyncio.sleep(0)
        second_task = asyncio.create_task(
            recorder.record_many([raw_event("second")])
        )
        await asyncio.sleep(0.05)

        assert first_task.done() is False
        assert second_task.done() is False
        assert len(calls) == 1

        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(first_task, timeout=1)
        await asyncio.wait_for(second_task, timeout=1)

        assert len(calls) == 2
        assert max_active_writers == 1
    finally:
        release.set()
        for task in (first_task, second_task):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(
                task
                for task in (first_task, second_task)
                if task is not None
            ),
            return_exceptions=True,
        )
