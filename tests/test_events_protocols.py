from __future__ import annotations

import base64
import json
import struct
from datetime import datetime, timezone
from pathlib import Path

import pytest

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
