from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts.preflight_db import inspect
from sniper_bot.database import Database
from sniper_bot.events import (
    EVENT_ID_V2_CUTOVER_SLOT,
    EVENT_IDENTITY_KEY,
    ChainEventType,
    EventEnvelope,
    EventSource,
    Protocol,
    RawEventReader,
    RawEventRecorder,
    has_chain_signature_format,
    make_event_id,
    make_event_id_v2,
    uses_event_id_v2,
)
from sniper_bot.protocols.anchor import _base58_encode
from sniper_bot.protocols.pump import PUMP_STATE_EVENT_NAMES, PumpDecoder
from sniper_bot.protocols.pumpswap import PumpSwapDecoder

NOW = datetime(2026, 10, 3, 22, tzinfo=timezone.utc)
SIGNATURE = _base58_encode(bytes([3]) * 64)


def _envelope(*, descriptor: object = None, v2: bool = True) -> EventEnvelope:
    payload = {"quote_amount_in": "1"}
    if v2:
        payload[EVENT_IDENTITY_KEY] = descriptor if descriptor is not None else {"version": 2, "ordinal": 0, "origin": "log"}
    return EventEnvelope(
        source=EventSource.REPLAY, protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.SWAP_BUY, slot=EVENT_ID_V2_CUTOVER_SLOT + 1,
        signature=SIGNATURE, instruction_index=2, block_time=NOW, observed_at=NOW,
        payload=payload,
    )


@pytest.mark.parametrize("raw,expected", [
    (bytes([1]) * 63, False), (bytes([1]) * 64, True), (bytes([1]) * 65, False),
    (bytes([255]) * 63, False), (bytes([255]) * 64, True), (bytes([255]) * 65, False),
    (bytes(64), True), (bytes(63) + b"x", True),
])
def test_signature_format_uses_decoded_byte_count(raw: bytes, expected: bool) -> None:
    assert has_chain_signature_format(_base58_encode(raw)) is expected


@pytest.mark.parametrize("signature", ["capacity-000000000001", "0" * 88, "I" * 88, "O" * 88, "l" * 88, "1" * 89, ""])
def test_nonchain_signatures_keep_v1_even_at_synthetic_future_slot(signature: str) -> None:
    assert has_chain_signature_format(signature) is False
    assert uses_event_id_v2(950_001_023, signature) is False


@pytest.mark.parametrize("offset,expected", [(-1, False), (0, False), (1, True)])
def test_cutover_boundary_is_fixed_and_inclusive_for_v1(offset: int, expected: bool) -> None:
    assert EVENT_ID_V2_CUTOVER_SLOT == 453_053_813
    assert uses_event_id_v2(EVENT_ID_V2_CUTOVER_SLOT + offset, SIGNATURE) is expected


def test_v2_hash_domain_contains_version_protocol_type_and_ordinal() -> None:
    event = _envelope()
    assert event.event_id == hashlib.sha256(f"v2:pumpswap:{SIGNATURE}:swap_buy:0".encode()).hexdigest()
    assert len(event.event_id) == 64
    assert event.event_id != make_event_id(SIGNATURE, 2, -1, ChainEventType.SWAP_BUY)
    assert event.event_id != make_event_id_v2(Protocol.PUMP, SIGNATURE, ChainEventType.SWAP_BUY, 0)
    assert event.event_id != make_event_id_v2(Protocol.PUMPSWAP, SIGNATURE, ChainEventType.SWAP_SELL, 0)
    assert event.event_id != make_event_id_v2(Protocol.PUMPSWAP, SIGNATURE, ChainEventType.SWAP_BUY, 1)
    changed = event.model_dump(exclude={"event_id"})
    changed.update(source=EventSource.RPC_RECOVERY, instruction_index=8)
    assert EventEnvelope.model_validate(changed).event_id == event.event_id


@pytest.mark.parametrize("descriptor", [
    {}, None, [], "2", {"version": 1, "ordinal": 0, "origin": "log"},
    {"version": 2.0, "ordinal": 0, "origin": "log"},
    {"version": 2, "ordinal": True, "origin": "log"},
    {"version": 2, "ordinal": -1, "origin": "log"},
    {"version": 2, "ordinal": "0", "origin": "log"},
    {"version": 2, "ordinal": 0.0, "origin": "log"},
    {"version": 2, "ordinal": 0, "origin": "invented"},
    {"version": 2, "ordinal": 0, "origin": "log", "extra": 1},
])
def test_identity_descriptor_is_strict(descriptor: object) -> None:
    value = _envelope().model_dump()
    value["payload"][EVENT_IDENTITY_KEY] = descriptor
    with pytest.raises(ValueError):
        EventEnvelope.model_validate(value)


@pytest.mark.parametrize("mutation", ["old_slot", "nonchain", "wrong_hash", "cpi_without_inner", "log_with_inner"])
def test_v2_envelope_rejects_inconsistent_cutover_hash_and_provenance(mutation: str) -> None:
    value = _envelope().model_dump()
    if mutation == "old_slot":
        value["slot"] = EVENT_ID_V2_CUTOVER_SLOT
    elif mutation == "nonchain":
        value["signature"] = "synthetic"
    elif mutation == "wrong_hash":
        value["event_id"] = "f" * 64
    elif mutation == "cpi_without_inner":
        value["payload"][EVENT_IDENTITY_KEY]["origin"] = "cpi"
    elif mutation == "log_with_inner":
        value["inner_instruction_index"] = 0
    with pytest.raises(ValueError):
        EventEnvelope.model_validate(value)


def test_descriptorless_historical_envelope_keeps_v1_and_unchanged_serialization() -> None:
    old = _envelope(v2=False)
    assert old.event_id == make_event_id(SIGNATURE, 2, -1, ChainEventType.SWAP_BUY)
    serialized = old.model_dump_json(exclude_none=True)
    assert EVENT_IDENTITY_KEY not in serialized
    assert '"ordinal"' not in serialized and '"version"' not in serialized
    assert EventEnvelope.model_validate_json(serialized).model_dump_json(exclude_none=True) == serialized


def _archive_bytes(root: Path) -> dict[Path, bytes]:
    return {path: path.read_bytes() for path in root.rglob("*.zst")}


@pytest.mark.asyncio
async def test_raw_v1_and_v2_envelopes_round_trip_without_rewriting_bytes(tmp_path: Path) -> None:
    events = [_envelope(v2=False), _envelope()]
    recorder = RawEventRecorder(tmp_path)
    await recorder.record_many(events)
    before = await asyncio.to_thread(_archive_bytes, tmp_path)
    assert list(RawEventReader(tmp_path).iter_events()) == events
    await recorder.record_many(events)
    assert await asyncio.to_thread(_archive_bytes, tmp_path) == before


@pytest.mark.asyncio
async def test_database_restart_preserves_v2_descriptor_and_one_log_cpi_claim(tmp_path: Path) -> None:
    dsn = f"sqlite+aiosqlite:///{tmp_path / 'identity.db'}"
    database = Database(dsn)
    await database.create_schema_for_tests()
    logged = _envelope()
    assert await database.record_event(logged)
    await database.mark_event_failed(logged.event_id, RuntimeError("retry"))
    await database.close()
    database = Database(dsn)
    recovered = await database.load_unprocessed_events()
    assert len(recovered) == 1 and recovered[0].event_id == logged.event_id
    assert recovered[0].payload[EVENT_IDENTITY_KEY] == logged.payload[EVENT_IDENTITY_KEY]
    cpi = logged.model_dump()
    cpi["payload"][EVENT_IDENTITY_KEY]["origin"] = "cpi"
    cpi.update(instruction_index=5, inner_instruction_index=9, source=EventSource.RPC_RECOVERY)
    cpi_event = EventEnvelope.model_validate(cpi)
    assert cpi_event.event_id == logged.event_id
    assert await database.record_event(cpi_event, reclaim=True)
    await database.mark_event_processed(logged.event_id, processed_at=NOW)
    assert await database.record_event(logged) is False
    assert await database.record_event(cpi_event) is False
    assert (await inspect(database))["legacy_chain_signatures_above_cutover"] == 0
    await database.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["legacy_chain", "legacy_synthetic", "legacy_before", "v2"])
async def test_db_preflight_blocks_only_genuine_chain_v1_aliases_above_cutover(tmp_path: Path, kind: str) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'preflight-id.db'}")
    await database.create_schema_for_tests()
    value = _envelope(v2=kind == "v2").model_dump(exclude={"event_id"})
    if kind == "legacy_synthetic":
        value.update(signature="capacity-000000000001", slot=950_001_023)
    elif kind == "legacy_before":
        value["slot"] = EVENT_ID_V2_CUTOVER_SLOT
    event = EventEnvelope.model_validate(value)
    assert await database.record_event(event)
    await database.mark_event_processed(event.event_id, processed_at=NOW)
    report = await inspect(database)
    assert report["legacy_chain_signatures_above_cutover"] == (1 if kind == "legacy_chain" else 0)
    assert report["stream_enabled_at_startup"] is (kind != "legacy_chain")
    await database.close()


@pytest.mark.parametrize("offset", [-1, 0, 1])
def test_decoder_chooses_same_cutover_in_live_recovery_and_replay(offset: int) -> None:
    fixture = Path(__file__).parent / "fixtures/pumpswap_create_pool_reversed.json"
    tx = json.loads(fixture.read_text(encoding="utf8"))
    tx["slot"] = EVENT_ID_V2_CUTOVER_SLOT + offset
    tx["transaction"]["signatures"] = [SIGNATURE]
    decoder = PumpSwapDecoder()
    events = [decoder.decode(copy.deepcopy(tx), source=source).events[0] for source in EventSource]
    assert len({event.event_id for event in events}) == 1
    assert all((EVENT_IDENTITY_KEY in event.payload) is (offset > 0) for event in events)


@pytest.mark.parametrize("protocol", ["pump", "pumpswap"])
def test_empty_unsigned_legacy_transaction_keeps_its_prior_behavior(protocol: str) -> None:
    decoder = PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES) if protocol == "pump" else PumpSwapDecoder()
    decoded = decoder.decode({"slot": 1, "blockTime": 1_776_700_123, "meta": {"logMessages": []}})
    assert decoded.events == []
    assert decoded.block_time is not None
