from __future__ import annotations

import base64
import copy
import struct
from pathlib import Path
from typing import Any

import pytest

from scripts.benchmark_postgres_capacity import BorshEventEncoder
from sniper_bot.events import EVENT_ID_V2_CUTOVER_SLOT, EVENT_IDENTITY_KEY
from sniper_bot.protocols import AnchorDecodeError
from sniper_bot.protocols.anchor import _base58_decode, _base58_encode
from sniper_bot.protocols.pump import PUMP_PROGRAM_ID, PumpDecoder
from sniper_bot.protocols.pump.decoder import PUMP_EVENT_NAMES
from sniper_bot.protocols.pumpswap import PUMPSWAP_PROGRAM_ID, PumpSwapDecoder
from sniper_bot.protocols.pumpswap.decoder import PUMPSWAP_EVENT_NAMES
from sniper_bot.registry import WSOL_MINT

OTHER = "11111111111111111111111111111111"
TAG = bytes.fromhex("e445a52e51cb9a1d")
TIMESTAMP = 1_776_700_123
ROOT = Path(__file__).parents[1] / "src/sniper_bot/protocols"
# protocol: decoder, program, consumed creation (operation, event, payload fields, argument bytes), consumed set
PROTOCOLS: dict[str, tuple[Any, str, str, str, dict[str, Any], int, frozenset[str]]] = {
    "pump": (PumpDecoder, PUMP_PROGRAM_ID, "create_v2", "CreateEvent", {}, 0, PUMP_EVENT_NAMES),
    "pumpswap": (PumpSwapDecoder, PUMPSWAP_PROGRAM_ID, "create_pool", "CreatePoolEvent",
                 {"quote_mint": WSOL_MINT}, 62, PUMPSWAP_EVENT_NAMES),
}
OPERATIONS = {
    "init_user_volume_accumulator": ("InitUserVolumeAccumulatorEvent", 72),
    "sync_user_volume_accumulator": ("SyncUserVolumeAccumulatorEvent", 56),
}
CASES = [(protocol, operation) for protocol in PROTOCOLS for operation in OPERATIONS]


def _decoder(protocol: str, selection: frozenset[str] | None = None) -> Any:
    cls = PROTOCOLS[protocol][0]
    return cls() if selection is None else cls(event_names=selection)


def _fixture(protocol: str, operation: str) -> dict[str, Any]:
    """A consumed creation and the ignored accumulator control in one nested group."""
    decoder_cls, program, create_op, create_event, create_fields, create_args, _ = PROTOCOLS[protocol]
    encoder = BorshEventEncoder(ROOT / protocol / "idl.json")
    event_name, body_bytes = OPERATIONS[operation]
    create = base64.b64decode(encoder.encode(create_event, {"timestamp": TIMESTAMP, **create_fields}))
    control = base64.b64decode(encoder.encode(event_name, {"timestamp": TIMESTAMP}))
    assert len(control) == 8 + body_bytes
    instructions = {item["name"]: bytes(item["discriminator"]) for item in decoder_cls()._anchor.idl["instructions"]}
    return {
        "slot": EVENT_ID_V2_CUTOVER_SLOT + 1, "blockTime": TIMESTAMP,
        "transaction": {"signatures": [_base58_encode(bytes([9]) * 64)], "message": {
            "instructions": [{"programId": OTHER} for _ in range(3)],
        }},
        "meta": {"err": None, "logMessages": [
            f"Program {OTHER} invoke [1]", f"Program {program} invoke [2]",
            "Program data: " + base64.b64encode(create).decode(), f"Program {program} success",
            f"Program {program} invoke [2]", "Program data: " + base64.b64encode(control).decode(),
            f"Program {program} success", f"Program {OTHER} success", "Log truncated",
        ], "innerInstructions": [{"index": 2, "instructions": [
            {"programId": OTHER, "stackHeight": 2},
            {"programId": program, "stackHeight": 2,
             "data": _base58_encode(instructions[create_op] + bytes(create_args))},
            {"programId": OTHER, "stackHeight": 3},
            {"programId": program, "stackHeight": 3, "data": _base58_encode(TAG + create)},
            {"programId": program, "stackHeight": 2, "data": _base58_encode(instructions[operation])},
            {"programId": OTHER, "stackHeight": 3},
            {"programId": program, "stackHeight": 3, "data": _base58_encode(TAG + control)},
        ]}]},
    }


def _complete(tx: dict[str, Any]) -> dict[str, Any]:
    tx = copy.deepcopy(tx)
    tx["meta"]["logMessages"] = tx["meta"]["logMessages"][:-1]
    return tx


def _fields(event: Any) -> dict[str, Any]:
    return {key: value for key, value in event.payload.items() if key != EVENT_IDENTITY_KEY}


@pytest.mark.parametrize(("protocol", "operation"), CASES)
@pytest.mark.parametrize("prefix", [False, True])
def test_creation_with_ignored_control_keeps_identity_clock_and_selection(
    protocol: str, operation: str, prefix: bool,
) -> None:
    tx = _fixture(protocol, operation)
    expected = _decoder(protocol).decode(_complete(tx)).events[0]
    tx["meta"]["logMessages"] = tx["meta"]["logMessages"][:4] + ["Log truncated"] if prefix else ["Log truncated"]
    unchanged = copy.deepcopy(tx)
    decoded = _decoder(protocol).decode(tx)
    assert len(decoded.events) == 1
    actual = decoded.events[0]
    assert actual.event_id == expected.event_id and actual.block_time == expected.block_time
    assert _fields(actual) == _fields(expected)
    assert actual.payload[EVENT_IDENTITY_KEY] == {"version": 2, "ordinal": 0, "origin": "log" if prefix else "cpi"}
    assert actual.instruction_index == (expected.instruction_index if prefix else 2)
    assert actual.inner_instruction_index == (-1 if prefix else 3)
    assert decoded.unknown_discriminators == ()
    ignored = _decoder(protocol, frozenset()).decode(tx)
    assert ignored.events == [] and ignored.block_time == decoded.block_time
    assert tx == unchanged


@pytest.mark.parametrize(("protocol", "operation"), CASES)
def test_creation_identity_is_stable_at_every_log_prefix(protocol: str, operation: str) -> None:
    tx = _fixture(protocol, operation)
    logs = tx["meta"]["logMessages"][:-1]
    expected = _decoder(protocol).decode(_complete(tx)).events[0]
    for length in range(len(logs) + 1):
        partial = copy.deepcopy(tx)
        partial["meta"]["logMessages"] = logs[:length] + ["Log truncated"]
        events = _decoder(protocol).decode(partial).events
        assert [event.event_id for event in events] == [expected.event_id]
        assert _fields(events[0]) == _fields(expected)
        assert _decoder(protocol, frozenset()).decode(partial).events == []


@pytest.mark.parametrize(("protocol", "operation"), CASES)
@pytest.mark.parametrize("block_time_present", [False, True])
def test_standalone_control_dates_from_its_cpi_without_state_or_identity(
    protocol: str, operation: str, block_time_present: bool,
) -> None:
    tx = _fixture(protocol, operation)
    expected = _decoder(protocol).decode(_complete(tx)).block_time
    inner = tx["meta"]["innerInstructions"][0]["instructions"]
    tx["meta"]["innerInstructions"][0]["instructions"] = [inner[0], *inner[4:]]
    tx["meta"]["logMessages"] = ["Log truncated"]
    if not block_time_present:
        del tx["blockTime"]
    for selection in (PROTOCOLS[protocol][6], frozenset()):
        decoded = _decoder(protocol, selection).decode(tx)
        assert decoded.events == [] and decoded.block_time == expected
    assert OPERATIONS[operation][0] not in PROTOCOLS[protocol][6]


@pytest.mark.parametrize(("protocol", "operation"), CASES)
@pytest.mark.parametrize("failure", [
    "extra", "short", "trailing", "clock", "wrong_parent", "wrong_event",
    "unknown_late_operation", "clock_without_block_time", "failed_metadata",
])
def test_ignored_control_requires_complete_body_clock_parent_and_exactly_one_cpi(
    protocol: str, operation: str, failure: str,
) -> None:
    program = PROTOCOLS[protocol][1]
    tx = _fixture(protocol, operation)
    inner = tx["meta"]["innerInstructions"][0]["instructions"]
    cpi = inner[6]
    if failure == "extra":
        inner.append(copy.deepcopy(cpi))
    elif failure in {"short", "trailing", "clock", "clock_without_block_time"}:
        raw = bytearray(_base58_decode(cpi["data"]))
        if failure == "short":
            raw.pop()
        elif failure == "trailing":
            raw.append(0)
        else:
            struct.pack_into("<q", raw, len(raw) - 8, TIMESTAMP + 120)
        cpi["data"] = _base58_encode(bytes(raw))
        if failure == "clock_without_block_time":
            del tx["blockTime"]
            tx["meta"]["logMessages"] = ["Log truncated"]
    elif failure == "wrong_parent":
        cpi["stackHeight"] = 2
    elif failure == "wrong_event":
        cpi["data"] = inner[3]["data"]
    elif failure == "unknown_late_operation":
        inner.append({"programId": program, "stackHeight": 2, "data": _base58_encode(bytes([255]) * 8)})
    elif failure == "failed_metadata":
        tx["meta"]["err"] = {"InstructionError": [2, {"Custom": 1}]}
    unchanged = copy.deepcopy(tx)
    for selection in (PROTOCOLS[protocol][6], frozenset()):
        with pytest.raises(AnchorDecodeError):
            _decoder(protocol, selection).decode(tx)
    assert tx == unchanged


@pytest.mark.parametrize(("protocol", "operation"), CASES)
@pytest.mark.parametrize("change", ["missing", "foreign_cpi"])
def test_control_without_its_event_keeps_the_creation(protocol: str, operation: str, change: str) -> None:
    # Observed natively: sync_user_volume_accumulator often moves nothing and emits no event.
    tx = _fixture(protocol, operation)
    expected = _decoder(protocol).decode(_complete(tx)).events
    tx["meta"]["logMessages"] = ["Log truncated"]
    inner = tx["meta"]["innerInstructions"][0]["instructions"]
    if change == "missing":
        inner.pop()
    else:
        inner[6]["programId"] = OTHER
    actual = _decoder(protocol).decode(tx).events
    assert [event.event_id for event in actual] == [event.event_id for event in expected]
