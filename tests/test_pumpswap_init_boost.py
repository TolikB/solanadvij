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
from sniper_bot.protocols.pumpswap import PUMPSWAP_PROGRAM_ID, PumpSwapDecoder
from sniper_bot.protocols.pumpswap.decoder import PUMPSWAP_EVENT_NAMES
from sniper_bot.registry import WSOL_MINT

OTHER = "11111111111111111111111111111111"
TAG = bytes.fromhex("e445a52e51cb9a1d")
BOOST_SELECTOR = bytes.fromhex("8ce9215e845ac28f")
BOOST_EVENT = bytes.fromhex("ae7c4af90451f611")
TIMESTAMP = 1_776_700_123


def _boost_payload(reserves: int = -1) -> bytes:
    return (
        BOOST_EVENT + struct.pack("<q", TIMESTAMP) + bytes([2]) * 96
        + reserves.to_bytes(16, "little", signed=True) + struct.pack("<Q", 1)
    )


def _fixture(reserves: int = -1) -> dict[str, Any]:
    encoder = BorshEventEncoder(Path(__file__).parents[1] / "src/sniper_bot/protocols/pumpswap/idl.json")
    create = base64.b64decode(encoder.encode("CreatePoolEvent", {"timestamp": TIMESTAMP, "quote_mint": WSOL_MINT}))
    definition = next(item for item in PumpSwapDecoder()._anchor.idl["instructions"] if item["name"] == "create_pool")
    boost = _boost_payload(reserves)
    assert len(boost) == 136
    return {
        "slot": EVENT_ID_V2_CUTOVER_SLOT + 1, "blockTime": TIMESTAMP,
        "transaction": {"signatures": [_base58_encode(bytes([7]) * 64)], "message": {
            "instructions": [{"programId": OTHER} for _ in range(3)],
        }},
        "meta": {"err": None, "logMessages": [
            f"Program {OTHER} invoke [1]", f"Program {PUMPSWAP_PROGRAM_ID} invoke [2]",
            "Program data: " + base64.b64encode(create).decode(),
            f"Program {PUMPSWAP_PROGRAM_ID} success", f"Program {PUMPSWAP_PROGRAM_ID} invoke [2]",
            "Program data: " + base64.b64encode(boost).decode(),
            f"Program {PUMPSWAP_PROGRAM_ID} success", f"Program {OTHER} success", "Log truncated",
        ], "innerInstructions": [{"index": 2, "instructions": [
            *({"programId": OTHER, "stackHeight": 2} for _ in range(22)),
            {"programId": PUMPSWAP_PROGRAM_ID, "stackHeight": 2, "accounts": [OTHER] * 18,
             "data": _base58_encode(bytes(definition["discriminator"]) + bytes(62))},
            *({"programId": OTHER, "stackHeight": 3} for _ in range(11)),
            {"programId": PUMPSWAP_PROGRAM_ID, "stackHeight": 3, "data": _base58_encode(TAG + create)},
            {"programId": OTHER, "stackHeight": 2},
            {"programId": PUMPSWAP_PROGRAM_ID, "stackHeight": 2, "accounts": [OTHER] * 14,
             "data": _base58_encode(BOOST_SELECTOR)},
            *({"programId": OTHER, "stackHeight": 3} for _ in range(6)),
            {"programId": PUMPSWAP_PROGRAM_ID, "stackHeight": 3, "data": _base58_encode(TAG + boost)},
        ]}]},
    }


def _complete(tx: dict[str, Any]) -> dict[str, Any]:
    tx = copy.deepcopy(tx)
    tx["meta"]["logMessages"] = tx["meta"]["logMessages"][:-1]
    return tx


def _fields(event: Any) -> dict[str, Any]:
    return {key: value for key, value in event.payload.items() if key != EVENT_IDENTITY_KEY}


@pytest.mark.parametrize("reserves", [-1, 0, -(2**127), 2**127 - 1])
@pytest.mark.parametrize("prefix", [False, True])
def test_nested_create_and_ignored_boost_preserve_identity_clock_and_selection(reserves: int, prefix: bool) -> None:
    tx = _fixture(reserves)
    expected = PumpSwapDecoder().decode(_complete(tx)).events[0]
    tx["meta"]["logMessages"] = tx["meta"]["logMessages"][:4] + ["Log truncated"] if prefix else ["Log truncated"]
    unchanged = copy.deepcopy(tx)
    decoded = PumpSwapDecoder().decode(tx)
    assert len(decoded.events) == 1
    actual = decoded.events[0]
    assert actual.event_id == expected.event_id and actual.block_time == expected.block_time
    assert _fields(actual) == _fields(expected)
    assert actual.payload[EVENT_IDENTITY_KEY] == {"version": 2, "ordinal": 0, "origin": "log" if prefix else "cpi"}
    assert actual.instruction_index == (expected.instruction_index if prefix else 2)
    assert actual.inner_instruction_index == (-1 if prefix else 34)
    assert decoded.appended_events == () and decoded.unknown_discriminators == ()
    selected = PumpSwapDecoder(event_names=frozenset({"CreatePoolEvent"})).decode(tx)
    assert selected.events[0].event_id == actual.event_id
    for selection in (frozenset({"BuyEvent"}), frozenset()):
        ignored = PumpSwapDecoder(event_names=selection).decode(tx)
        assert ignored.events == [] and ignored.block_time == decoded.block_time
    assert tx == unchanged


def test_create_identity_is_stable_at_every_compound_log_prefix() -> None:
    tx = _fixture()
    logs = tx["meta"]["logMessages"][:-1]
    expected = PumpSwapDecoder().decode(_complete(tx)).events[0]
    for length in range(len(logs) + 1):
        partial = copy.deepcopy(tx)
        partial["meta"]["logMessages"] = logs[:length] + ["Log truncated"]
        unchanged = copy.deepcopy(partial)
        decoded = PumpSwapDecoder().decode(partial)
        assert len(decoded.events) == 1
        event = decoded.events[0]
        assert event.event_id == expected.event_id and event.block_time == expected.block_time
        assert _fields(event) == _fields(expected)
        assert event.payload[EVENT_IDENTITY_KEY]["ordinal"] == 0
        assert PumpSwapDecoder(event_names=frozenset()).decode(partial).events == []
        assert partial == unchanged


def test_legacy_keeps_visible_create_but_does_not_recover_missing_create() -> None:
    tx = _fixture()
    tx["slot"] = EVENT_ID_V2_CUTOVER_SLOT
    expected = PumpSwapDecoder().decode(_complete(tx)).events[0]
    tx["meta"]["logMessages"] = tx["meta"]["logMessages"][:4] + ["Log truncated"]
    actual = PumpSwapDecoder().decode(tx).events[0]
    assert actual.model_dump(exclude={"observed_at"}) == expected.model_dump(exclude={"observed_at"})
    tx["meta"]["logMessages"] = ["Log truncated"]
    with pytest.raises(AnchorDecodeError, match="missing from the verified log prefix"):
        PumpSwapDecoder().decode(tx)
    empty = PumpSwapDecoder(event_names=frozenset()).decode(tx)
    assert empty.events == [] and empty.block_time == actual.block_time


@pytest.mark.parametrize("reserves", [-1, 0, -(2**127), 2**127 - 1])
@pytest.mark.parametrize("block_time_present", [False, True])
def test_standalone_boost_validates_signed_body_and_clock_without_selected_event(reserves: int, block_time_present: bool) -> None:
    tx = _fixture(reserves)
    expected = PumpSwapDecoder().decode(_complete(tx)).block_time
    tx["meta"]["innerInstructions"][0]["instructions"] = tx["meta"]["innerInstructions"][0]["instructions"][36:]
    tx["meta"]["logMessages"] = ["Log truncated"]
    if not block_time_present:
        del tx["blockTime"]
    unchanged = copy.deepcopy(tx)
    for selection in (PUMPSWAP_EVENT_NAMES, frozenset()):
        decoded = PumpSwapDecoder(event_names=selection).decode(tx)
        assert decoded.events == [] and decoded.block_time == expected
    fields = PumpSwapDecoder()._anchor._decode_struct("InitBoostEvent", _boost_payload(reserves)[8:])
    assert fields["virtual_quote_reserves"] == reserves
    assert "InitBoostEvent" not in PUMPSWAP_EVENT_NAMES
    assert tx == unchanged


@pytest.mark.parametrize("failure", [
    "extra", "short", "partial_i128", "trailing", "clock", "wrong_parent",
    "wrong_event", "unknown_late_operation", "altered_create_prefix",
    "clock_without_block_time", "failed_metadata",
])
def test_ignored_boost_requires_complete_body_clock_parent_and_exactly_one_cpi(failure: str) -> None:
    tx = _fixture()
    inner = tx["meta"]["innerInstructions"][0]["instructions"]
    cpi = inner[43]
    if failure == "extra":
        inner.append(copy.deepcopy(cpi))
    elif failure in {"short", "partial_i128", "trailing", "clock", "clock_without_block_time"}:
        raw = bytearray(_base58_decode(cpi["data"]))
        if failure == "short":
            raw.pop()
        elif failure == "partial_i128":
            del raw[len(TAG) + 8 + 8 + 96 + 15:]
        elif failure == "trailing":
            raw.append(0)
        else:
            struct.pack_into("<q", raw, len(TAG) + 8, TIMESTAMP + 120)
        cpi["data"] = _base58_encode(bytes(raw))
        if failure == "clock_without_block_time":
            del tx["blockTime"]
            tx["meta"]["logMessages"] = ["Log truncated"]
    elif failure == "wrong_parent":
        cpi["stackHeight"] = 2
    elif failure == "wrong_event":
        cpi["data"] = inner[34]["data"]
    elif failure == "unknown_late_operation":
        inner.append({"programId": PUMPSWAP_PROGRAM_ID, "stackHeight": 2, "data": _base58_encode(bytes([255]) * 8)})
    elif failure == "altered_create_prefix":
        logged = bytearray(base64.b64decode(tx["meta"]["logMessages"][2].split(": ")[1]))
        logged[20] ^= 1
        tx["meta"]["logMessages"][2] = "Program data: " + base64.b64encode(logged).decode()
    elif failure == "failed_metadata":
        tx["meta"]["err"] = {"InstructionError": [2, {"Custom": 1}]}
    unchanged = copy.deepcopy(tx)
    for selection in (PUMPSWAP_EVENT_NAMES, frozenset()):
        with pytest.raises(AnchorDecodeError):
            PumpSwapDecoder(event_names=selection).decode(tx)
    assert tx == unchanged


@pytest.mark.parametrize("change", ["missing", "foreign_cpi"])
def test_boost_without_its_event_keeps_the_create(change: str) -> None:
    tx = _fixture()
    expected = PumpSwapDecoder().decode(_complete(tx)).events
    tx["meta"]["logMessages"] = ["Log truncated"]
    inner = tx["meta"]["innerInstructions"][0]["instructions"]
    if change == "missing":
        inner.pop()
    else:
        inner[43]["programId"] = OTHER
    actual = PumpSwapDecoder().decode(tx).events
    assert [event.event_id for event in actual] == [event.event_id for event in expected]
