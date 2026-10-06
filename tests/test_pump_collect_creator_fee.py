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
from sniper_bot.protocols.pump.decoder import PUMP_EVENT_NAMES, PUMP_STATE_EVENT_NAMES

OTHER = "11111111111111111111111111111111"
TOKEN = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
TAG = bytes.fromhex("e445a52e51cb9a1d")
BUY_QUOTE_V2 = bytes.fromhex("c2ab1c46684d5b2f")
COLLECT = {"collect_creator_fee_v2": (bytes.fromhex("cf118af204221338"), 10),
           "collect_creator_fee": (bytes.fromhex("1416567bc61cdb84"), 5)}
COLLECT_EVENT = bytes.fromhex("7a027f010ebf0caf")
TIMESTAMP = 1_776_700_123


def _collect_payload(fee: int = 5) -> bytes:
    return COLLECT_EVENT + struct.pack("<q", TIMESTAMP) + bytes([2]) * 32 + struct.pack("<Q", fee) + bytes([3]) * 32


def _fixture(operation: str = "collect_creator_fee_v2", fee: int = 5) -> dict[str, Any]:
    """Outer buy-quote v2 and outer fee collection, shaped like the authentic slot 453214987."""
    encoder = BorshEventEncoder(Path(__file__).parents[1] / "src/sniper_bot/protocols/pump/idl.json")
    # The authentic Trade CPI carried 8 bytes appended after the vendored layout.
    trade = base64.b64decode(encoder.encode("TradeEvent", {
        "timestamp": TIMESTAMP, "is_buy": True, "real_token_reserves": 10, "ix_name": "buy_exact_quote_in",
    })) + bytes(8)
    collect = _collect_payload(fee)
    assert len(trade) == 397 and len(collect) == 88
    selector, accounts = COLLECT[operation]
    return {
        "slot": EVENT_ID_V2_CUTOVER_SLOT + 1, "blockTime": TIMESTAMP,
        "transaction": {"signatures": [_base58_encode(bytes([7]) * 64)], "message": {"instructions": [
            *({"programId": OTHER} for _ in range(7)),
            {"programId": PUMP_PROGRAM_ID, "accounts": [OTHER] * 27,
             "data": _base58_encode(BUY_QUOTE_V2 + struct.pack("<QQ", 1, 1))},
            {"programId": OTHER}, {"programId": OTHER},
            {"programId": PUMP_PROGRAM_ID, "accounts": [OTHER] * accounts, "data": _base58_encode(selector)},
        ]}},
        "meta": {"err": None, "logMessages": [
            f"Program {PUMP_PROGRAM_ID} invoke [1]", "Program data: " + base64.b64encode(trade).decode(),
            f"Program {PUMP_PROGRAM_ID} success", f"Program {PUMP_PROGRAM_ID} invoke [1]",
            "Program data: " + base64.b64encode(collect).decode(), f"Program {PUMP_PROGRAM_ID} success",
            "Log truncated",
        ], "innerInstructions": [
            {"index": 7, "instructions": [
                *({"programId": OTHER, "stackHeight": 2} for _ in range(6)),
                {"programId": PUMP_PROGRAM_ID, "stackHeight": 2, "data": _base58_encode(TAG + trade)},
            ]},
            {"index": 10, "instructions": [
                {"programId": TOKEN, "stackHeight": 2},
                {"programId": PUMP_PROGRAM_ID, "stackHeight": 2, "data": _base58_encode(TAG + collect)},
            ]},
        ]},
    }


def _complete(tx: dict[str, Any]) -> dict[str, Any]:
    tx = copy.deepcopy(tx)
    tx["meta"]["logMessages"] = tx["meta"]["logMessages"][:-1]
    return tx


def _fields(event: Any) -> dict[str, Any]:
    return {key: value for key, value in event.payload.items() if key != EVENT_IDENTITY_KEY}


@pytest.mark.parametrize("operation", sorted(COLLECT))
@pytest.mark.parametrize("prefix", [False, True])
def test_trade_and_ignored_collection_preserve_identity_clock_and_selection(operation: str, prefix: bool) -> None:
    tx = _fixture(operation)
    expected = PumpDecoder().decode(_complete(tx)).events[0]
    tx["meta"]["logMessages"] = tx["meta"]["logMessages"][:3] + ["Log truncated"] if prefix else ["Log truncated"]
    unchanged = copy.deepcopy(tx)
    decoded = PumpDecoder().decode(tx)
    assert len(decoded.events) == 1
    actual = decoded.events[0]
    assert actual.event_type.value == "swap_buy"
    assert actual.event_id == expected.event_id and actual.block_time == expected.block_time
    assert _fields(actual) == _fields(expected)
    assert actual.payload[EVENT_IDENTITY_KEY] == {"version": 2, "ordinal": 0, "origin": "log" if prefix else "cpi"}
    assert actual.instruction_index == (expected.instruction_index if prefix else 7)
    assert actual.inner_instruction_index == (-1 if prefix else 6)
    assert decoded.appended_events == ("TradeEvent",) and decoded.unknown_discriminators == ()
    for selection in (PUMP_STATE_EVENT_NAMES, frozenset()):
        ignored = PumpDecoder(event_names=selection).decode(tx)
        assert ignored.events == [] and ignored.block_time == decoded.block_time
    assert tx == unchanged


def test_trade_identity_is_stable_at_every_log_prefix() -> None:
    tx = _fixture()
    logs = tx["meta"]["logMessages"][:-1]
    expected = PumpDecoder().decode(_complete(tx)).events[0]
    for length in range(len(logs) + 1):
        partial = copy.deepcopy(tx)
        partial["meta"]["logMessages"] = logs[:length] + ["Log truncated"]
        unchanged = copy.deepcopy(partial)
        events = PumpDecoder().decode(partial).events
        assert [event.event_id for event in events] == [expected.event_id]
        assert _fields(events[0]) == _fields(expected) and events[0].block_time == expected.block_time
        assert PumpDecoder(event_names=frozenset()).decode(partial).events == []
        assert partial == unchanged


def test_legacy_keeps_visible_trade_but_does_not_recover_missing_trade() -> None:
    tx = _fixture()
    tx["slot"] = EVENT_ID_V2_CUTOVER_SLOT
    expected = PumpDecoder().decode(_complete(tx)).events[0]
    tx["meta"]["logMessages"] = tx["meta"]["logMessages"][:3] + ["Log truncated"]
    actual = PumpDecoder().decode(tx).events[0]
    assert actual.model_dump(exclude={"observed_at"}) == expected.model_dump(exclude={"observed_at"})
    tx["meta"]["logMessages"] = ["Log truncated"]
    with pytest.raises(AnchorDecodeError, match="missing from the verified log prefix"):
        PumpDecoder().decode(tx)
    empty = PumpDecoder(event_names=frozenset()).decode(tx)
    assert empty.events == [] and empty.block_time == actual.block_time


@pytest.mark.parametrize("operation", sorted(COLLECT))
@pytest.mark.parametrize("fee", [0, 2**64 - 1])
@pytest.mark.parametrize("block_time_present", [False, True])
def test_standalone_collection_dates_from_cpi_without_state_or_identity(
    operation: str, fee: int, block_time_present: bool,
) -> None:
    tx = _fixture(operation, fee)
    expected = PumpDecoder().decode(_complete(tx)).block_time
    tx["transaction"]["message"]["instructions"] = tx["transaction"]["message"]["instructions"][10:]
    tx["meta"]["innerInstructions"] = [{**tx["meta"]["innerInstructions"][1], "index": 0}]
    tx["meta"]["logMessages"] = ["Log truncated"]
    if not block_time_present:
        del tx["blockTime"]
    unchanged = copy.deepcopy(tx)
    for selection in (PUMP_EVENT_NAMES, PUMP_STATE_EVENT_NAMES, frozenset()):
        decoded = PumpDecoder(event_names=selection).decode(tx)
        assert decoded.events == [] and decoded.block_time == expected
    fields = PumpDecoder()._anchor._decode_struct("CollectCreatorFeeEvent", _collect_payload(fee)[8:])
    assert fields["creator_fee"] == fee
    assert "CollectCreatorFeeEvent" not in PUMP_EVENT_NAMES
    assert tx == unchanged


@pytest.mark.parametrize("operation", sorted(COLLECT))
@pytest.mark.parametrize("failure", [
    "extra", "short", "trailing", "clock", "wrong_parent", "wrong_event",
    "unknown_late_operation", "altered_trade_prefix", "clock_without_block_time", "failed_metadata",
])
def test_ignored_collection_requires_complete_body_clock_parent_and_exactly_one_cpi(
    operation: str, failure: str,
) -> None:
    tx = _fixture(operation)
    inner = tx["meta"]["innerInstructions"][1]["instructions"]
    cpi = inner[1]
    if failure == "extra":
        inner.append(copy.deepcopy(cpi))
    elif failure in {"short", "trailing", "clock", "clock_without_block_time"}:
        raw = bytearray(_base58_decode(cpi["data"]))
        if failure == "short":
            raw.pop()
        elif failure == "trailing":
            raw.append(0)
        else:
            struct.pack_into("<q", raw, len(TAG) + 8, TIMESTAMP + 120)
        cpi["data"] = _base58_encode(bytes(raw))
        if failure == "clock_without_block_time":
            del tx["blockTime"]
            tx["meta"]["logMessages"] = ["Log truncated"]
    elif failure == "wrong_parent":
        cpi["stackHeight"] = 3
    elif failure == "wrong_event":
        cpi["data"] = tx["meta"]["innerInstructions"][0]["instructions"][-1]["data"]
    elif failure == "unknown_late_operation":
        tx["transaction"]["message"]["instructions"].append(
            {"programId": PUMP_PROGRAM_ID, "data": _base58_encode(bytes([255]) * 8)})
    elif failure == "altered_trade_prefix":
        logged = bytearray(base64.b64decode(tx["meta"]["logMessages"][1].split(": ")[1]))
        logged[20] ^= 1
        tx["meta"]["logMessages"][1] = "Program data: " + base64.b64encode(logged).decode()
    elif failure == "failed_metadata":
        tx["meta"]["err"] = {"InstructionError": [10, {"Custom": 1}]}
    unchanged = copy.deepcopy(tx)
    for selection in (PUMP_EVENT_NAMES, frozenset()):
        with pytest.raises(AnchorDecodeError):
            PumpDecoder(event_names=selection).decode(tx)
    assert tx == unchanged


@pytest.mark.parametrize("operation", sorted(COLLECT))
@pytest.mark.parametrize("change", ["missing", "foreign_cpi"])
def test_collection_without_its_event_keeps_the_trade(operation: str, change: str) -> None:
    # A collection with nothing to move emits no event (observed natively).
    tx = _fixture(operation)
    expected = PumpDecoder().decode(_complete(tx)).events
    tx["meta"]["logMessages"] = ["Log truncated"]
    inner = tx["meta"]["innerInstructions"][1]["instructions"]
    if change == "missing":
        inner.pop()
    else:
        inner[1]["programId"] = OTHER
    actual = PumpDecoder().decode(tx).events
    assert [event.event_id for event in actual] == [event.event_id for event in expected]


def test_one_collection_cannot_carry_the_event_of_another() -> None:
    tx = _fixture("collect_creator_fee")
    inner = tx["meta"]["innerInstructions"][1]["instructions"]
    inner.append(copy.deepcopy(inner[1]))
    v2 = _fixture("collect_creator_fee_v2")["transaction"]["message"]["instructions"][10]
    tx["transaction"]["message"]["instructions"].append(v2)
    with pytest.raises(AnchorDecodeError, match="does not cover its parent"):
        PumpDecoder().decode(tx)
