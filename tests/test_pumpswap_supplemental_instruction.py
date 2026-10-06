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
from sniper_bot.protocols.pumpswap import PUMPSWAP_PROGRAM_ID, PumpSwapDecoder
from sniper_bot.protocols.pumpswap.decoder import PUMPSWAP_EVENT_NAMES

OTHER = "11111111111111111111111111111111"
TAG = bytes.fromhex("e445a52e51cb9a1d")
OPERATIONS = {
    "buy_exact_quote_in_v2": bytes.fromhex("c2ab1c46684d5b2f"),
    "buy_v2": bytes.fromhex("b817ee6167c5d33d"),
    "sell_v2": bytes.fromhex("5df6823ce7e940b2"),
}


def _event_name(name: str) -> str:
    return "SellEvent" if name == "sell_v2" else "BuyEvent"


def _payload(name: str, *, appended: bool = False) -> bytes:
    encoder = BorshEventEncoder(Path(__file__).parents[1] / "src/sniper_bot/protocols/pumpswap/idl.json")
    raw = base64.b64decode(encoder.encode(_event_name(name), {"timestamp": 1_776_700_123}))
    return raw + (bytes(8) if appended else b"")


def _fixture(name: str, *, appended: bool = False) -> dict[str, Any]:
    payload = _payload(name, appended=appended)
    return {
        "slot": EVENT_ID_V2_CUTOVER_SLOT + 1, "blockTime": 1_776_700_123,
        "transaction": {"signatures": [_base58_encode(bytes([7]) * 64)], "message": {
            "instructions": [{"programId": OTHER} for _ in range(4)],
        }},
        "meta": {"err": None, "logMessages": [
            f"Program {OTHER} invoke [1]", f"Program {PUMPSWAP_PROGRAM_ID} invoke [2]",
            "Program data: " + base64.b64encode(payload).decode(),
            f"Program {PUMPSWAP_PROGRAM_ID} success", f"Program {OTHER} success", "Log truncated",
        ], "innerInstructions": [{"index": 3, "instructions": [
            *({"programId": OTHER, "stackHeight": 2} for _ in range(21)),
            {"programId": PUMPSWAP_PROGRAM_ID, "stackHeight": 2, "accounts": [OTHER] * 17,
             "data": _base58_encode(OPERATIONS[name] + struct.pack("<QQ", 1, 1))},
            *({"programId": OTHER, "stackHeight": 3} for _ in range(3)),
            {"programId": PUMPSWAP_PROGRAM_ID, "stackHeight": 3, "data": _base58_encode(TAG + payload)},
        ]}]},
    }


def _without_marker(tx: dict[str, Any]) -> dict[str, Any]:
    tx = copy.deepcopy(tx)
    tx["meta"]["logMessages"] = tx["meta"]["logMessages"][:-1]
    return tx


@pytest.mark.parametrize("name", OPERATIONS)
@pytest.mark.parametrize("appended", [False, True])
@pytest.mark.parametrize("prefix", [False, True])
def test_v2_nested_trade_preserves_identity_payload_clock_and_selection(name: str, appended: bool, prefix: bool) -> None:
    tx = _fixture(name, appended=appended)
    complete = PumpSwapDecoder().decode(_without_marker(tx))
    expected = complete.events[0]
    if not prefix:
        tx["meta"]["logMessages"] = ["Log truncated"]
    unchanged = copy.deepcopy(tx)
    decoded = PumpSwapDecoder().decode(tx)
    actual = decoded.events[0]
    assert actual.event_id == expected.event_id and actual.block_time == expected.block_time
    assert {k: v for k, v in actual.payload.items() if k != EVENT_IDENTITY_KEY} == {
        k: v for k, v in expected.payload.items() if k != EVENT_IDENTITY_KEY
    }
    assert actual.payload[EVENT_IDENTITY_KEY] == {"version": 2, "ordinal": 0, "origin": "log" if prefix else "cpi"}
    assert decoded.appended_events == complete.appended_events == ((_event_name(name),) if appended else ())
    assert actual.instruction_index == (expected.instruction_index if prefix else 3)
    assert actual.inner_instruction_index == (-1 if prefix else 25)
    assert PumpSwapDecoder(event_names=frozenset({_event_name(name)})).decode(tx).events[0].event_id == actual.event_id
    assert PumpSwapDecoder(event_names=frozenset()).decode(tx).events == []
    assert tx == unchanged


@pytest.mark.parametrize("name", OPERATIONS)
def test_legacy_keeps_original_prefix_id_and_missing_event_cannot_be_recovered(name: str) -> None:
    tx = _fixture(name)
    tx["slot"] = EVENT_ID_V2_CUTOVER_SLOT
    actual = PumpSwapDecoder().decode(tx).events
    expected = PumpSwapDecoder().decode(_without_marker(tx)).events
    assert [e.model_dump(exclude={"observed_at"}) for e in actual] == [e.model_dump(exclude={"observed_at"}) for e in expected]
    tx["meta"]["logMessages"] = ["Log truncated"]
    with pytest.raises(AnchorDecodeError, match="missing from the verified log prefix"):
        PumpSwapDecoder().decode(tx)
    assert PumpSwapDecoder(event_names=frozenset()).decode(tx).events == []


@pytest.mark.parametrize("name", OPERATIONS)
@pytest.mark.parametrize("failure", [
    "missing", "extra", "short_body", "invalid_bool", "clock", "wrong_parent",
    "foreign_cpi", "wrong_event", "unknown_late_operation", "altered_prefix",
])
def test_full_body_clock_parent_exactly_one_and_prefix_remain_required(name: str, failure: str) -> None:
    tx = _fixture(name)
    inner = tx["meta"]["innerInstructions"][0]["instructions"]
    cpi = inner[-1]
    if failure == "missing":
        inner.pop()
    elif failure == "extra":
        inner.append(copy.deepcopy(cpi))
    elif failure == "short_body":
        cpi["data"] = _base58_encode(_base58_decode(cpi["data"])[:40])
    elif failure == "invalid_bool":
        raw = bytearray(_base58_decode(cpi["data"]))
        raw[-25] = 2  # can_boost before base_supply and the two holder fields.
        cpi["data"] = _base58_encode(bytes(raw))
    elif failure == "clock":
        tx["blockTime"] += 3600
    elif failure == "wrong_parent":
        cpi["stackHeight"] = 2
    elif failure == "foreign_cpi":
        cpi["programId"] = OTHER
    elif failure == "wrong_event":
        other = "buy_v2" if name == "sell_v2" else "sell_v2"
        cpi["data"] = _base58_encode(TAG + _payload(other))
    elif failure == "unknown_late_operation":
        inner.append({"programId": PUMPSWAP_PROGRAM_ID, "stackHeight": 2, "data": _base58_encode(bytes([255]) * 8)})
    elif failure == "altered_prefix":
        index = 2
        logged = bytearray(base64.b64decode(tx["meta"]["logMessages"][index].split(": ")[1]))
        logged[20] ^= 1
        tx["meta"]["logMessages"][index] = "Program data: " + base64.b64encode(logged).decode()
    unchanged = copy.deepcopy(tx)
    for selected in (PUMPSWAP_EVENT_NAMES, frozenset()):
        with pytest.raises(AnchorDecodeError):
            PumpSwapDecoder(event_names=selected).decode(tx)
    assert tx == unchanged


@pytest.mark.parametrize("name", OPERATIONS)
@pytest.mark.parametrize("supplement", [None, {}])
def test_generic_scanner_default_does_not_admit_supplemental_operation(name: str, supplement: Any) -> None:
    from sniper_bot.protocols.pumpswap.decoder import _TRUNCATION_INSTRUCTION_EVENTS

    tx = _fixture(name)
    with pytest.raises(AnchorDecodeError, match="unverified own operation"):
        PumpSwapDecoder()._anchor.scan_verified_cpi_events(
            tx, tx["meta"]["logMessages"], event_names=PUMPSWAP_EVENT_NAMES,
            instruction_events=_TRUNCATION_INSTRUCTION_EVENTS, supplemental_instructions=supplement,
        )


@pytest.mark.parametrize("name", OPERATIONS)
def test_same_global_selector_does_not_bypass_pump_trade_event_contract(name: str) -> None:
    tx = _fixture(name)
    tx["meta"]["logMessages"] = ["Log truncated"]
    inner = tx["meta"]["innerInstructions"][0]["instructions"]
    for instruction in inner:
        if instruction["programId"] == PUMPSWAP_PROGRAM_ID:
            instruction["programId"] = PUMP_PROGRAM_ID
    with pytest.raises(AnchorDecodeError):
        PumpDecoder().decode(tx)


SWEEPS = {bytes.fromhex("20f6bf3408c949ba"): "sweep_creator_fee", bytes.fromhex("0830be07b644b7e5"): "sweep_protocol_fee"}


def test_unknown_control_operations_remain_closed() -> None:
    tx = _fixture("buy_v2")
    own = tx["meta"]["innerInstructions"][0]["instructions"][21]
    own["data"] = _base58_encode(bytes([255]) * 8 + _base58_decode(own["data"])[8:])
    with pytest.raises(AnchorDecodeError, match="unverified own operation"):
        PumpSwapDecoder().decode(tx)


@pytest.mark.parametrize("selector", sorted(SWEEPS))
def test_reviewed_sweep_control_cannot_claim_a_trade_event(selector: bytes) -> None:
    tx = _fixture("buy_v2")
    own = tx["meta"]["innerInstructions"][0]["instructions"][21]
    own["data"] = _base58_encode(selector + _base58_decode(own["data"])[8:])
    with pytest.raises(AnchorDecodeError, match="does not cover its parent"):
        PumpSwapDecoder().decode(tx)


def test_supplemental_map_is_immutable_and_separate_from_pump() -> None:
    from sniper_bot.protocols.pump.decoder import _SUPPLEMENTAL_INSTRUCTIONS as pump_map
    from sniper_bot.protocols.pumpswap.decoder import _SUPPLEMENTAL_INSTRUCTIONS

    assert dict(_SUPPLEMENTAL_INSTRUCTIONS) == {
        **{selector: name for name, selector in OPERATIONS.items()}, **SWEEPS,
    }
    # Anchor selectors hash only the name, so both programs share the sweeps;
    # each decoder still binds them to its own program and event.
    assert set(_SUPPLEMENTAL_INSTRUCTIONS) & set(pump_map) == set(SWEEPS)
    assert all(pump_map[selector] == name for selector, name in SWEEPS.items())
    with pytest.raises(TypeError):
        _SUPPLEMENTAL_INSTRUCTIONS[OPERATIONS["buy_v2"]] = "sell_v2"  # type: ignore[index]


def test_composite_buys_and_sell_preserve_per_type_ordinals_at_every_prefix() -> None:
    parts = [_fixture(name) for name in OPERATIONS]
    tx = copy.deepcopy(parts[0])
    tx["transaction"]["message"]["instructions"] = [{"programId": OTHER} for _ in parts]
    tx["meta"]["innerInstructions"] = [
        {"index": index, "instructions": part["meta"]["innerInstructions"][0]["instructions"]}
        for index, part in enumerate(parts)
    ]
    logs = [line for part in parts for line in part["meta"]["logMessages"][:-1]]
    tx["meta"]["logMessages"] = logs + ["Log truncated"]
    expected = PumpSwapDecoder().decode(_without_marker(tx)).events
    assert [event.event_type.value for event in expected] == ["swap_buy", "swap_buy", "swap_sell"]
    assert [event.payload[EVENT_IDENTITY_KEY]["ordinal"] for event in expected] == [0, 1, 0]
    for length in range(len(logs) + 1):
        partial = copy.deepcopy(tx)
        partial["meta"]["logMessages"] = logs[:length] + ["Log truncated"]
        unchanged = copy.deepcopy(partial)
        actual = PumpSwapDecoder().decode(partial).events
        assert [event.event_id for event in actual] == [event.event_id for event in expected]
        for index, (event, complete) in enumerate(zip(actual, expected, strict=True)):
            assert {k: v for k, v in event.payload.items() if k != EVENT_IDENTITY_KEY} == {
                k: v for k, v in complete.payload.items() if k != EVENT_IDENTITY_KEY
            }
            if event.payload[EVENT_IDENTITY_KEY]["origin"] == "cpi":
                assert event.instruction_index == index and event.inner_instruction_index == 25
            else:
                assert event.instruction_index == complete.instruction_index and event.inner_instruction_index == -1
        assert [event.event_id for event in PumpSwapDecoder(event_names=frozenset({"BuyEvent"})).decode(partial).events] == [event.event_id for event in expected[:2]]
        assert PumpSwapDecoder(event_names=frozenset()).decode(partial).events == []
        assert partial == unchanged
