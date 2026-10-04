from __future__ import annotations

import base64
import copy
import hashlib
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
from sniper_bot.protocols.pumpswap import PUMPSWAP_PROGRAM_ID, PumpSwapDecoder

OTHER = "11111111111111111111111111111111"
TAG = bytes.fromhex("e445a52e51cb9a1d")
SELECTOR = bytes.fromhex("e1f7501ed5b38488")
SUPPLEMENT = {SELECTOR: "buy_exact_quote_in_v3"}
CONTRACT = {"buy_exact_quote_in_v3": "TradeEvent"}


def _v2(tx: dict[str, Any]) -> dict[str, Any]:
    tx = copy.deepcopy(tx)
    tx["slot"] = EVENT_ID_V2_CUTOVER_SLOT + 1
    tx["transaction"]["signatures"] = [_base58_encode(bytes([3]) * 64)]
    return tx


def _without_marker(tx: dict[str, Any]) -> dict[str, Any]:
    tx = copy.deepcopy(tx)
    logs = tx["meta"]["logMessages"]
    tx["meta"]["logMessages"] = logs[:logs.index("Log truncated")]
    return tx


def _trade_payload(tx: dict[str, Any], **changes: Any) -> bytes:
    anchor = PumpDecoder()._anchor
    cpi = tx["meta"]["innerInstructions"][0]["instructions"][-1]
    fields = anchor._decode_struct("TradeEvent", _base58_decode(cpi["data"])[16:])
    fields.update(changes)
    encoder = BorshEventEncoder(Path(__file__).parents[1] / "src/sniper_bot/protocols/pump/idl.json")
    return base64.b64decode(encoder.encode("TradeEvent", fields))


def _pump_v3() -> dict[str, Any]:
    stamp = 1_776_700_123
    encoder = BorshEventEncoder(Path(__file__).parents[1] / "src/sniper_bot/protocols/pump/idl.json")
    payload = base64.b64decode(encoder.encode("TradeEvent", {
        "timestamp": stamp, "is_buy": True, "real_token_reserves": 10,
        "ix_name": "buy_exact_quote_in",
    }))
    tx = {
        "slot": 5, "blockTime": stamp,
        "transaction": {"signatures": ["pump-v3"], "message": {}},
        "meta": {"err": None, "logMessages": [
            f"Program {PUMP_PROGRAM_ID} invoke [1]",
            "Program data: " + base64.b64encode(payload).decode(),
            f"Program {PUMP_PROGRAM_ID} success", "Log truncated",
        ]},
    }
    operation = {
        "programId": PUMP_PROGRAM_ID, "stackHeight": 2,
        "accounts": [OTHER] * 17,
        "data": _base58_encode(SELECTOR + struct.pack("<QQB", 1, 1, 0)),
    }
    cpi = {"programId": PUMP_PROGRAM_ID, "stackHeight": 3, "data": _base58_encode(TAG + payload)}
    # The authentic failure has outer 3 / operation inner 19 / Trade inner 23.
    tx["transaction"]["message"]["instructions"] = [{"programId": OTHER} for _ in range(4)]
    tx["meta"]["innerInstructions"] = [{"index": 3, "instructions": [
        *({"programId": OTHER, "stackHeight": 2} for _ in range(19)), operation,
        *({"programId": OTHER, "stackHeight": 3} for _ in range(3)), cpi,
    ]}]
    return tx


@pytest.mark.parametrize("prefix_available", [True, False])
def test_v3_nested_trade_keeps_v2_id_payload_clock_and_live_selection(prefix_available: bool) -> None:
    tx = _v2(_pump_v3())
    expected = PumpDecoder().decode(_without_marker(tx)).events[0]
    if not prefix_available:
        tx["meta"]["logMessages"] = ["Log truncated"]
    unchanged = copy.deepcopy(tx)
    event = PumpDecoder().decode(tx).events[0]
    assert event.event_id == expected.event_id
    assert {k: v for k, v in event.payload.items() if k != EVENT_IDENTITY_KEY} == {
        k: v for k, v in expected.payload.items() if k != EVENT_IDENTITY_KEY
    }
    assert event.payload["ix_name"] == "buy_exact_quote_in"
    assert event.payload[EVENT_IDENTITY_KEY] == {
        "version": 2, "ordinal": 0, "origin": "log" if prefix_available else "cpi",
    }
    assert event.instruction_index == (expected.instruction_index if prefix_available else 3)
    assert event.inner_instruction_index == (-1 if prefix_available else 23)
    live = PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES).decode(tx)
    assert live.events == [] and live.block_time == event.block_time
    assert PumpDecoder(event_names=frozenset()).decode(tx).events == []
    assert tx == unchanged


def test_v3_legacy_prefix_keeps_original_id_and_missing_trade_cannot_get_v2_identity() -> None:
    tx = _v2(_pump_v3())
    tx["slot"] = EVENT_ID_V2_CUTOVER_SLOT
    expected = PumpDecoder().decode(_without_marker(tx)).events
    actual = PumpDecoder().decode(tx).events
    assert [event.model_dump(mode="json", exclude={"observed_at"}) for event in actual] == [
        event.model_dump(mode="json", exclude={"observed_at"}) for event in expected
    ]
    tx["meta"]["logMessages"] = ["Log truncated"]
    with pytest.raises(AnchorDecodeError, match="consumed events are missing"):
        PumpDecoder().decode(tx)
    assert PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES).decode(tx).events == []


@pytest.mark.parametrize("failure", [
    "missing", "extra", "short_body", "invalid_bool", "clock", "wrong_parent",
    "foreign_cpi", "completion", "unknown_late_operation", "altered_prefix",
])
def test_v3_still_requires_complete_trade_body_clock_parent_and_exactly_one_cpi(failure: str) -> None:
    tx = _v2(_pump_v3())
    inner = tx["meta"]["innerInstructions"][0]["instructions"]
    cpi = inner[-1]
    if failure == "missing":
        inner.pop()
    elif failure == "extra":
        inner.append(copy.deepcopy(cpi))
    elif failure == "short_body":
        cpi["data"] = _base58_encode(_base58_decode(cpi["data"])[:100])
    elif failure == "invalid_bool":
        data = bytearray(_base58_decode(cpi["data"]))
        data[len(TAG) + 8 + 32 + 16] = 2
        cpi["data"] = _base58_encode(data)
    elif failure == "clock":
        tx["blockTime"] += 120
    elif failure == "wrong_parent":
        cpi["stackHeight"] = 4
    elif failure == "foreign_cpi":
        cpi["programId"] = OTHER
    elif failure == "completion":
        cpi["data"] = _base58_encode(TAG + _trade_payload(tx, real_token_reserves=0))
    elif failure == "unknown_late_operation":
        inner.append({"programId": PUMP_PROGRAM_ID, "stackHeight": 2, "data": _base58_encode(bytes([255]) * 8)})
    elif failure == "altered_prefix":
        payload = bytearray(_base58_decode(cpi["data"])[8:])
        payload[40] ^= 1
        tx["meta"]["logMessages"][1] = "Program data: " + base64.b64encode(payload).decode()
    if failure != "altered_prefix":
        tx["meta"]["logMessages"] = ["Log truncated"]
    for decoder in (PumpDecoder(), PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES)):
        with pytest.raises(AnchorDecodeError):
            decoder.decode(tx)


@pytest.mark.parametrize("selector", [
    bytes.fromhex("07051dc4f5176550"), bytes.fromhex("1c92de7726c469d5"),
    hashlib.sha256(b"global:buy_exact_quote_in").digest()[:8],
])
def test_unreviewed_v3_operations_and_event_name_hash_remain_closed(selector: bytes) -> None:
    tx = _v2(_pump_v3())
    own = tx["meta"]["innerInstructions"][0]["instructions"][19]
    own["data"] = _base58_encode(selector + _base58_decode(own["data"])[8:])
    with pytest.raises(AnchorDecodeError, match="unverified own operation"):
        PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES).decode(tx)


@pytest.mark.parametrize("supplement", [None, {}])
def test_generic_scanner_default_does_not_admit_supplemental_pump_operation(supplement: Any) -> None:
    tx = _pump_v3()
    with pytest.raises(AnchorDecodeError, match="unverified own operation"):
        PumpDecoder()._anchor.scan_verified_cpi_events(
            tx, tx["meta"]["logMessages"], event_names=PUMP_EVENT_NAMES,
            instruction_events=CONTRACT, supplemental_instructions=supplement,
        )


def test_optional_supplement_and_legacy_wrapper_leave_vendored_idl_unchanged() -> None:
    tx = _pump_v3()
    anchor = PumpDecoder()._anchor
    unchanged = copy.deepcopy(anchor.idl)
    logs = tx["meta"]["logMessages"]
    assert anchor.verified_cpi_log_prefix(
        tx, logs, event_names=PUMP_EVENT_NAMES, instruction_events=CONTRACT,
        supplemental_instructions=SUPPLEMENT,
    ) == logs[:logs.index("Log truncated")]
    assert anchor.idl == unchanged
    with pytest.raises(AnchorDecodeError, match="unverified own operation"):
        anchor.verified_cpi_log_prefix(tx, logs, event_names=PUMP_EVENT_NAMES, instruction_events=CONTRACT)
    swap = copy.deepcopy(tx)
    swap["meta"]["innerInstructions"][0]["instructions"][19]["programId"] = PUMPSWAP_PROGRAM_ID
    with pytest.raises(AnchorDecodeError, match="unverified own operation"):
        PumpSwapDecoder().decode(swap)


@pytest.mark.parametrize("failure", [
    "short_selector", "long_selector", "string_selector", "empty_name", "blank_name",
    "nonstring_name", "selector_collision", "name_collision", "event_tag_collision",
    "missing_contract", "unknown_event", "duplicate_name",
])
def test_supplemental_contract_cannot_shadow_or_bypass_explicit_event_contract(failure: str) -> None:
    tx = _pump_v3()
    anchor = PumpDecoder()._anchor
    supplement: dict[Any, Any] = dict(SUPPLEMENT)
    contract = dict(CONTRACT)
    if failure in {"short_selector", "long_selector", "string_selector"}:
        key = SELECTOR[:-1] if failure == "short_selector" else (SELECTOR + b"x" if failure == "long_selector" else SELECTOR.hex())
        supplement = {key: "buy_exact_quote_in_v3"}
    elif failure in {"empty_name", "blank_name", "nonstring_name"}:
        supplement[SELECTOR] = "" if failure == "empty_name" else (" " if failure == "blank_name" else 1)
    elif failure == "selector_collision":
        supplement = {bytes(anchor.idl["instructions"][0]["discriminator"]): "buy_exact_quote_in_v3"}
    elif failure == "name_collision":
        supplement[SELECTOR] = "buy"
        contract["buy"] = "TradeEvent"
    elif failure == "event_tag_collision":
        supplement = {TAG: "buy_exact_quote_in_v3"}
    elif failure == "missing_contract":
        contract.clear()
    elif failure == "unknown_event":
        contract["buy_exact_quote_in_v3"] = "UnreviewedEvent"
    elif failure == "duplicate_name":
        supplement[bytes([255]) * 8] = "buy_exact_quote_in_v3"
    unchanged = copy.deepcopy(anchor.idl)
    with pytest.raises(AnchorDecodeError, match="invalid supplemental instruction contract"):
        anchor.scan_verified_cpi_events(
            tx, tx["meta"]["logMessages"], event_names=PUMP_EVENT_NAMES,
            instruction_events=contract, supplemental_instructions=supplement,
        )
    assert anchor.idl == unchanged


def test_pump_supplemental_contract_is_immutable() -> None:
    from sniper_bot.protocols.pump.decoder import _SUPPLEMENTAL_INSTRUCTIONS

    assert dict(_SUPPLEMENTAL_INSTRUCTIONS) == SUPPLEMENT
    with pytest.raises(TypeError):
        _SUPPLEMENTAL_INSTRUCTIONS[SELECTOR] = "buy_v3"  # type: ignore[index]
