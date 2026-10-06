"""Every reviewed truncated-log operation obeys its CPI contract.

Parametrized over the live truncation mappings, so a control added later is
covered without a new fixture. An ignored control proves zero or one complete
CPI of its own event (fee controls skip the event when nothing moves) and
creates no state event; a control without an event of its own proves none;
consumed operations still need exactly one. Anything extra, malformed,
unknown or substituted fails closed.
"""

from __future__ import annotations

import base64
import copy
import struct
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from scripts.benchmark_postgres_capacity import BorshEventEncoder
from sniper_bot.events import EVENT_ID_V2_CUTOVER_SLOT
from sniper_bot.protocols import AnchorDecodeError
from sniper_bot.protocols.anchor import AnchorIdlDecoder, _base58_decode, _base58_encode, contract_events
from sniper_bot.protocols.pump import PUMP_PROGRAM_ID, PumpDecoder
from sniper_bot.protocols.pump import decoder as pump_decoder
from sniper_bot.protocols.pumpswap import PUMPSWAP_PROGRAM_ID, PumpSwapDecoder
from sniper_bot.protocols.pumpswap import decoder as pumpswap_decoder

OTHER = "11111111111111111111111111111111"
TAG = bytes.fromhex("e445a52e51cb9a1d")
TIMESTAMP = 1_776_700_123
ROOT = Path(__file__).parents[1] / "src/sniper_bot/protocols"
PROTOCOLS: dict[str, tuple[Any, str, Any, frozenset[str]]] = {
    "pump": (PumpDecoder, PUMP_PROGRAM_ID, pump_decoder, pump_decoder.PUMP_EVENT_NAMES),
    "pumpswap": (PumpSwapDecoder, PUMPSWAP_PROGRAM_ID, pumpswap_decoder, pumpswap_decoder.PUMPSWAP_EVENT_NAMES),
}
# Events whose validation needs a non-empty vector to be well formed.
OVERRIDES: dict[str, dict[str, Any]] = {
    "DistributeCreatorFeesEvent": {"shareholders": [{"address": OTHER, "share_bps": 10_000}]},
}
CASES = [
    (protocol, operation, event)
    for protocol, (_, _, module, consumed) in PROTOCOLS.items()
    for operation, event in sorted(module._TRUNCATION_INSTRUCTION_EVENTS.items())
    if isinstance(event, str) and event not in consumed
]
EVENTLESS = [
    (protocol, operation)
    for protocol, (_, _, module, _) in PROTOCOLS.items()
    for operation, event in sorted(module._TRUNCATION_INSTRUCTION_EVENTS.items())
    if event is None
]
CONSUMED = [
    ("pump", "buy", "TradeEvent"), ("pump", "create", "CreateEvent"), ("pump", "create_v2", "CreateEvent"),
    ("pump", "migrate", "CompletePumpAmmMigrationEvent"), ("pump", "migrate_v2", "CompletePumpAmmMigrationEvent"),
    ("pumpswap", "buy", "BuyEvent"), ("pumpswap", "create_pool", "CreatePoolEvent"),
    ("pumpswap", "boost_buy_and_burn", "BuyEvent"),
]


def _anchor(protocol: str) -> AnchorIdlDecoder:
    anchor: AnchorIdlDecoder = PROTOCOLS[protocol][0]()._anchor
    return anchor


def _event_fields(protocol: str, event: str) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = _anchor(protocol)._types[event]["fields"]
    return fields


def _selector(protocol: str, operation: str) -> bytes:
    module = PROTOCOLS[protocol][2]
    declared = {item["name"]: bytes(item["discriminator"]) for item in _anchor(protocol).idl["instructions"]}
    supplemental = {name: selector for selector, name in module._SUPPLEMENTAL_INSTRUCTIONS.items()}
    return declared.get(operation) or supplemental[operation]


def _event_payload(protocol: str, event: str) -> bytes:
    anchor = _anchor(protocol)
    discriminator = next(key for key, name in anchor._events.items() if name == event)
    names = {field["name"] for field in _event_fields(protocol, event)}
    values = {"timestamp": TIMESTAMP} if "timestamp" in names else {}
    if event in {item["name"] for item in anchor.idl["events"]}:
        encoder = BorshEventEncoder(ROOT / protocol / "idl.json")
        return base64.b64decode(encoder.encode(event, {**values, **OVERRIDES.get(event, {})}))
    # A supplemental event outside the pinned IDL file: fixed-size fields only.
    body = b""
    for field in _event_fields(protocol, event):
        kind = field["type"]
        if field["name"] == "timestamp":
            body += struct.pack("<q", TIMESTAMP)
        else:
            body += {"pubkey": bytes(32), "u64": bytes(8), "u8": bytes(1)}[kind]
    return discriminator + body


def _fixture(protocol: str, operation: str, payload: bytes | None, *more: bytes) -> dict[str, Any]:
    program = PROTOCOLS[protocol][1]
    inner: list[dict[str, Any]] = [
        {"programId": program, "stackHeight": 2, "data": _base58_encode(_selector(protocol, operation))},
        {"programId": OTHER, "stackHeight": 3},
    ]
    for item in (payload, *more):
        if item is not None:
            inner.append({"programId": program, "stackHeight": 3, "data": _base58_encode(TAG + item)})
    return {
        "slot": EVENT_ID_V2_CUTOVER_SLOT + 1, "blockTime": TIMESTAMP,
        "transaction": {"signatures": [_base58_encode(bytes([5]) * 64)], "message": {
            "instructions": [{"programId": OTHER}],
        }},
        "meta": {"err": None, "logMessages": ["Log truncated"],
                 "innerInstructions": [{"index": 0, "instructions": inner}]},
    }


def _decode_all(protocol: str, tx: dict[str, Any]) -> list[Any]:
    decoder_cls, _, _, consumed = PROTOCOLS[protocol]
    return [decoder_cls(event_names=selection).decode(tx) for selection in (consumed, frozenset())]


def test_every_reviewed_operation_names_a_known_instruction_and_event() -> None:
    assert len(CASES) >= 40 and len(EVENTLESS) >= 2
    for protocol, (_, _, module, consumed) in PROTOCOLS.items():
        anchor = _anchor(protocol)
        for operation, contract in module._TRUNCATION_INSTRUCTION_EVENTS.items():
            events = contract_events(contract)
            assert _selector(protocol, operation)
            assert set(events) <= set(anchor._events.values())
            # Only ignored controls may skip their events.
            assert (operation in module._OPTIONAL_INSTRUCTIONS) == (bool(events) and consumed.isdisjoint(events))


def _dating_cases() -> list[tuple[str, str, str, bool]]:
    # An event without a Clock field cannot date a transaction lacking blockTime.
    return [
        (protocol, operation, event, block_time_present)
        for protocol, operation, event in CASES
        for block_time_present in (True, False)
        if block_time_present or "timestamp" in {field["name"] for field in _event_fields(protocol, event)}
    ]


@pytest.mark.parametrize(("protocol", "operation", "event", "block_time_present"), _dating_cases())
def test_ignored_control_alone_is_dated_by_its_cpi_without_state(
    protocol: str, operation: str, event: str, block_time_present: bool,
) -> None:
    tx = _fixture(protocol, operation, _event_payload(protocol, event))
    if not block_time_present:
        del tx["blockTime"]
    unchanged = copy.deepcopy(tx)
    for decoded in _decode_all(protocol, tx):
        assert decoded.events == []
        assert decoded.block_time == datetime.fromtimestamp(TIMESTAMP, tz=timezone.utc)
    assert tx == unchanged


@pytest.mark.parametrize(("protocol", "operation", "event"), CASES)
def test_ignored_control_may_skip_its_event(protocol: str, operation: str, event: str) -> None:
    tx = _fixture(protocol, operation, None)
    for decoded in _decode_all(protocol, tx):
        assert decoded.events == []
        assert decoded.block_time == datetime.fromtimestamp(TIMESTAMP, tz=timezone.utc)


@pytest.mark.parametrize(("protocol", "operation", "event"), CASES)
@pytest.mark.parametrize("failure", ["extra", "short", "trailing", "wrong_event", "unknown_event"])
def test_ignored_control_still_rejects_any_other_cpi(
    protocol: str, operation: str, event: str, failure: str,
) -> None:
    tx = _fixture(protocol, operation, _event_payload(protocol, event))
    inner = tx["meta"]["innerInstructions"][0]["instructions"]
    cpi = inner[2]
    if failure == "extra":
        inner.append(copy.deepcopy(cpi))
    elif failure in {"short", "trailing"}:
        raw = bytearray(_base58_decode(cpi["data"]))
        if failure == "short":
            raw.pop()
        else:
            raw.append(0)
        cpi["data"] = _base58_encode(bytes(raw))
    elif failure == "wrong_event":
        other = next(ev for proto, _, ev in CASES if proto == protocol and ev != event)
        cpi["data"] = _base58_encode(TAG + _event_payload(protocol, other))
    else:
        cpi["data"] = _base58_encode(TAG + bytes([254]) * 8 + bytes(16))
    for selection in (PROTOCOLS[protocol][3], frozenset()):
        with pytest.raises(AnchorDecodeError):
            PROTOCOLS[protocol][0](event_names=selection).decode(tx)


@pytest.mark.parametrize(("protocol", "operation"), EVENTLESS)
def test_control_without_an_event_proves_none(protocol: str, operation: str) -> None:
    tx = _fixture(protocol, operation, None)
    for decoded in _decode_all(protocol, tx):
        assert decoded.events == []
    some_event = next(ev for proto, _, ev in CASES if proto == protocol)
    tx = _fixture(protocol, operation, _event_payload(protocol, some_event))
    for selection in (PROTOCOLS[protocol][3], frozenset()):
        with pytest.raises(AnchorDecodeError, match="does not cover its parent"):
            PROTOCOLS[protocol][0](event_names=selection).decode(tx)


@pytest.mark.parametrize(("protocol", "operation", "event"), CONSUMED)
def test_consumed_operation_still_needs_its_event(protocol: str, operation: str, event: str) -> None:
    assert operation not in PROTOCOLS[protocol][2]._OPTIONAL_INSTRUCTIONS
    tx = _fixture(protocol, operation, None)
    for selection in (PROTOCOLS[protocol][3], frozenset()):
        with pytest.raises(AnchorDecodeError, match="do not cover"):
            PROTOCOLS[protocol][0](event_names=selection).decode(tx)


def test_an_operation_whose_event_is_selected_cannot_be_optional() -> None:
    tx = _fixture("pump", "buy", None)
    with pytest.raises(AnchorDecodeError, match="invalid optional instruction contract"):
        _anchor("pump").scan_verified_cpi_events(
            tx, ["Log truncated"], event_names=frozenset({"TradeEvent"}),
            instruction_events={"buy": "TradeEvent"}, optional_instructions=frozenset({"buy"}),
        )


@pytest.mark.parametrize("collision", ["discriminator", "name", "tag", "length", "kind"])
def test_supplemental_events_cannot_shadow_the_pinned_idl(collision: str) -> None:
    path = ROOT / "pump/idl.json"
    pinned = AnchorIdlDecoder(path)
    trade = next(key for key, name in pinned._events.items() if name == "TradeEvent")
    definition: dict[str, Any] = {"kind": "struct", "fields": [{"name": "timestamp", "type": "i64"}]}
    entry: tuple[bytes, tuple[str, dict[str, Any]]] = {
        "discriminator": (trade, ("NewEvent", definition)),
        "name": (bytes([9]) * 8, ("TradeEvent", definition)),
        "tag": (TAG, ("NewEvent", definition)),
        "length": (bytes([9]) * 7, ("NewEvent", definition)),
        "kind": (bytes([9]) * 8, ("NewEvent", {"kind": "enum", "variants": []})),
    }[collision]
    with pytest.raises(AnchorDecodeError, match="invalid supplemental event contract"):
        AnchorIdlDecoder(path, supplemental_events=dict([entry]))


def _encoded(protocol: str, event: str, **fields: Any) -> bytes:
    encoder = BorshEventEncoder(ROOT / protocol / "idl.json")
    return base64.b64decode(encoder.encode(event, {"timestamp": TIMESTAMP, **fields}))


@pytest.mark.parametrize("order", ["contract", "reversed", "missing_burn", "missing_buy", "extra_burn", "neither"])
def test_boost_buy_and_burn_proves_its_ordered_buy_and_burn(order: str) -> None:
    from sniper_bot.registry import WSOL_MINT

    buy = _encoded("pumpswap", "BuyEvent", quote_mint=WSOL_MINT) if "quote_mint" in {
        field["name"] for field in _event_fields("pumpswap", "BuyEvent")} else _encoded("pumpswap", "BuyEvent")
    burn = _encoded("pumpswap", "BoostBuyAndBurnEvent")
    payloads = {
        "contract": (buy, burn), "reversed": (burn, buy), "missing_burn": (buy,), "missing_buy": (burn,),
        "extra_burn": (buy, burn, burn), "neither": (),
    }[order]
    tx = _fixture("pumpswap", "boost_buy_and_burn", *payloads) if payloads else _fixture(
        "pumpswap", "boost_buy_and_burn", None)
    assert "boost_buy_and_burn" not in pumpswap_decoder._OPTIONAL_INSTRUCTIONS
    if order == "contract":
        decoded = PumpSwapDecoder().decode(tx)
        assert [event.event_type.value for event in decoded.events] == ["swap_buy"]
        assert decoded.events[0].inner_instruction_index == 2
        assert PumpSwapDecoder(event_names=frozenset()).decode(tx).events == []
        return
    for selection in (pumpswap_decoder.PUMPSWAP_EVENT_NAMES, frozenset()):
        with pytest.raises(AnchorDecodeError):
            PumpSwapDecoder(event_names=selection).decode(tx)


@pytest.mark.parametrize("case", ["completion", "completion_without_complete", "complete_after_plain_buy",
                                  "sell_at_zero_reserves"])
def test_completing_buy_must_then_prove_the_completion(case: str) -> None:
    completing = case != "complete_after_plain_buy"
    trade = _encoded("pump", "TradeEvent", is_buy=case != "sell_at_zero_reserves",
                     real_token_reserves=0 if completing else 10, ix_name="buy")
    complete = _encoded("pump", "CompleteEvent")
    payloads = {
        "completion": (trade, complete), "completion_without_complete": (trade,),
        "complete_after_plain_buy": (trade, complete), "sell_at_zero_reserves": (trade,),
    }[case]
    tx = _fixture("pump", "buy", *payloads)
    if case == "completion":
        decoded = PumpDecoder().decode(tx)
        assert [event.event_type.value for event in decoded.events] == ["swap_buy", "bonding_curve_completed"]
        state = PumpDecoder(event_names=pump_decoder.PUMP_STATE_EVENT_NAMES).decode(tx)
        assert [event.event_type.value for event in state.events] == ["bonding_curve_completed"]
        return
    if case == "sell_at_zero_reserves":
        decoded = PumpDecoder().decode(tx)
        assert [event.event_type.value for event in decoded.events] == ["swap_sell"]
        return
    for selection in (pump_decoder.PUMP_EVENT_NAMES, frozenset()):
        with pytest.raises(AnchorDecodeError):
            PumpDecoder(event_names=selection).decode(tx)
