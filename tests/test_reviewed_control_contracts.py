"""Every reviewed ignored control obeys the same truncated CPI contract.

Parametrized over the live truncation mappings, so a control added later is
covered without a new fixture: alone in a truncated transaction it is dated by
its one complete CPI event and creates no state event, and any incomplete,
extra, malformed, foreign or substituted CPI still fails closed.
"""

from __future__ import annotations

import base64
import copy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from scripts.benchmark_postgres_capacity import BorshEventEncoder
from sniper_bot.events import EVENT_ID_V2_CUTOVER_SLOT
from sniper_bot.protocols import AnchorDecodeError
from sniper_bot.protocols.anchor import _base58_decode, _base58_encode
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
    if event not in consumed
]


def _event_fields(protocol: str, event: str) -> set[str]:
    anchor = PROTOCOLS[protocol][0]()._anchor
    definition = next(item for item in anchor.idl["types"] if item["name"] == event)
    return {field["name"] for field in definition["type"]["fields"]}


def _fixture(protocol: str, operation: str, event: str) -> dict[str, Any]:
    decoder_cls, program, _, _ = PROTOCOLS[protocol]
    encoder = BorshEventEncoder(ROOT / protocol / "idl.json")
    fields = {"timestamp": TIMESTAMP} if "timestamp" in _event_fields(protocol, event) else {}
    payload = base64.b64decode(encoder.encode(event, {**fields, **OVERRIDES.get(event, {})}))
    selector = next(
        bytes(item["discriminator"]) for item in decoder_cls()._anchor.idl["instructions"] if item["name"] == operation
    )
    return {
        "slot": EVENT_ID_V2_CUTOVER_SLOT + 1, "blockTime": TIMESTAMP,
        "transaction": {"signatures": [_base58_encode(bytes([5]) * 64)], "message": {
            "instructions": [{"programId": OTHER}],
        }},
        "meta": {"err": None, "logMessages": ["Log truncated"], "innerInstructions": [{"index": 0, "instructions": [
            {"programId": program, "stackHeight": 2, "data": _base58_encode(selector)},
            {"programId": OTHER, "stackHeight": 3},
            {"programId": program, "stackHeight": 3, "data": _base58_encode(TAG + payload)},
        ]}]},
    }


def test_every_ignored_control_names_a_declared_instruction_and_event() -> None:
    assert len(CASES) >= 40
    for protocol, operation, event in CASES:
        anchor = PROTOCOLS[protocol][0]()._anchor
        assert operation in {item["name"] for item in anchor.idl["instructions"]}
        assert event in set(anchor._events.values())


def _dating_cases() -> list[tuple[str, str, str, bool]]:
    # An event without a Clock field cannot date a transaction lacking blockTime.
    return [
        (protocol, operation, event, block_time_present)
        for protocol, operation, event in CASES
        for block_time_present in (True, False)
        if block_time_present or "timestamp" in _event_fields(protocol, event)
    ]


@pytest.mark.parametrize(("protocol", "operation", "event", "block_time_present"), _dating_cases())
def test_ignored_control_alone_is_dated_by_its_cpi_without_state(
    protocol: str, operation: str, event: str, block_time_present: bool,
) -> None:
    tx = _fixture(protocol, operation, event)
    if not block_time_present:
        del tx["blockTime"]
    unchanged = copy.deepcopy(tx)
    decoder_cls, _, _, consumed = PROTOCOLS[protocol]
    for selection in (consumed, frozenset()):
        decoded = decoder_cls(event_names=selection).decode(tx)
        assert decoded.events == []
        assert decoded.block_time == datetime.fromtimestamp(TIMESTAMP, tz=timezone.utc)
    assert tx == unchanged


@pytest.mark.parametrize(("protocol", "operation", "event"), CASES)
@pytest.mark.parametrize("failure", ["missing", "extra", "short", "trailing", "foreign_cpi", "wrong_event"])
def test_ignored_control_still_requires_exactly_its_complete_cpi(
    protocol: str, operation: str, event: str, failure: str,
) -> None:
    tx = _fixture(protocol, operation, event)
    inner = tx["meta"]["innerInstructions"][0]["instructions"]
    cpi = inner[2]
    if failure == "missing":
        inner.pop()
    elif failure == "extra":
        inner.append(copy.deepcopy(cpi))
    elif failure in {"short", "trailing"}:
        raw = bytearray(_base58_decode(cpi["data"]))
        if failure == "short":
            raw.pop()
        else:
            raw.append(0)
        cpi["data"] = _base58_encode(bytes(raw))
    elif failure == "foreign_cpi":
        cpi["programId"] = OTHER
    else:
        other_operation, other_event = next(
            (op, ev) for proto, op, ev in CASES if proto == protocol and ev != event
        )
        cpi["data"] = _fixture(protocol, other_operation, other_event)["meta"]["innerInstructions"][0][
            "instructions"][2]["data"]
    decoder_cls, _, _, consumed = PROTOCOLS[protocol]
    for selection in (consumed, frozenset()):
        with pytest.raises(AnchorDecodeError):
            decoder_cls(event_names=selection).decode(tx)
