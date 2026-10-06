from __future__ import annotations

import copy
from typing import Any

from scripts.mainnet_decode_probe import (
    MAX_KEPT_FAILURES,
    MAX_KEPT_PER_SHAPE,
    decode_report,
    mentions_subscribed_program,
    synthetic_truncation,
)
from sniper_bot.events import EVENT_ID_V2_CUTOVER_SLOT
from sniper_bot.protocols.anchor import _base58_encode
from sniper_bot.protocols.pump import PUMP_PROGRAM_ID, PumpDecoder

OTHER = "11111111111111111111111111111111"
CLAIM_CASHBACK_V2 = next(
    bytes(item["discriminator"])
    for item in PumpDecoder()._anchor.idl["instructions"]
    if item["name"] == "claim_cashback_v2"
)
UNKNOWN = bytes([255]) * 8


def _transaction(selector: bytes, *, err: Any = None, truncated: bool = True, mention: bool = True) -> dict[str, Any]:
    logs = ["Log truncated"] if truncated else [
        f"Program {PUMP_PROGRAM_ID} invoke [2]", f"Program {PUMP_PROGRAM_ID} success",
    ]
    return {
        "slot": EVENT_ID_V2_CUTOVER_SLOT + 1, "blockTime": 1_776_700_123,
        "transaction": {"signatures": [_base58_encode(bytes([4]) * 64)], "message": {
            "accountKeys": [{"pubkey": PUMP_PROGRAM_ID, "source": "transaction"}] if mention else [],
            "instructions": [{"programId": OTHER}],
        }},
        "meta": {"err": err, "logMessages": logs, "innerInstructions": [{"index": 0, "instructions": [
            {"programId": PUMP_PROGRAM_ID, "stackHeight": 2, "data": _base58_encode(selector)},
        ]}]},
    }


def test_report_decodes_only_delivered_transactions_like_the_pipeline() -> None:
    good = _transaction(CLAIM_CASHBACK_V2)
    unknown = _transaction(UNKNOWN)
    failed = _transaction(UNKNOWN, err={"InstructionError": [0, {"Custom": 1}]})
    unmentioned = _transaction(UNKNOWN, mention=False)
    report = decode_report([good, unknown, copy.deepcopy(unknown), failed, unmentioned])
    assert report["counts"] == {
        "delivered": 3, "truncated": 3, "real_decoded_pump": 1, "real_failures": 2, "failures": 2,
    }
    assert report["failure_shapes"] == {
        "real pump: unverified own operation in truncated transaction | ['?ffffffffffffffff']": 2,
    }
    assert [item["transaction"] for item in report["failures"]] == [unknown, unknown]


def test_lookup_loaded_mentions_are_delivered() -> None:
    tx = _transaction(UNKNOWN, mention=False)
    assert not mentions_subscribed_program(tx)
    tx["meta"]["loadedAddresses"] = {"writable": [], "readonly": [PUMP_PROGRAM_ID]}
    assert mentions_subscribed_program(tx)


def test_complete_transactions_are_also_checked_as_if_truncated() -> None:
    complete = _transaction(UNKNOWN, truncated=False)
    synthetic = synthetic_truncation(complete)
    assert synthetic is not None and synthetic["meta"]["logMessages"] == [complete["meta"]["logMessages"][0], "Log truncated"]
    assert complete["meta"]["logMessages"][-1] != "Log truncated"
    assert synthetic_truncation(_transaction(UNKNOWN)) is None
    report = decode_report([complete])
    assert report["counts"] == {
        "delivered": 1, "truncated": 0, "real_decoded_pump": 1, "synthetic_failures": 1, "failures": 1,
    }
    assert list(report["failure_shapes"]) == [
        "synthetic pump: unverified own operation in truncated transaction | ['?ffffffffffffffff']",
    ]


def test_report_keeps_a_bounded_sample_of_every_failure_shape() -> None:
    common = [_transaction(UNKNOWN)] * (MAX_KEPT_FAILURES + 5)
    rare = _transaction(bytes([254]) * 8)
    report = decode_report([*common, rare])
    assert report["counts"]["failures"] == MAX_KEPT_FAILURES + 6
    shapes = [item["shape"] for item in report["failures"]]
    assert len(shapes) == MAX_KEPT_PER_SHAPE + 1 and shapes[-1].endswith("['?fefefefefefefefe']")
