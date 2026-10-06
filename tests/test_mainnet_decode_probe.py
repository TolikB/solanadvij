from __future__ import annotations

import copy
from typing import Any

from scripts.mainnet_decode_probe import MAX_KEPT_FAILURES, decode_report
from sniper_bot.events import EVENT_ID_V2_CUTOVER_SLOT
from sniper_bot.protocols.anchor import _base58_encode
from sniper_bot.protocols.pump import PUMP_PROGRAM_ID, PumpDecoder

OTHER = "11111111111111111111111111111111"
CLAIM_CASHBACK_V2 = next(
    bytes(item["discriminator"])
    for item in PumpDecoder()._anchor.idl["instructions"]
    if item["name"] == "claim_cashback_v2"
)


def _truncated(selector: bytes, *, err: Any = None) -> dict[str, Any]:
    return {
        "slot": EVENT_ID_V2_CUTOVER_SLOT + 1, "blockTime": 1_776_700_123,
        "transaction": {"signatures": [_base58_encode(bytes([4]) * 64)], "message": {
            "instructions": [{"programId": OTHER}],
        }},
        "meta": {"err": err, "logMessages": ["Log truncated"], "innerInstructions": [{"index": 0, "instructions": [
            {"programId": PUMP_PROGRAM_ID, "stackHeight": 2, "data": _base58_encode(selector)},
        ]}]},
    }


def test_report_decodes_like_the_pipeline_and_groups_failures() -> None:
    good = _truncated(CLAIM_CASHBACK_V2)
    unknown = _truncated(bytes([255]) * 8)
    failed = _truncated(bytes([255]) * 8, err={"InstructionError": [0, {"Custom": 1}]})
    unrelated = {"slot": 1, "transaction": {"message": {"instructions": []}}, "meta": {"err": None, "logMessages": []}}
    report = decode_report([good, unknown, copy.deepcopy(unknown), failed, unrelated])
    assert report["counts"] == {"routed": 3, "truncated": 3, "decoded_pump": 1, "failures": 2}
    [(shape, count)] = report["failure_shapes"].items()
    assert count == 2
    assert shape == "pump: unverified own operation in truncated transaction | ['?ffffffffffffffff']"
    assert [item["transaction"] for item in report["failures"]] == [unknown, unknown]


def test_report_keeps_a_bounded_sample_of_failing_transactions() -> None:
    report = decode_report([_truncated(bytes([255]) * 8)] * (MAX_KEPT_FAILURES + 5))
    assert report["counts"]["failures"] == MAX_KEPT_FAILURES + 5
    assert len(report["failures"]) == MAX_KEPT_FAILURES
