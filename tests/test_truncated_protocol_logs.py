from __future__ import annotations

import asyncio
import base64
import copy
import json
import struct
from pathlib import Path
from typing import Any

import pytest

from sniper_bot.events import EventSource, Protocol
from sniper_bot.metrics import BotMetrics
from sniper_bot.pipeline import ConfirmationPipeline
from sniper_bot.protocols import AnchorDecodeError
from sniper_bot.protocols.anchor import _base58_encode
from sniper_bot.protocols.pump import PUMP_PROGRAM_ID, PumpDecoder
from sniper_bot.protocols.pump.decoder import PUMP_STATE_EVENT_NAMES
from sniper_bot.protocols.pumpswap import PUMPSWAP_PROGRAM_ID, PumpSwapDecoder
from sniper_bot.solana_rpc import SolanaRpcClient
from sniper_bot.stream import EntryGate, HeliusStreamGateway, _transaction_protocols

TAG = bytes.fromhex("e445a52e51cb9a1d")
OTHER = "11111111111111111111111111111111"


def _fixture() -> dict[str, Any]:
    return json.loads(
        (Path(__file__).parent / "fixtures/pumpswap_create_pool_reversed.json")
        .read_text(encoding="utf8")
    )


def _instruction(name: str) -> dict[str, Any]:
    anchor = PumpSwapDecoder()._anchor
    definition = next(item for item in anchor.idl["instructions"] if item["name"] == name)
    return {"programId": PUMPSWAP_PROGRAM_ID, "data": _base58_encode(bytes(definition["discriminator"]))}


def _cpi(payload: bytes, *, program_id: str = PUMPSWAP_PROGRAM_ID) -> dict[str, Any]:
    return {"programId": program_id, "data": _base58_encode(TAG + payload), "stackHeight": 2}


def _truncated() -> dict[str, Any]:
    tx = copy.deepcopy(_fixture())
    # Keep every real event and its original index, then emulate small logs
    # admitted after the runtime stopped recording invocation boundaries.
    tx["meta"]["err"] = None
    tx["meta"]["logMessages"].extend(["Log truncated", "Program data: YWJj"])
    payload = base64.b64decode(PumpSwapDecoder()._anchor._own_event_lines(tx["meta"]["logMessages"])[0][1])
    tx["transaction"]["message"] = {
        "accountKeys": [{"pubkey": PUMPSWAP_PROGRAM_ID, "source": "lookupTable"}],
        "instructions": [_instruction("create_pool")],
    }
    tx["meta"]["innerInstructions"] = [{"index": 0, "instructions": [_cpi(payload)]}]
    return tx


def _gateway() -> HeliusStreamGateway:
    async def handler(*_args: Any) -> None:
        return None

    metrics = BotMetrics()
    return HeliusStreamGateway(
        websocket_url="wss://example.invalid",
        rpc=SolanaRpcClient("https://example.invalid"),
        handler=handler,
        entry_gate=EntryGate(metrics),
        metrics=metrics,
    )


def _notification(tx: dict[str, Any]) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "method": "logsNotification",
        "params": {"result": {
            "context": {"slot": tx["slot"]},
            "value": {"signature": tx["transaction"]["signatures"][0], "err": None,
                      "logs": tx["meta"]["logMessages"]},
        }},
    }


def test_verified_prefix_preserves_events_and_canonical_keys() -> None:
    expected = PumpSwapDecoder().decode(_fixture())
    actual = PumpSwapDecoder().decode(_truncated())
    assert [e.event_id for e in actual.events] == [e.event_id for e in expected.events]
    assert [e.instruction_index for e in actual.events] == [e.instruction_index for e in expected.events]
    assert [e.payload for e in actual.events] == [e.payload for e in expected.events]


def test_repeated_identical_events_keep_multiplicity() -> None:
    tx = _truncated()
    logs = tx["meta"]["logMessages"]
    index = next(i for i, line in enumerate(logs) if line.startswith("Program data: "))
    logs.insert(index + 1, logs[index])
    tx["transaction"]["message"]["instructions"].append(_instruction("create_pool"))
    tx["meta"]["innerInstructions"].append(copy.deepcopy(tx["meta"]["innerInstructions"][0]))
    tx["meta"]["innerInstructions"][1]["index"] = 1
    events = PumpSwapDecoder().decode(tx).events
    assert len(events) == 2
    assert len({e.event_id for e in events}) == 2
    tx["meta"]["innerInstructions"].pop()
    with pytest.raises(AnchorDecodeError, match="cover"):
        PumpSwapDecoder().decode(tx)


@pytest.mark.parametrize("failure", [
    "missing_cpi", "extra_cpi", "foreign_cpi", "short_cpi", "invalid_base58",
    "unknown_cpi", "unknown_operation", "missing_later_operation_cpi",
    "altered_payload", "missing_log", "malformed_group", "duplicate_group",
    "compiled_program_id", "failed_transaction", "missing_err", "missing_instructions",
])
def test_truncation_proof_rejects_incomplete_or_untrusted_metadata(failure: str) -> None:
    tx = _truncated()
    instructions = tx["meta"]["innerInstructions"][0]["instructions"]
    if failure == "missing_cpi":
        instructions.clear()
    elif failure == "extra_cpi":
        instructions.append(copy.deepcopy(instructions[0]))
    elif failure == "foreign_cpi":
        instructions[0]["programId"] = OTHER
    elif failure == "short_cpi":
        instructions[0]["data"] = _base58_encode(TAG + b"x")
    elif failure == "invalid_base58":
        instructions[0]["data"] = "0"
    elif failure == "unknown_cpi":
        instructions[0]["data"] = _base58_encode(TAG + b"\xff" * 8)
    elif failure == "unknown_operation":
        tx["transaction"]["message"]["instructions"][0] = _instruction("extend_account")
    elif failure == "missing_later_operation_cpi":
        # Matching CreatePool logs/CPI alone cannot prove a later Buy was not lost.
        tx["transaction"]["message"]["instructions"].append(_instruction("buy"))
    elif failure == "altered_payload":
        instructions[0]["data"] = _base58_encode(TAG + b"\x01" * 64)
    elif failure == "missing_log":
        tx["meta"]["logMessages"] = [
            line for line in tx["meta"]["logMessages"]
            if not line.startswith("Program data: ") or line == "Program data: YWJj"
        ]
    elif failure == "malformed_group":
        tx["meta"]["innerInstructions"][0]["index"] = True
    elif failure == "duplicate_group":
        tx["meta"]["innerInstructions"].append(copy.deepcopy(tx["meta"]["innerInstructions"][0]))
    elif failure == "compiled_program_id":
        del instructions[0]["programId"]
        instructions[0]["programIdIndex"] = 0
    elif failure == "failed_transaction":
        tx["meta"]["err"] = {"InstructionError": [0, "Custom"]}
    elif failure == "missing_err":
        del tx["meta"]["err"]
    elif failure == "missing_instructions":
        tx["meta"]["innerInstructions"] = None
    with pytest.raises(AnchorDecodeError):
        PumpSwapDecoder().decode(tx)


def test_exact_payload_order_is_required() -> None:
    tx = _truncated()
    logs = tx["meta"]["logMessages"]
    index, encoded = PumpSwapDecoder()._anchor._own_event_lines(logs)[0]
    altered = bytearray(base64.b64decode(encoded))
    altered[-16] ^= 1
    logs.insert(index + 1, "Program data: " + base64.b64encode(altered).decode())
    tx["transaction"]["message"]["instructions"].append(_instruction("create_pool"))
    tx["meta"]["innerInstructions"].append(
        {"index": 1, "instructions": [_cpi(bytes(altered))]}
    )
    assert len(PumpSwapDecoder().decode(tx).events) == 2
    first = tx["meta"]["innerInstructions"][0]["instructions"][0]
    second = tx["meta"]["innerInstructions"][1]["instructions"][0]
    tx["meta"]["innerInstructions"][0]["instructions"][0] = second
    tx["meta"]["innerInstructions"][1]["instructions"][0] = first
    with pytest.raises(AnchorDecodeError, match="missing"):
        PumpSwapDecoder().decode(tx)


def test_unverified_generic_truncation_cannot_be_silently_ignored() -> None:
    with pytest.raises(AnchorDecodeError, match="verified"):
        PumpDecoder()._anchor.scan_logs(["Log truncated"])


@pytest.mark.asyncio
async def test_truncated_fetch_keeps_receive_order_and_source(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = _gateway()
    tx = _truncated()
    first = _notification(tx)
    second = copy.deepcopy(first)
    second["params"]["result"]["value"]["signature"] = "second"
    second["params"]["result"]["value"]["logs"] = ["Program log: complete"]
    started = asyncio.Event()
    release = asyncio.Event()
    queued: list[tuple[dict[str, Any], EventSource, dict[str, Any]]] = []

    async def fetch(_signature: str) -> dict[str, Any]:
        started.set()
        await release.wait()
        return tx

    async def queue(value: dict[str, Any], source: EventSource, **kwargs: Any) -> None:
        queued.append((value, source, kwargs))

    monkeypatch.setattr(gateway.rpc, "get_transaction", fetch)
    monkeypatch.setattr(gateway, "_queue_transaction", queue)
    await gateway.handle_message(first)
    await asyncio.wait_for(started.wait(), timeout=1)
    await gateway.handle_message(second)
    assert queued == []
    received_at = gateway.last_observed_at
    release.set()
    await asyncio.wait_for(gateway._notification_queue.join(), timeout=1)
    assert queued[0][0] == tx
    assert queued[0][0] is not tx
    assert queued[1][0]["signature"] == "second"
    assert queued[0][1] == EventSource.SOLANA_WSS
    assert queued[0][2]["received_at"] <= received_at
    assert queued[0][2]["generation"] == gateway._stream_generation
    assert len(PumpSwapDecoder().decode(queued[0][0]).events) == 1
    await gateway._cancel_notification_dispatcher()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [
    "not_found", "wrong_signature", "wrong_slot", "failed", "missing_err",
    "wrong_prefix", "rpc_failure",
])
async def test_rpc_failure_preserves_original_for_permanent_quarantine(
    monkeypatch: pytest.MonkeyPatch, failure: str, caplog: pytest.LogCaptureFixture
) -> None:
    gateway = _gateway()
    full = _truncated()
    original = {
        "slot": full["slot"],
        "signature": full["transaction"]["signatures"][0],
        "meta": {"err": None, "logMessages": full["meta"]["logMessages"]},
    }
    if failure == "wrong_signature":
        full["transaction"]["signatures"] = ["different"]
    elif failure == "wrong_slot":
        full["slot"] += 1
    elif failure == "failed":
        full["meta"]["err"] = True
    elif failure == "missing_err":
        del full["meta"]["err"]
    elif failure == "wrong_prefix":
        full["meta"]["logMessages"] = ["different"]

    async def fetch(_signature: str) -> dict[str, Any] | None:
        if failure == "rpc_failure":
            raise RuntimeError("private-provider-url")
        return None if failure == "not_found" else full

    monkeypatch.setattr(gateway.rpc, "get_transaction", fetch)
    assert await gateway._recover_truncated_transaction(original) is original
    with pytest.raises(AnchorDecodeError):
        PumpSwapDecoder().decode(original)
    assert "private-provider-url" not in caplog.text
    assert "original evidence retained" in caplog.text


@pytest.mark.asyncio
async def test_cancelled_truncated_fetch_retains_recovery_gap(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = _gateway()
    started = asyncio.Event()

    async def fetch(_signature: str) -> None:
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(gateway.rpc, "get_transaction", fetch)
    await gateway.handle_message(_notification(_truncated()))
    await asyncio.wait_for(started.wait(), timeout=1)
    await gateway._cancel_ingress_dispatch()
    assert gateway._dispatch_recovery_pending is True
    assert "stream_recovery_gap" in gateway.entry_gate.reasons
    assert gateway._notification_dispatch_task is None


@pytest.mark.asyncio
async def test_failure_flows_through_existing_pipeline_quarantine(tmp_path: Path) -> None:
    metrics = BotMetrics()
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path), strategy_version="test-version", config_hash="test-hash",
        entry_gate=EntryGate(metrics), metrics=metrics, record_raw=False,
    )
    tx = _truncated()
    del tx["meta"]["innerInstructions"]
    await pipeline.process_transaction(Protocol.PUMPSWAP, tx, EventSource.BASELINE_WSS)
    assert "protocol:pumpswap" in pipeline.entry_gate.reasons



@pytest.mark.asyncio
async def test_rpc_without_marker_cannot_bypass_completeness_proof(monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = _gateway()
    full = _truncated()
    original = {
        "slot": full["slot"], "signature": full["transaction"]["signatures"][0],
        "meta": {"err": None, "logMessages": list(full["meta"]["logMessages"])},
    }
    full["transaction"]["message"]["instructions"].append(_instruction("create_pool"))
    full["meta"]["innerInstructions"].append({
        "index": 1, "instructions": copy.deepcopy(full["meta"]["innerInstructions"][0]["instructions"]),
    })
    full["meta"]["logMessages"] = full["meta"]["logMessages"][:-2]

    async def fetch(_signature: str) -> dict[str, Any]:
        return full

    monkeypatch.setattr(gateway.rpc, "get_transaction", fetch)
    recovered = await gateway._recover_truncated_transaction(original)
    assert recovered["meta"]["logMessages"] == original["meta"]["logMessages"]
    assert "Log truncated" not in full["meta"]["logMessages"]
    with pytest.raises(AnchorDecodeError, match="missing"):
        PumpSwapDecoder().decode(recovered)


def test_outer_event_tag_cannot_substitute_for_cpi_evidence() -> None:
    tx = _truncated()
    event = tx["meta"]["innerInstructions"][0]["instructions"].pop()
    tx["transaction"]["message"]["instructions"].append(event)
    with pytest.raises(AnchorDecodeError, match="parent"):
        PumpSwapDecoder().decode(tx)


@pytest.mark.parametrize("height", [None, True, 1, 3])
def test_cpi_requires_complete_parent_stack(height: Any) -> None:
    tx = _truncated()
    tx["meta"]["innerInstructions"][0]["instructions"][0]["stackHeight"] = height
    with pytest.raises(AnchorDecodeError):
        PumpSwapDecoder().decode(tx)


def test_two_operations_cannot_borrow_one_parent_cpi() -> None:
    tx = _truncated()
    logs = tx["meta"]["logMessages"]
    index = next(i for i, line in enumerate(logs) if line.startswith("Program data: "))
    logs.insert(index + 1, logs[index])
    tx["transaction"]["message"]["instructions"].append(_instruction("create_pool"))
    first_group = tx["meta"]["innerInstructions"][0]["instructions"]
    first_group.append(copy.deepcopy(first_group[0]))
    with pytest.raises(AnchorDecodeError, match="parent"):
        PumpSwapDecoder().decode(tx)


def test_unconsumed_close_event_body_is_required() -> None:
    tx = _truncated()
    anchor = PumpSwapDecoder()._anchor
    discriminator = next(key for key, name in anchor._events.items()
                         if name == "CloseUserVolumeAccumulatorEvent")
    tx["transaction"]["message"]["instructions"].append(_instruction("close_user_volume_accumulator"))
    tx["meta"]["innerInstructions"].append({"index": 1, "instructions": [_cpi(discriminator)]})
    with pytest.raises(AnchorDecodeError, match="truncated"):
        PumpSwapDecoder().decode(tx)


@pytest.mark.parametrize("where", ["outer", "inner"])
def test_supported_protocol_after_marker_is_still_routed(where: str) -> None:
    tx = _truncated()
    operation = {"programId": PUMP_PROGRAM_ID, "data": "11111111", "stackHeight": 2}
    if where == "outer":
        tx["transaction"]["message"]["instructions"].append(operation)
    else:
        tx["meta"]["innerInstructions"][0]["instructions"].append(operation)
    assert _transaction_protocols(tx) == [Protocol.PUMP, Protocol.PUMPSWAP]
    assert len(PumpSwapDecoder().decode(tx).events) == 1
    with pytest.raises(AnchorDecodeError, match="verified"):
        PumpDecoder().decode(tx)


def test_truncation_without_program_identity_cannot_drop_evidence() -> None:
    assert _transaction_protocols({"meta": {"logMessages": ["Log truncated"]}}) == [
        Protocol.PUMP, Protocol.PUMPSWAP,
    ]


def test_valid_unconsumed_close_event_does_not_change_selected_prefix() -> None:
    tx = _truncated()
    anchor = PumpSwapDecoder()._anchor
    discriminator = next(key for key, name in anchor._events.items()
                         if name == "CloseUserVolumeAccumulatorEvent")
    tx["transaction"]["message"]["instructions"].append(_instruction("close_user_volume_accumulator"))
    tx["meta"]["innerInstructions"].append({
        "index": 1, "instructions": [_cpi(discriminator + bytes(72))],
    })
    assert [e.event_id for e in PumpSwapDecoder().decode(tx).events] == [
        e.event_id for e in PumpSwapDecoder().decode(_fixture()).events
    ]


def _pump_trade(
    name: str = "buy_exact_quote_in_v2", *, layout: str = "current",
    is_buy: bool = True, reserves: int = 10,
) -> dict[str, Any]:
    anchor = PumpDecoder()._anchor
    definition = next(item for item in anchor.idl["instructions"] if item["name"] == name)
    discriminator = next(key for key, event in anchor._events.items() if event == "TradeEvent")
    chunks = [discriminator]
    for field in anchor._types["TradeEvent"]["fields"]:
        kind = field["type"]
        if kind == "pubkey":
            value = bytes([2]) * 32
        elif kind == "u64":
            number = reserves if field["name"] == "real_token_reserves" else 1
            value = struct.pack("<Q", number)
        elif kind == "i64":
            value = struct.pack("<q", 1_776_700_123)
        elif kind == "bool":
            value = bytes([is_buy if field["name"] == "is_buy" else 0])
        elif kind == "string":
            value = struct.pack("<I", len(name)) + name.encode()
        elif isinstance(kind, dict) and "vec" in kind:
            value = struct.pack("<I", 0)
        else:
            raise AssertionError("unexpected fixture field")
        chunks.append(value)
        if layout == "minimum" and field["name"] == "real_quote_reserves":
            break
    payload = b"".join(chunks)
    if layout == "appended":
        payload += bytes(16)
    return {
        "slot": 5, "blockTime": 1_776_700_123,
        "transaction": {"signatures": ["pump-trade"], "message": {"instructions": [{
            "programId": PUMP_PROGRAM_ID, "data": _base58_encode(bytes(definition["discriminator"])),
        }]}},
        "meta": {"err": None, "logMessages": [
            f"Program {PUMP_PROGRAM_ID} invoke [1]",
            "Program data: " + base64.b64encode(payload).decode(),
            f"Program {PUMP_PROGRAM_ID} success", "Log truncated", "Program data: YWJj",
        ], "innerInstructions": [{"index": 0, "instructions": [_cpi(payload, program_id=PUMP_PROGRAM_ID)]}]},
    }


@pytest.mark.parametrize("name", [
    "buy", "buy_exact_sol_in", "buy_v2", "buy_exact_quote_in_v2", "sell", "sell_v2",
])
@pytest.mark.parametrize("layout", ["minimum", "current", "appended"])
def test_verified_pump_trade_preserves_ids_payloads_and_state_filter(name: str, layout: str) -> None:
    tx = _pump_trade(name, layout=layout, is_buy=not name.startswith("sell"))
    complete = copy.deepcopy(tx)
    complete["meta"]["logMessages"] = complete["meta"]["logMessages"][:3]
    expected = PumpDecoder().decode(complete)
    actual = PumpDecoder().decode(tx)
    assert len(actual.events) == 1
    assert [e.event_id for e in actual.events] == [e.event_id for e in expected.events]
    assert actual.events[0].payload == expected.events[0].payload
    ignored = PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES).decode(tx)
    assert ignored.events == []
    assert ignored.block_time == expected.block_time


@pytest.mark.parametrize("failure", [
    "missing_cpi", "missing_log", "short_body", "partial_append", "unknown_operation",
    "completion_cpi", "completion_without_cpi", "wrong_timestamp",
])
def test_pump_trade_proof_rejects_incomplete_evidence_and_completion(failure: str) -> None:
    tx = _pump_trade(reserves=0 if failure == "completion_without_cpi" else 10,
                     layout="minimum" if failure == "partial_append" else "current")
    inner = tx["meta"]["innerInstructions"][0]["instructions"]
    if failure == "missing_cpi":
        inner.clear()
    elif failure == "missing_log":
        tx["meta"]["logMessages"].pop(1)
    elif failure in ("short_body", "partial_append"):
        payload = base64.b64decode(tx["meta"]["logMessages"][1].removeprefix("Program data: "))
        payload = payload[:100] if failure == "short_body" else payload + b"x"
        inner[0] = _cpi(payload, program_id=PUMP_PROGRAM_ID)
        tx["meta"]["logMessages"][1] = "Program data: " + base64.b64encode(payload).decode()
    elif failure == "unknown_operation":
        tx["transaction"]["message"]["instructions"][0]["data"] = "11111111"
    elif failure == "completion_cpi":
        # An extra CompleteEvent after the marker must not be dropped even
        # when TradeEvent is not selected by live state.
        anchor = PumpDecoder()._anchor
        discriminator = next(key for key, event in anchor._events.items() if event == "CompleteEvent")
        inner.append(_cpi(discriminator + bytes(136), program_id=PUMP_PROGRAM_ID))
    elif failure == "wrong_timestamp":
        tx["blockTime"] += 120
    for decoder in (PumpDecoder(), PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES)):
        with pytest.raises(AnchorDecodeError):
            decoder.decode(tx)


def test_nested_pump_trade_is_verified_in_its_own_parent() -> None:
    tx = _pump_trade()
    own = tx["transaction"]["message"]["instructions"][0]
    tx["transaction"]["message"]["instructions"] = [{"programId": OTHER}]
    own["stackHeight"] = 2
    cpi = tx["meta"]["innerInstructions"][0]["instructions"][0]
    cpi["stackHeight"] = 3
    tx["meta"]["innerInstructions"][0]["instructions"] = [own, cpi]
    assert len(PumpDecoder().decode(tx).events) == 1


def test_truncated_pump_sell_at_zero_token_reserves_is_not_a_buy_completion() -> None:
    assert len(PumpDecoder().decode(_pump_trade("sell_v2", is_buy=False, reserves=0)).events) == 1
