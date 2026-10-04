from __future__ import annotations

import asyncio
import base64
import copy
import json
import struct
from pathlib import Path
from typing import Any

import pytest

from sniper_bot.events import EVENT_ID_V2_CUTOVER_SLOT, EVENT_IDENTITY_KEY, EventSource, Protocol
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
        "index": 1, "instructions": [_cpi(discriminator + bytes(32) + struct.pack("<q", tx["blockTime"]) + bytes(32))],
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
    "missing_cpi", "short_body", "partial_append", "unknown_operation",
    "completion_cpi", "completion_without_cpi", "wrong_timestamp",
])
def test_pump_trade_proof_rejects_incomplete_evidence_and_completion(failure: str) -> None:
    tx = _pump_trade(reserves=0 if failure == "completion_without_cpi" else 10,
                     layout="minimum" if failure == "partial_append" else "current")
    inner = tx["meta"]["innerInstructions"][0]["instructions"]
    if failure == "missing_cpi":
        inner.clear()
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


@pytest.mark.parametrize("block_time_available", [True, False])
def test_ignored_trade_missing_from_logs_uses_verified_cpi_clock_without_event_id(
    block_time_available: bool,
) -> None:
    tx = _pump_trade()
    tx["meta"]["logMessages"].pop(1)
    if not block_time_available:
        del tx["blockTime"]
    ignored = PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES).decode(tx)
    assert ignored.events == []
    assert ignored.block_time is not None
    assert int(ignored.block_time.timestamp()) == 1_776_700_123
    with pytest.raises(AnchorDecodeError, match="consumed events are missing"):
        PumpDecoder().decode(tx)


@pytest.mark.parametrize("failure", ["short_body", "partial_append", "completion", "wrong_clock"])
def test_missing_ignored_trade_still_requires_complete_safe_cpi(failure: str) -> None:
    tx = _pump_trade(reserves=0 if failure == "completion" else 10,
                     layout="minimum" if failure == "partial_append" else "current")
    payload = base64.b64decode(tx["meta"]["logMessages"].pop(1).removeprefix("Program data: "))
    if failure == "short_body":
        payload = payload[:100]
    elif failure == "partial_append":
        payload += b"x"
    elif failure == "wrong_clock":
        tx["blockTime"] += 120
    tx["meta"]["innerInstructions"][0]["instructions"][0] = _cpi(payload, program_id=PUMP_PROGRAM_ID)
    with pytest.raises(AnchorDecodeError):
        PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES).decode(tx)


def test_unlogged_ignored_trade_cannot_hide_missing_consumed_state_event() -> None:
    tx = _pump_trade()
    tx["meta"]["logMessages"].pop(1)
    anchor = PumpDecoder()._anchor
    discriminator = next(key for key, name in anchor._events.items() if name == "CompleteEvent")
    tx["meta"]["innerInstructions"][0]["instructions"].append(
        _cpi(discriminator + bytes(136), program_id=PUMP_PROGRAM_ID)
    )
    with pytest.raises(AnchorDecodeError, match="parent operation"):
        PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES).decode(tx)


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


@pytest.mark.parametrize("prefix_available", [True, False])
def test_v2_missing_events_use_same_id_as_full_logs_and_true_cpi_coordinates(prefix_available: bool) -> None:
    tx = _v2(_truncated())
    complete = _without_marker(tx)
    if not prefix_available:
        tx["meta"]["logMessages"] = ["Log truncated", "Program data: YWJj"]
    expected = PumpSwapDecoder().decode(complete).events
    recovered = PumpSwapDecoder().decode(tx, source=EventSource.RPC_RECOVERY).events
    assert [event.event_id for event in recovered] == [event.event_id for event in expected]
    assert len(recovered) == 1
    assert recovered[0].payload[EVENT_IDENTITY_KEY]["origin"] == ("log" if prefix_available else "cpi")
    assert recovered[0].instruction_index == (expected[0].instruction_index if prefix_available else 0)
    assert recovered[0].inner_instruction_index == (-1 if prefix_available else 0)
    assert {k: v for k, v in recovered[0].payload.items() if k != EVENT_IDENTITY_KEY} == {
        k: v for k, v in expected[0].payload.items() if k != EVENT_IDENTITY_KEY
    }


def test_v2_repeated_identical_cpi_events_preserve_multiplicity_and_execution_order() -> None:
    tx = _v2(_truncated())
    tx["transaction"]["message"]["instructions"].append(_instruction("create_pool"))
    tx["meta"]["innerInstructions"].append({
        "index": 1, "instructions": copy.deepcopy(tx["meta"]["innerInstructions"][0]["instructions"]),
    })
    complete = _without_marker(tx)
    index, encoded = PumpSwapDecoder()._anchor._own_event_lines(complete["meta"]["logMessages"])[0]
    complete["meta"]["logMessages"].insert(index + 1, "Program data: " + encoded)
    expected = PumpSwapDecoder().decode(complete).events
    recovered = PumpSwapDecoder().decode(tx).events
    assert [event.event_id for event in recovered] == [event.event_id for event in expected]
    assert len({event.event_id for event in recovered}) == 2
    assert [event.payload[EVENT_IDENTITY_KEY]["ordinal"] for event in recovered] == [0, 1]
    assert [event.payload[EVENT_IDENTITY_KEY]["origin"] for event in recovered] == ["log", "cpi"]
    assert recovered[1].instruction_index == 1 and recovered[1].inner_instruction_index == 0


def test_v2_available_log_subsequence_is_not_a_prefix() -> None:
    tx = _v2(_truncated())
    index, encoded = PumpSwapDecoder()._anchor._own_event_lines(tx["meta"]["logMessages"])[0]
    first = base64.b64decode(encoded)
    second = bytearray(first)
    second[-16] ^= 1
    tx["transaction"]["message"]["instructions"].append(_instruction("create_pool"))
    tx["meta"]["innerInstructions"].append({"index": 1, "instructions": [_cpi(bytes(second))]})
    # Only the second event is logged, so this is an interior gap, not a suffix.
    tx["meta"]["logMessages"][index] = "Program data: " + base64.b64encode(second).decode()
    with pytest.raises(AnchorDecodeError, match="verified log prefix"):
        PumpSwapDecoder().decode(tx)


@pytest.mark.parametrize("failure", [
    "missing_cpi", "extra_cpi", "foreign_cpi", "short_cpi", "short_body", "unknown_cpi",
    "unknown_operation", "missing_later_cpi", "malformed_group", "duplicate_group",
    "compiled_program_id", "failed_transaction", "missing_err", "missing_instructions", "duplicate_marker",
])
def test_v2_recovered_suffix_still_requires_full_cpi_proof(failure: str) -> None:
    tx = _v2(_truncated())
    payload = base64.b64decode(PumpSwapDecoder()._anchor._own_event_lines(tx["meta"]["logMessages"])[0][1])
    tx["meta"]["logMessages"] = ["Log truncated"]
    inner = tx["meta"]["innerInstructions"][0]["instructions"]
    if failure == "missing_cpi":
        inner.clear()
    elif failure == "extra_cpi":
        inner.append(copy.deepcopy(inner[0]))
    elif failure == "foreign_cpi":
        inner[0]["programId"] = OTHER
    elif failure == "short_cpi":
        inner[0]["data"] = _base58_encode(TAG + b"x")
    elif failure == "short_body":
        inner[0]["data"] = _base58_encode(TAG + payload[:40])
    elif failure == "unknown_cpi":
        inner[0]["data"] = _base58_encode(TAG + bytes([255]) * 8)
    elif failure == "unknown_operation":
        tx["transaction"]["message"]["instructions"][0] = _instruction("extend_account")
    elif failure == "missing_later_cpi":
        tx["transaction"]["message"]["instructions"].append(_instruction("buy"))
    elif failure == "malformed_group":
        tx["meta"]["innerInstructions"][0]["index"] = True
    elif failure == "duplicate_group":
        tx["meta"]["innerInstructions"].append(copy.deepcopy(tx["meta"]["innerInstructions"][0]))
    elif failure == "compiled_program_id":
        del inner[0]["programId"]
        inner[0]["programIdIndex"] = 0
    elif failure == "failed_transaction":
        tx["meta"]["err"] = True
    elif failure == "missing_err":
        del tx["meta"]["err"]
    elif failure == "missing_instructions":
        tx["meta"]["innerInstructions"] = None
    elif failure == "duplicate_marker":
        tx["meta"]["logMessages"].append("Log truncated")
    with pytest.raises(AnchorDecodeError):
        PumpSwapDecoder().decode(tx)


def test_v2_recovered_trade_clock_is_validated_without_any_log_clock() -> None:
    tx = _v2(_pump_trade())
    tx["meta"]["logMessages"] = ["Log truncated"]
    tx["blockTime"] += 120
    with pytest.raises(AnchorDecodeError, match="disagree"):
        PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES).decode(tx)


@pytest.mark.parametrize("future", [True, False])
def test_pump_create_extend_trade_contract_validates_ignored_extend(future: bool) -> None:
    from scripts.benchmark_postgres_capacity import BorshEventEncoder
    anchor = PumpDecoder()._anchor
    encoder = BorshEventEncoder(Path(__file__).parents[1] / "src/sniper_bot/protocols/pump/idl.json")
    tx = _pump_trade()
    timestamp = tx["blockTime"]
    create = base64.b64decode(encoder.encode("CreateEvent", {"timestamp": timestamp}))
    extend = base64.b64decode(encoder.encode("ExtendAccountEvent", {"timestamp": timestamp}))
    def operation(name: str) -> dict[str, Any]:
        definition = next(item for item in anchor.idl["instructions"] if item["name"] == name)
        return {"programId": PUMP_PROGRAM_ID, "data": _base58_encode(bytes(definition["discriminator"]))}
    trade = tx["transaction"]["message"]["instructions"][0]
    tx["transaction"]["message"]["instructions"] = [operation("create_v2"), operation("extend_account"), trade]
    tx["meta"]["innerInstructions"] = [
        {"index": 0, "instructions": [_cpi(create, program_id=PUMP_PROGRAM_ID)]},
        {"index": 1, "instructions": [_cpi(extend, program_id=PUMP_PROGRAM_ID)]},
        {"index": 2, "instructions": tx["meta"]["innerInstructions"][0]["instructions"]},
    ]
    tx["meta"]["logMessages"] = [
        f"Program {PUMP_PROGRAM_ID} invoke [1]",
        "Program data: " + base64.b64encode(create).decode(),
        "Program data: " + base64.b64encode(extend).decode(),
        "Log truncated",
    ]
    if future:
        tx = _v2(tx)
    state = PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES).decode(tx).events
    assert [event.event_type.value for event in state] == ["token_created"]
    if future:
        all_events = PumpDecoder().decode(tx).events
        assert all_events[0].event_id == state[0].event_id
        assert all_events[1].payload[EVENT_IDENTITY_KEY]["origin"] == "cpi"
        assert all_events[1].instruction_index == 2 and all_events[1].inner_instruction_index == 0
    bad = copy.deepcopy(tx)
    bad["meta"]["innerInstructions"][1]["instructions"][0] = _cpi(extend[:40], program_id=PUMP_PROGRAM_ID)
    with pytest.raises(AnchorDecodeError):
        PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES).decode(bad)


@pytest.mark.asyncio
async def test_v2_stream_rpc_recovery_shares_decoder_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    tx = _v2(_truncated())
    expected = PumpSwapDecoder().decode(_without_marker(tx)).events
    tx["meta"]["logMessages"] = ["Log truncated"]
    gateway = _gateway()
    async def fetch(_signature: str) -> dict[str, Any]:
        return tx
    monkeypatch.setattr(gateway.rpc, "get_transaction", fetch)
    notification = {"slot": tx["slot"], "signature": tx["transaction"]["signatures"][0],
                    "meta": {"err": None, "logMessages": ["Log truncated"]}}
    recovered = await gateway._recover_truncated_transaction(notification)
    assert [event.event_id for event in PumpSwapDecoder().decode(recovered).events] == [event.event_id for event in expected]



def _pump_migration() -> dict[str, Any]:
    """Emulate the observed migration with its nested PumpSwap create-pool CPI."""
    from scripts.benchmark_postgres_capacity import BorshEventEncoder

    swap = _truncated()
    timestamp = swap["blockTime"]
    encoder = BorshEventEncoder(Path(__file__).parents[1] / "src/sniper_bot/protocols/pump/idl.json")
    pool = PumpSwapDecoder().decode(swap).events[0]
    payload = base64.b64decode(encoder.encode("CompletePumpAmmMigrationEvent", {
        "timestamp": timestamp, "pool": pool.pool_address, "mint": pool.mint,
        "mint_amount": 100, "sol_amount": 200, "pool_migration_fee": 3,
    }))
    assert len(payload) == 200
    anchor = PumpDecoder()._anchor
    definition = next(item for item in anchor.idl["instructions"] if item["name"] == "migrate_v2")
    foreign = {"programId": OTHER}
    nested = {**_instruction("create_pool"), "stackHeight": 2}
    swap_cpi = {**swap["meta"]["innerInstructions"][0]["instructions"][0], "stackHeight": 3}
    swap_log = PumpSwapDecoder()._anchor._own_event_lines(swap["meta"]["logMessages"])[0][1]
    tx = {
        "slot": 5, "blockTime": timestamp,
        "transaction": {"signatures": ["pump-migration"], "message": {"instructions": [
            foreign, copy.deepcopy(foreign),
            {"programId": PUMP_PROGRAM_ID, "data": _base58_encode(bytes(definition["discriminator"]))},
        ]}},
        "meta": {"err": None, "logMessages": [
            f"Program {PUMP_PROGRAM_ID} invoke [1]", f"Program {PUMPSWAP_PROGRAM_ID} invoke [2]",
            "Program data: " + swap_log, f"Program {PUMPSWAP_PROGRAM_ID} success",
            "Program data: " + base64.b64encode(payload).decode(), f"Program {PUMP_PROGRAM_ID} success",
            "Log truncated",
        ], "innerInstructions": [{"index": 2, "instructions": [
            nested, swap_cpi, _cpi(payload, program_id=PUMP_PROGRAM_ID),
        ]}]},
    }
    return tx


@pytest.mark.parametrize("prefix_available", [True, False])
def test_v2_migration_and_nested_pool_keep_complete_log_ids_and_true_coordinates(prefix_available: bool) -> None:
    tx = _v2(_pump_migration())
    complete = _without_marker(tx)
    if not prefix_available:
        tx["meta"]["logMessages"] = ["Log truncated"]
    for decoder, event_type, inner_index in [
        (PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES), "migration", 2),
        (PumpSwapDecoder(), "pool_created", 1),
    ]:
        expected = decoder.decode(complete).events
        recovered = decoder.decode(tx, source=EventSource.RPC_RECOVERY).events
        assert len(recovered) == len(expected) == 1
        event = recovered[0]
        assert event.event_type.value == event_type
        assert event.event_id == expected[0].event_id
        assert {k: v for k, v in event.payload.items() if k != EVENT_IDENTITY_KEY} == {
            k: v for k, v in expected[0].payload.items() if k != EVENT_IDENTITY_KEY
        }
        assert event.payload[EVENT_IDENTITY_KEY] == {
            "version": 2, "ordinal": 0, "origin": "log" if prefix_available else "cpi",
        }
        assert event.instruction_index == (expected[0].instruction_index if prefix_available else 2)
        assert event.inner_instruction_index == (-1 if prefix_available else inner_index)
    assert PumpDecoder().decode(tx).events[0].event_id == PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES).decode(tx).events[0].event_id


def test_legacy_migration_preserves_existing_log_identity_and_rejects_missing_suffix() -> None:
    tx = _pump_migration()
    decoder = PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES)
    expected = decoder.decode(_without_marker(tx)).events
    actual = decoder.decode(tx).events
    assert [event.model_dump(mode="json", exclude={"observed_at"}) for event in actual] == [
        event.model_dump(mode="json", exclude={"observed_at"}) for event in expected
    ]
    tx = _v2(tx)
    tx["slot"] = EVENT_ID_V2_CUTOVER_SLOT
    tx["meta"]["logMessages"] = ["Log truncated"]
    with pytest.raises(AnchorDecodeError, match="consumed events are missing"):
        decoder.decode(tx)


@pytest.mark.parametrize("failure", [
    "missing_migration_cpi", "extra_migration_cpi", "short_body", "wrong_clock",
    "wrong_event", "wrong_parent", "unknown_late_operation", "legacy_migrate", "failed_transaction",
])
def test_v2_migration_contract_rejects_incomplete_or_unreviewed_evidence(failure: str) -> None:
    tx = _v2(_pump_migration())
    tx["meta"]["logMessages"] = ["Log truncated"]
    inner = tx["meta"]["innerInstructions"][0]["instructions"]
    if failure == "missing_migration_cpi":
        inner.pop()
    elif failure == "extra_migration_cpi":
        inner.append(copy.deepcopy(inner[-1]))
    elif failure == "short_body":
        from sniper_bot.protocols.anchor import _base58_decode
        inner[-1]["data"] = _base58_encode(_base58_decode(inner[-1]["data"])[:-1])
    elif failure == "wrong_clock":
        tx["blockTime"] += 120
    elif failure == "wrong_event":
        inner[-1] = copy.deepcopy(_pump_trade()["meta"]["innerInstructions"][0]["instructions"][0])
    elif failure == "wrong_parent":
        inner[-1]["stackHeight"] = 3
    elif failure == "unknown_late_operation":
        tx["transaction"]["message"]["instructions"].append({
            "programId": PUMP_PROGRAM_ID, "data": _base58_encode(bytes([255]) * 8),
        })
    elif failure == "legacy_migrate":
        definition = next(item for item in PumpDecoder()._anchor.idl["instructions"] if item["name"] == "migrate")
        tx["transaction"]["message"]["instructions"][2]["data"] = _base58_encode(bytes(definition["discriminator"]))
    elif failure == "failed_transaction":
        tx["meta"]["err"] = True
    with pytest.raises(AnchorDecodeError):
        PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES).decode(tx)



def _pump_trade_close() -> dict[str, Any]:
    """A foreign router calls Pump buy and then closes its volume account."""
    from scripts.benchmark_postgres_capacity import BorshEventEncoder

    tx = _pump_trade()
    trade_log = PumpDecoder()._anchor._own_event_lines(tx["meta"]["logMessages"])[0][1]
    encoder = BorshEventEncoder(Path(__file__).parents[1] / "src/sniper_bot/protocols/pump/idl.json")
    close = base64.b64decode(encoder.encode("CloseUserVolumeAccumulatorEvent", {"timestamp": tx["blockTime"]}))
    assert len(close) == 80
    anchor = PumpDecoder()._anchor
    definition = next(item for item in anchor.idl["instructions"] if item["name"] == "close_user_volume_accumulator")
    trade = {**tx["transaction"]["message"]["instructions"][0], "stackHeight": 2}
    trade_cpi = {**tx["meta"]["innerInstructions"][0]["instructions"][0], "stackHeight": 3}
    close_ix = {"programId": PUMP_PROGRAM_ID, "data": _base58_encode(bytes(definition["discriminator"])), "stackHeight": 2}
    tx["transaction"]["message"]["instructions"] = [{"programId": OTHER}] * 3
    tx["meta"]["innerInstructions"] = [{"index": 2, "instructions": [
        trade, trade_cpi, close_ix, {**_cpi(close, program_id=PUMP_PROGRAM_ID), "stackHeight": 3},
    ]}]
    tx["meta"]["logMessages"] = [
        f"Program {OTHER} invoke [1]", f"Program {PUMP_PROGRAM_ID} invoke [2]",
        "Program data: " + trade_log, f"Program {PUMP_PROGRAM_ID} success",
        f"Program {PUMP_PROGRAM_ID} invoke [2]", "Program data: " + base64.b64encode(close).decode(),
        f"Program {PUMP_PROGRAM_ID} success", f"Program {OTHER} success", "Log truncated",
    ]
    return tx


@pytest.mark.parametrize("prefix_available", [True, False])
def test_v2_nested_pump_close_keeps_trade_identity_and_emits_no_extra_state(prefix_available: bool) -> None:
    tx = _v2(_pump_trade_close())
    expected = PumpDecoder().decode(_without_marker(tx)).events
    if not prefix_available:
        tx["meta"]["logMessages"] = ["Log truncated"]
    actual = PumpDecoder().decode(tx).events
    assert len(actual) == len(expected) == 1
    assert actual[0].event_id == expected[0].event_id
    assert {k: v for k, v in actual[0].payload.items() if k != EVENT_IDENTITY_KEY} == {
        k: v for k, v in expected[0].payload.items() if k != EVENT_IDENTITY_KEY
    }
    assert actual[0].instruction_index == (expected[0].instruction_index if prefix_available else 2)
    assert actual[0].inner_instruction_index == (-1 if prefix_available else 1)
    assert PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES).decode(tx).events == []


def test_legacy_pump_close_keeps_visible_trade_identity_and_validates_ignored_clock() -> None:
    tx = _pump_trade_close()
    expected = PumpDecoder().decode(_without_marker(tx)).events
    actual = PumpDecoder().decode(tx).events
    assert [event.model_dump(mode="json", exclude={"observed_at"}) for event in actual] == [
        event.model_dump(mode="json", exclude={"observed_at"}) for event in expected
    ]
    tx["meta"]["logMessages"] = ["Log truncated"]
    assert PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES).decode(tx).events == []
    with pytest.raises(AnchorDecodeError, match="consumed events are missing"):
        PumpDecoder().decode(tx)


@pytest.mark.parametrize("failure", [
    "missing_close_cpi", "extra_close_cpi", "short_body", "wrong_clock",
    "wrong_event", "wrong_parent", "unknown_late_operation",
])
def test_ignored_pump_close_requires_full_body_clock_parent_and_cardinality(failure: str) -> None:
    from sniper_bot.protocols.anchor import _base58_decode

    tx = _v2(_pump_trade_close())
    tx["meta"]["logMessages"] = ["Log truncated"]
    inner = tx["meta"]["innerInstructions"][0]["instructions"]
    if failure == "missing_close_cpi":
        inner.pop()
    elif failure == "extra_close_cpi":
        inner.append(copy.deepcopy(inner[-1]))
    elif failure == "short_body":
        inner[-1]["data"] = _base58_encode(_base58_decode(inner[-1]["data"])[:-1])
    elif failure == "wrong_clock":
        payload = bytearray(_base58_decode(inner[-1]["data"]))
        struct.pack_into("<q", payload, len(TAG) + 8 + 32, tx["blockTime"] + 120)
        inner[-1]["data"] = _base58_encode(payload)
    elif failure == "wrong_event":
        inner[-1] = copy.deepcopy(inner[1])
    elif failure == "wrong_parent":
        inner[-1]["stackHeight"] = 4
    elif failure == "unknown_late_operation":
        inner.append({"programId": PUMP_PROGRAM_ID, "data": _base58_encode(bytes([255]) * 8), "stackHeight": 2})
    with pytest.raises(AnchorDecodeError):
        PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES).decode(tx)



def _swap_sell_cashback() -> dict[str, Any]:
    """A foreign router calls PumpSwap sell and claims cashback afterward."""
    from scripts.benchmark_postgres_capacity import BorshEventEncoder
    from sniper_bot.registry import WSOL_MINT

    fixture = _fixture()
    timestamp = fixture["blockTime"]
    encoder = BorshEventEncoder(Path(__file__).parents[1] / "src/sniper_bot/protocols/pumpswap/idl.json")
    sell = base64.b64decode(encoder.encode("SellEvent", {"timestamp": timestamp, "quote_mint": WSOL_MINT}))
    cashback = base64.b64decode(encoder.encode("ClaimCashbackEvent", {"timestamp": timestamp, "amount": 1}))
    assert len(cashback) == 72
    tx = {
        "slot": 5, "blockTime": timestamp,
        "transaction": {"signatures": ["swap-cashback"], "message": {"instructions": [{"programId": OTHER}] * 3}},
        "meta": {"err": None, "innerInstructions": [{"index": 2, "instructions": [
            {**_instruction("sell"), "stackHeight": 2}, {**_cpi(sell), "stackHeight": 3},
            {**_instruction("claim_cashback"), "stackHeight": 2}, {**_cpi(cashback), "stackHeight": 3},
        ]}], "logMessages": [
            f"Program {OTHER} invoke [1]", f"Program {PUMPSWAP_PROGRAM_ID} invoke [2]",
            "Program data: " + base64.b64encode(sell).decode(), f"Program {PUMPSWAP_PROGRAM_ID} success",
            f"Program {PUMPSWAP_PROGRAM_ID} invoke [2]", "Program data: " + base64.b64encode(cashback).decode(),
            f"Program {PUMPSWAP_PROGRAM_ID} success", f"Program {OTHER} success", "Log truncated",
        ]},
    }
    return tx


@pytest.mark.parametrize("prefix_available", [True, False])
def test_v2_cashback_preserves_sell_identity_and_creates_no_extra_event(prefix_available: bool) -> None:
    tx = _v2(_swap_sell_cashback())
    expected = PumpSwapDecoder().decode(_without_marker(tx)).events
    if not prefix_available:
        tx["meta"]["logMessages"] = ["Log truncated"]
    actual = PumpSwapDecoder().decode(tx).events
    assert len(actual) == len(expected) == 1
    assert actual[0].event_type.value == "swap_sell"
    assert actual[0].event_id == expected[0].event_id
    assert {k: v for k, v in actual[0].payload.items() if k != EVENT_IDENTITY_KEY} == {
        k: v for k, v in expected[0].payload.items() if k != EVENT_IDENTITY_KEY
    }
    assert actual[0].instruction_index == (expected[0].instruction_index if prefix_available else 2)
    assert actual[0].inner_instruction_index == (-1 if prefix_available else 1)
    assert PumpSwapDecoder(event_names=frozenset()).decode(tx).events == []


def test_legacy_cashback_preserves_visible_sell_identity_but_rejects_missing_sell() -> None:
    tx = _swap_sell_cashback()
    expected = PumpSwapDecoder().decode(_without_marker(tx)).events
    actual = PumpSwapDecoder().decode(tx).events
    assert [event.model_dump(mode="json", exclude={"observed_at"}) for event in actual] == [
        event.model_dump(mode="json", exclude={"observed_at"}) for event in expected
    ]
    tx["meta"]["logMessages"] = ["Log truncated"]
    with pytest.raises(AnchorDecodeError, match="consumed events are missing"):
        PumpSwapDecoder().decode(tx)


@pytest.mark.parametrize("failure", [
    "missing_cashback_cpi", "extra_cashback_cpi", "short_body", "wrong_clock",
    "wrong_event", "wrong_parent", "unknown_late_operation",
])
def test_ignored_cashback_requires_full_body_clock_parent_and_cardinality(failure: str) -> None:
    from sniper_bot.protocols.anchor import _base58_decode

    tx = _v2(_swap_sell_cashback())
    tx["meta"]["logMessages"] = ["Log truncated"]
    inner = tx["meta"]["innerInstructions"][0]["instructions"]
    if failure == "missing_cashback_cpi":
        inner.pop()
    elif failure == "extra_cashback_cpi":
        inner.append(copy.deepcopy(inner[-1]))
    elif failure == "short_body":
        inner[-1]["data"] = _base58_encode(_base58_decode(inner[-1]["data"])[:-1])
    elif failure == "wrong_clock":
        payload = bytearray(_base58_decode(inner[-1]["data"]))
        struct.pack_into("<q", payload, len(TAG) + 8 + 32 + 8, tx["blockTime"] + 120)
        inner[-1]["data"] = _base58_encode(payload)
    elif failure == "wrong_event":
        inner[-1] = copy.deepcopy(inner[1])
    elif failure == "wrong_parent":
        inner[-1]["stackHeight"] = 4
    elif failure == "unknown_late_operation":
        inner.append({"programId": PUMPSWAP_PROGRAM_ID, "data": _base58_encode(bytes([255]) * 8), "stackHeight": 2})
    with pytest.raises(AnchorDecodeError):
        PumpSwapDecoder(event_names=frozenset()).decode(tx)


def test_standalone_ignored_cashback_resolves_cpi_clock_without_state_or_identity() -> None:
    tx = _v2(_swap_sell_cashback())
    expected = PumpSwapDecoder().decode(tx).block_time
    del tx["blockTime"]
    tx["meta"]["innerInstructions"][0]["instructions"] = tx["meta"]["innerInstructions"][0]["instructions"][2:]
    tx["meta"]["logMessages"] = ["Log truncated"]
    actual = PumpSwapDecoder().decode(tx)
    assert actual.events == [] and actual.block_time == expected



def _pump_creator_fees() -> dict[str, Any]:
    """Create, nested creator controls and a buy share one transaction."""
    from scripts.benchmark_postgres_capacity import BorshEventEncoder

    tx = _pump_trade(name="buy")
    anchor = PumpDecoder()._anchor
    encoder = BorshEventEncoder(Path(__file__).parents[1] / "src/sniper_bot/protocols/pump/idl.json")
    stamp = tx["blockTime"]

    def event(name: str, **fields: Any) -> bytes:
        return base64.b64decode(encoder.encode(name, {"timestamp": stamp, **fields}))

    def operation(name: str, height: int = 1) -> dict[str, Any]:
        definition = next(item for item in anchor.idl["instructions"] if item["name"] == name)
        return {"programId": PUMP_PROGRAM_ID, "data": _base58_encode(bytes(definition["discriminator"])),
                "stackHeight": height}

    create = event("CreateEvent")
    extend = event("ExtendAccountEvent")
    migrate = event("MigrateBondingCurveCreatorEvent")
    distribute = event("DistributeCreatorFeesEvent", shareholders=[{"address": OTHER, "share_bps": 10_000}])
    assert len(migrate) == 176 and len(distribute) == 222
    buy = tx["transaction"]["message"]["instructions"][0]
    trade_cpi = tx["meta"]["innerInstructions"][0]["instructions"][0]
    trade_log = anchor._own_event_lines(tx["meta"]["logMessages"])[0][1]
    tx["transaction"]["message"]["instructions"] = [
        {"programId": OTHER}, {"programId": OTHER}, operation("create_v2"),
        {"programId": OTHER}, {"programId": OTHER}, {"programId": OTHER}, buy,
    ]
    tx["meta"]["innerInstructions"] = [
        {"index": 2, "instructions": [_cpi(create, program_id=PUMP_PROGRAM_ID)]},
        {"index": 3, "instructions": [
            operation("extend_account", 2), {**_cpi(extend, program_id=PUMP_PROGRAM_ID), "stackHeight": 3},
            operation("migrate_bonding_curve_creator", 2), {**_cpi(migrate, program_id=PUMP_PROGRAM_ID), "stackHeight": 3},
        ]},
        {"index": 4, "instructions": [
            operation("distribute_creator_fees", 2), {**_cpi(distribute, program_id=PUMP_PROGRAM_ID), "stackHeight": 3},
        ]},
        {"index": 6, "instructions": [trade_cpi]},
    ]
    logs: list[str] = []
    for payload in (create, extend, migrate, distribute):
        logs.extend([f"Program {PUMP_PROGRAM_ID} invoke [1]",
                     "Program data: " + base64.b64encode(payload).decode(), f"Program {PUMP_PROGRAM_ID} success"])
    logs.extend([f"Program {PUMP_PROGRAM_ID} invoke [1]", "Program data: " + trade_log,
                 f"Program {PUMP_PROGRAM_ID} success", "Log truncated"])
    tx["meta"]["logMessages"] = logs
    return tx


@pytest.mark.parametrize("prefix", ["complete", "controls", "empty"])
def test_v2_creator_controls_preserve_create_trade_ids_and_live_selection(prefix: str) -> None:
    tx = _v2(_pump_creator_fees())
    decoder = PumpDecoder()
    expected = decoder.decode(_without_marker(tx)).events
    if prefix == "controls":
        tx["meta"]["logMessages"] = tx["meta"]["logMessages"][:-4] + ["Log truncated"]
    elif prefix == "empty":
        tx["meta"]["logMessages"] = ["Log truncated"]
    unchanged = copy.deepcopy(tx)
    actual = decoder.decode(tx).events
    live = PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES).decode(tx).events
    assert [event.event_type.value for event in actual] == ["token_created", "swap_buy"]
    assert [event.event_id for event in actual] == [event.event_id for event in expected]
    assert [event.event_id for event in live] == [actual[0].event_id]
    for event, complete in zip(actual, expected, strict=True):
        assert {k: v for k, v in event.payload.items() if k != EVENT_IDENTITY_KEY} == {
            k: v for k, v in complete.payload.items() if k != EVENT_IDENTITY_KEY
        }
        assert event.payload[EVENT_IDENTITY_KEY]["ordinal"] == 0
    if prefix != "complete":
        assert actual[1].instruction_index == 6 and actual[1].inner_instruction_index == 0
    if prefix == "empty":
        assert actual[0].instruction_index == 2 and actual[0].inner_instruction_index == 0
    assert PumpDecoder(event_names=frozenset()).decode(tx).events == []
    assert tx == unchanged


def test_legacy_creator_controls_keep_visible_ids_and_require_consumed_trade_prefix() -> None:
    tx = _pump_creator_fees()
    decoder = PumpDecoder()
    expected = decoder.decode(_without_marker(tx)).events
    actual = decoder.decode(tx).events
    assert [event.model_dump(mode="json", exclude={"observed_at"}) for event in actual] == [
        event.model_dump(mode="json", exclude={"observed_at"}) for event in expected
    ]
    tx["meta"]["logMessages"] = tx["meta"]["logMessages"][:-4] + ["Log truncated"]
    live = PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES).decode(tx).events
    assert [event.event_id for event in live] == [expected[0].event_id]
    with pytest.raises(AnchorDecodeError, match="consumed events are missing"):
        decoder.decode(tx)


@pytest.mark.parametrize("operation", ["migrate", "distribute"])
@pytest.mark.parametrize("failure", ["missing", "extra", "short", "trailing", "clock", "wrong_event", "wrong_parent"])
def test_ignored_creator_control_requires_full_body_clock_own_parent_and_one_cpi(operation: str, failure: str) -> None:
    from sniper_bot.protocols.anchor import _base58_decode

    tx = _v2(_pump_creator_fees())
    tx["meta"]["logMessages"] = ["Log truncated"]
    group = tx["meta"]["innerInstructions"][1 if operation == "migrate" else 2]["instructions"]
    index = 3 if operation == "migrate" else 1
    if failure == "missing":
        group.pop(index)
    elif failure == "extra":
        group.insert(index + 1, copy.deepcopy(group[index]))
    elif failure in {"short", "trailing", "clock"}:
        payload = bytearray(_base58_decode(group[index]["data"]))
        if failure == "short":
            payload.pop()
        elif failure == "trailing":
            payload.append(0)
        else:
            struct.pack_into("<q", payload, len(TAG) + 8, tx["blockTime"] + 120)
        group[index]["data"] = _base58_encode(payload)
    elif failure == "wrong_event":
        group[index]["data"] = tx["meta"]["innerInstructions"][0]["instructions"][0]["data"]
    elif failure == "wrong_parent":
        group[index]["stackHeight"] = 4
    with pytest.raises(AnchorDecodeError):
        PumpDecoder(event_names=frozenset()).decode(tx)


@pytest.mark.parametrize("failure", ["truncated_count", "truncated_shareholder", "missing_shareholder", "zero_count", "unreasonable_count"])
def test_ignored_fee_distribution_validates_shareholder_vector(failure: str) -> None:
    from sniper_bot.protocols.anchor import _base58_decode

    tx = _v2(_pump_creator_fees())
    tx["meta"]["logMessages"] = ["Log truncated"]
    cpi = tx["meta"]["innerInstructions"][2]["instructions"][1]
    payload = bytearray(_base58_decode(cpi["data"]))
    count_offset = len(TAG) + 8 + 8 + 4 * 32
    if failure == "truncated_count":
        del payload[count_offset + 3:]
    elif failure == "truncated_shareholder":
        del payload[count_offset + 4 + 20:]
    else:
        count = 2 if failure == "missing_shareholder" else (0 if failure == "zero_count" else 0xFFFFFFFF)
        struct.pack_into("<I", payload, count_offset, count)
    cpi["data"] = _base58_encode(payload)
    with pytest.raises(AnchorDecodeError):
        PumpDecoder(event_names=frozenset()).decode(tx)


@pytest.mark.parametrize("operation", ["migrate", "distribute"])
def test_standalone_creator_control_dates_from_cpi_without_state_or_identity(operation: str) -> None:
    tx = _v2(_pump_creator_fees())
    expected = PumpDecoder().decode(tx).block_time
    group = tx["meta"]["innerInstructions"][1 if operation == "migrate" else 2]
    pair = group["instructions"][-2:]
    tx["transaction"]["message"]["instructions"] = [{"programId": OTHER}]
    tx["meta"]["innerInstructions"] = [{"index": 0, "instructions": pair}]
    tx["meta"]["logMessages"] = ["Log truncated"]
    del tx["blockTime"]
    actual = PumpDecoder().decode(tx)
    assert actual.events == [] and actual.block_time == expected


def test_creator_controls_do_not_allow_an_unreviewed_late_operation() -> None:
    tx = _v2(_pump_creator_fees())
    tx["transaction"]["message"]["instructions"].append({
        "programId": PUMP_PROGRAM_ID, "data": _base58_encode(bytes([255]) * 8),
    })
    with pytest.raises(AnchorDecodeError, match="unverified own operation"):
        PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES).decode(tx)
