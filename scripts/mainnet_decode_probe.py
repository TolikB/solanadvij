"""Decode sampled finalized mainnet blocks with this release's protocol decoders.

Run inside the release image before a record calibration. It finds the
transactions the stream would quarantine (an unreviewed operation, a CPI
contract mismatch, a layout change) in minutes instead of failing a 24-hour
window on the first one. Read-only: getSlot and getBlock through the configured
Helius RPC, whose URL is never printed. Exits 1 when any successful routed
transaction fails to decode, and writes a report with up to 20 such transactions.

    python scripts/mainnet_decode_probe.py --blocks 1500 --hours 24 --output probe.json
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from collections import Counter
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import httpx

from sniper_bot.config import AppConfig
from sniper_bot.events import Protocol
from sniper_bot.protocols import AnchorDecodeError
from sniper_bot.protocols.anchor import _base58_decode
from sniper_bot.protocols.pump.decoder import PUMP_STATE_EVENT_NAMES, PumpDecoder
from sniper_bot.protocols.pumpswap.decoder import PumpSwapDecoder
from sniper_bot.stream import _transaction_protocols

_EVENT_TAG = bytes.fromhex("e445a52e51cb9a1d")
_SLOT_SECONDS = 0.4
_UNAVAILABLE_SLOT_CODES = {-32004, -32007, -32009}
MAX_KEPT_FAILURES = 20


def _operations(decoder: Any, transaction: dict[str, Any]) -> list[str]:
    """Own operation names in execution order, for grouping failures."""
    anchor = decoder._anchor
    names = {bytes(item["discriminator"]): item["name"] for item in anchor.idl["instructions"]}
    groups = {
        group.get("index"): group.get("instructions") or []
        for group in (transaction.get("meta") or {}).get("innerInstructions") or []
    }
    message = (transaction.get("transaction") or {}).get("message") or {}
    operations: list[str] = []
    for index, root in enumerate(message.get("instructions") or []):
        for instruction in [root, *groups.get(index, [])]:
            if instruction.get("programId") != anchor.program_id:
                continue
            data = _base58_decode(instruction.get("data") or "")
            if not data.startswith(_EVENT_TAG):
                operations.append(names.get(data[:8], "?" + data[:8].hex()))
    return operations


def decode_report(transactions: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Decode exactly as the pipeline does: route, then one decoder per protocol."""
    decoders = {Protocol.PUMP: PumpDecoder(event_names=PUMP_STATE_EVENT_NAMES), Protocol.PUMPSWAP: PumpSwapDecoder()}
    counts: Counter[str] = Counter()
    shapes: Counter[str] = Counter()
    kept: list[dict[str, Any]] = []
    for transaction in transactions:
        meta = transaction.get("meta") or {}
        if meta.get("err") is not None:
            continue
        protocols = _transaction_protocols(transaction)
        if not protocols:
            continue
        counts["routed"] += 1
        counts["truncated"] += "Log truncated" in (meta.get("logMessages") or [])
        for protocol in protocols:
            decoder = decoders[protocol]
            try:
                decoder.decode(transaction)
            except AnchorDecodeError as error:
                reason = str(error)
            except Exception as error:  # the pipeline would crash rather than quarantine
                reason = f"unexpected {type(error).__name__}"
            else:
                counts[f"decoded_{protocol.value}"] += 1
                continue
            counts["failures"] += 1
            shape = f"{protocol.value}: {reason} | {_operations(decoder, transaction)}"
            shapes[shape] += 1
            if len(kept) < MAX_KEPT_FAILURES:
                kept.append({"shape": shape, "transaction": transaction})
    return {"counts": dict(counts), "failure_shapes": dict(shapes.most_common()), "failures": kept}


class _Rpc:
    def __init__(self, url: str) -> None:
        self._client = httpx.Client(timeout=60)
        self._url = url

    def call(self, method: str, params: list[Any], attempts: int = 6) -> Any:
        body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        for attempt in range(attempts):
            try:
                payload = self._client.post(self._url, json=body).json()
            except (httpx.HTTPError, ValueError):
                payload = None
            if isinstance(payload, dict) and "result" in payload:
                return payload["result"]
            if isinstance(payload, dict) and payload.get("error", {}).get("code") in _UNAVAILABLE_SLOT_CODES:
                return None
            time.sleep(1.5 * (attempt + 1))
        raise RuntimeError(f"{method} failed after {attempts} attempts")


def sample_transactions(rpc: _Rpc, *, blocks: int, hours: float, workers: int) -> Iterator[dict[str, Any]]:
    head = int(rpc.call("getSlot", [{"commitment": "finalized"}]))
    step = max(1, int(hours * 3600 / _SLOT_SECONDS) // blocks)
    slots = [head - index * step - random.randrange(step) for index in range(blocks)]
    options = {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1, "transactionDetails": "full",
               "rewards": False, "commitment": "finalized"}

    def fetch(slot: int) -> list[dict[str, Any]]:
        block = rpc.call("getBlock", [slot, options])
        if not block:
            return []
        return [
            {"slot": slot, "blockTime": block.get("blockTime"), "version": item.get("version"),
             "transaction": item["transaction"], "meta": item.get("meta") or {}}
            for item in block.get("transactions") or []
        ]

    with ThreadPoolExecutor(workers) as pool:
        for transactions in pool.map(fetch, slots):
            yield from transactions


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--blocks", type=int, default=1500)
    parser.add_argument("--hours", type=float, default=24.0)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    config = AppConfig.load(os.environ.get("CONFIG_PATH") or None)
    rpc = _Rpc(config.resolved_helius_rpc_url())
    report = decode_report(sample_transactions(rpc, blocks=args.blocks, hours=args.hours, workers=args.workers))
    report["sample"] = {"blocks": args.blocks, "hours": args.hours}
    args.output.write_text(json.dumps(report) + "\n")
    print(json.dumps({"counts": report["counts"], "failure_shapes": report["failure_shapes"]}))
    return 1 if report["counts"].get("failures") else 0


if __name__ == "__main__":
    sys.exit(main())
