"""Pump event adapter backed by the official vendored Anchor IDL."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ...events import ChainEventType, EventEnvelope, EventSource, Protocol
from ..anchor import AnchorIdlDecoder

PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
ADAPTER_VERSION = "pump-idl-9c82f61"

_EVENT_TYPES = {
    "CreateEvent": ChainEventType.TOKEN_CREATED,
    "TradeEvent": None,
    "CompleteEvent": ChainEventType.BONDING_CURVE_COMPLETED,
    "CompletePumpAmmMigrationEvent": ChainEventType.MIGRATION,
}


class PumpDecoder:
    def __init__(self, idl_path: str | Path | None = None) -> None:
        path = Path(idl_path) if idl_path else Path(__file__).with_name("idl.json")
        self._anchor = AnchorIdlDecoder(path)
        if self._anchor.program_id != PUMP_PROGRAM_ID:
            raise ValueError("vendored Pump IDL has unexpected program address")

    def decode_transaction(
        self,
        transaction: dict[str, Any],
        *,
        source: EventSource = EventSource.HELIUS_WSS,
        observed_at: datetime | None = None,
    ) -> list[EventEnvelope]:
        meta = transaction.get("meta") or transaction.get("transaction", {}).get("meta") or {}
        logs = meta.get("logMessages") or transaction.get("logs") or []
        signature = _signature(transaction)
        slot = int(transaction.get("slot", 0))
        block_time = _block_time(transaction)
        observed = observed_at or datetime.now(tz=timezone.utc)
        result: list[EventEnvelope] = []

        for event in self._anchor.decode_logs(list(logs)):
            event_type = _EVENT_TYPES.get(event.name)
            if event.name == "TradeEvent":
                event_type = (
                    ChainEventType.SWAP_BUY
                    if event.fields.get("is_buy")
                    else ChainEventType.SWAP_SELL
                )
            if event_type is None:
                continue
            fields = {**event.fields, "anchor_event": event.name, "adapter_version": ADAPTER_VERSION}
            result.append(
                EventEnvelope(
                    source=source,
                    protocol=Protocol.PUMP,
                    event_type=event_type,
                    slot=slot,
                    signature=signature,
                    instruction_index=event.log_index,
                    inner_instruction_index=-1,
                    block_time=block_time,
                    observed_at=observed,
                    mint=fields.get("mint"),
                    pool_address=fields.get("pool") or fields.get("bonding_curve"),
                    payload=fields,
                )
            )
        return result


def _signature(transaction: dict[str, Any]) -> str:
    signature = transaction.get("signature")
    if signature:
        return str(signature)
    signatures = transaction.get("transaction", {}).get("signatures") or []
    if signatures:
        return str(signatures[0])
    raise ValueError("transaction signature is missing")


def _block_time(transaction: dict[str, Any]) -> datetime:
    value = transaction.get("blockTime")
    if value is None:
        raise ValueError("transaction blockTime is missing")
    return datetime.fromtimestamp(int(value), tz=timezone.utc)
