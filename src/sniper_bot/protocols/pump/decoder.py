"""Pump event adapter backed by the official vendored Anchor IDL."""

from __future__ import annotations

from dataclasses import dataclass
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
PUMP_EVENT_NAMES = frozenset(_EVENT_TYPES)
# Bonding-curve trades carry no pool address, so live state never applies
# them; they are most of the Pump volume and only dated, never decoded.
PUMP_STATE_EVENT_NAMES = PUMP_EVENT_NAMES - {"TradeEvent"}


@dataclass(frozen=True)
class DecodedTransaction:
    events: list[EventEnvelope]
    block_time: datetime | None
    unknown_discriminators: int = 0


class PumpDecoder:
    def __init__(
        self,
        idl_path: str | Path | None = None,
        *,
        event_names: frozenset[str] = PUMP_EVENT_NAMES,
    ) -> None:
        path = Path(idl_path) if idl_path else Path(__file__).with_name("idl.json")
        self._anchor = AnchorIdlDecoder(path)
        if self._anchor.program_id != PUMP_PROGRAM_ID:
            raise ValueError("vendored Pump IDL has unexpected program address")
        self._event_names = event_names

    def decode_transaction(
        self,
        transaction: dict[str, Any],
        *,
        source: EventSource = EventSource.HELIUS_WSS,
        observed_at: datetime | None = None,
    ) -> list[EventEnvelope]:
        return self.decode(transaction, source=source, observed_at=observed_at).events

    def decode(
        self,
        transaction: dict[str, Any],
        *,
        source: EventSource = EventSource.HELIUS_WSS,
        observed_at: datetime | None = None,
    ) -> DecodedTransaction:
        scan = self._anchor.scan_logs(
            list(_log_messages(transaction)), event_names=self._event_names
        )
        block_time = _resolve_block_time(transaction, scan.timestamp)
        if not scan.events:
            return DecodedTransaction([], block_time, scan.skipped_unknown_discriminators)
        signature = _signature(transaction)
        slot = int(transaction.get("slot", 0))
        observed = observed_at or datetime.now(tz=timezone.utc)
        confirmed_at = _require_block_time(block_time)
        result: list[EventEnvelope] = []

        for event in scan.events:
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
                    block_time=confirmed_at,
                    observed_at=observed,
                    mint=fields.get("mint"),
                    pool_address=fields.get("pool") or fields.get("bonding_curve"),
                    payload=fields,
                )
            )
        return DecodedTransaction(result, block_time, scan.skipped_unknown_discriminators)


def _log_messages(transaction: dict[str, Any]) -> list[str]:
    meta = transaction.get("meta") or _inner_transaction(transaction).get("meta") or {}
    return list(meta.get("logMessages") or transaction.get("logs") or [])


def _inner_transaction(transaction: dict[str, Any]) -> dict[str, Any]:
    inner = transaction.get("transaction")
    return inner if isinstance(inner, dict) else {}


def _signature(transaction: dict[str, Any]) -> str:
    signature = transaction.get("signature")
    if signature:
        return str(signature)
    signatures = _inner_transaction(transaction).get("signatures") or []
    if signatures:
        return str(signatures[0])
    raise ValueError("transaction signature is missing")


def _resolve_block_time(
    transaction: dict[str, Any], event_timestamp: int | None
) -> datetime | None:
    """Prefer the RPC block time; otherwise use the program's Clock timestamp.

    Streaming notifications carry no block time, but every Pump and PumpSwap
    event records ``Clock::unix_timestamp``, which is the value getBlockTime
    reports for the slot.
    """
    value = transaction.get("blockTime")
    if value is None:
        value = event_timestamp
    if value is None:
        return None
    return datetime.fromtimestamp(int(value), tz=timezone.utc)


def _require_block_time(block_time: datetime | None) -> datetime:
    if block_time is None:
        raise ValueError("transaction blockTime is missing")
    return block_time
