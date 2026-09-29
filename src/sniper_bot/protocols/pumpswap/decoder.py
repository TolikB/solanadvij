"""PumpSwap event adapter backed by the official vendored Anchor IDL."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ...events import ChainEventType, EventEnvelope, EventSource, Protocol
from ...registry import target_mint_for_pool
from ..anchor import AnchorIdlDecoder
from ..pump.decoder import (
    DecodedTransaction,
    _log_messages,
    _require_block_time,
    _resolve_block_time,
    _signature,
)

PUMPSWAP_PROGRAM_ID = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
ADAPTER_VERSION = "pumpswap-idl-9c82f61"

_EVENT_TYPES = {
    "CreatePoolEvent": ChainEventType.POOL_CREATED,
    "BuyEvent": ChainEventType.SWAP_BUY,
    "SellEvent": ChainEventType.SWAP_SELL,
    "DepositEvent": ChainEventType.LIQUIDITY_ADDED,
    "WithdrawEvent": ChainEventType.LIQUIDITY_REMOVED,
}
PUMPSWAP_EVENT_NAMES = frozenset(_EVENT_TYPES)


class PumpSwapDecoder:
    def __init__(
        self,
        idl_path: str | Path | None = None,
        *,
        event_names: frozenset[str] = PUMPSWAP_EVENT_NAMES,
    ) -> None:
        path = Path(idl_path) if idl_path else Path(__file__).with_name("idl.json")
        self._anchor = AnchorIdlDecoder(path)
        if self._anchor.program_id != PUMPSWAP_PROGRAM_ID:
            raise ValueError("vendored PumpSwap IDL has unexpected program address")
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
            if event_type is None:
                continue
            fields = {**event.fields, "anchor_event": event.name, "adapter_version": ADAPTER_VERSION}
            result.append(
                EventEnvelope(
                    source=source,
                    protocol=Protocol.PUMPSWAP,
                    event_type=event_type,
                    slot=slot,
                    signature=signature,
                    instruction_index=event.log_index,
                    inner_instruction_index=-1,
                    block_time=confirmed_at,
                    observed_at=observed,
                    mint=target_mint_for_pool(
                        fields.get("base_mint"), fields.get("quote_mint")
                    ),
                    pool_address=fields.get("pool"),
                    payload=fields,
                )
            )
        return DecodedTransaction(result, block_time, scan.skipped_unknown_discriminators)
