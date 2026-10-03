"""PumpSwap event adapter backed by the official vendored Anchor IDL."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ...events import (
    EVENT_IDENTITY_KEY,
    ChainEventType,
    EventEnvelope,
    EventSource,
    Protocol,
    uses_event_id_v2,
)
from ...registry import target_mint_for_pool
from ..anchor import AnchorDecodeError, AnchorIdlDecoder
from ..pump.decoder import (
    DecodedTransaction,
    _log_messages,
    _require_block_time,
    _resolve_block_time,
    _signature,
    check_event_timestamps,
    decoded_transaction,
)

PUMPSWAP_PROGRAM_ID = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
ADAPTER_VERSION = "pumpswap-idl-cb188ce"

_EVENT_TYPES = {
    "CreatePoolEvent": ChainEventType.POOL_CREATED,
    "BuyEvent": ChainEventType.SWAP_BUY,
    "SellEvent": ChainEventType.SWAP_SELL,
    "DepositEvent": ChainEventType.LIQUIDITY_ADDED,
    "WithdrawEvent": ChainEventType.LIQUIDITY_REMOVED,
}
PUMPSWAP_EVENT_NAMES = frozenset(_EVENT_TYPES)
# Last field of each consumed event as deployed at pump-public-docs 9c82f61;
# fields appended since then decode when present.
PUMPSWAP_MINIMUM_FIELDS = {
    "CreatePoolEvent": "is_mayhem_mode",
    "BuyEvent": "base_supply",
    "SellEvent": "base_supply",
    "DepositEvent": "user_pool_token_account",
    "WithdrawEvent": "user_pool_token_account",
}


# Only these operation/event pairs have a completeness contract for truncated
# logs. Every other own operation remains quarantined until explicitly reviewed.
_TRUNCATION_INSTRUCTION_EVENTS = {
    "create_pool": "CreatePoolEvent",
    "buy": "BuyEvent",
    "buy_exact_quote_in": "BuyEvent",
    "sell": "SellEvent",
    "deposit": "DepositEvent",
    "withdraw": "WithdrawEvent",
    "close_user_volume_accumulator": "CloseUserVolumeAccumulatorEvent",
}


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
        logs = list(_log_messages(transaction))
        slot = int(transaction.get("slot", 0))
        signature = _signature(transaction, required=False)
        v2 = uses_event_id_v2(slot, signature)
        # Count each event type before the caller's selection/admission filters.
        event_names = PUMPSWAP_EVENT_NAMES if v2 else self._event_names
        if "Log truncated" in logs:
            scan = self._anchor.scan_verified_cpi_events(
                transaction, logs, event_names=event_names,
                instruction_events=_TRUNCATION_INSTRUCTION_EVENTS,
                minimum_fields=PUMPSWAP_MINIMUM_FIELDS, recover_missing=v2,
            )
        else:
            scan = self._anchor.scan_logs(
                logs, event_names=event_names, minimum_fields=PUMPSWAP_MINIMUM_FIELDS,
            )
        check_event_timestamps(transaction, scan)
        block_time = _resolve_block_time(transaction, scan.timestamp)
        if not scan.events:
            return decoded_transaction([], block_time, scan)
        signature = _signature(transaction)
        observed = observed_at or datetime.now(tz=timezone.utc)
        confirmed_at = _require_block_time(block_time)
        result: list[EventEnvelope] = []
        occurrences: dict[ChainEventType, int] = {}

        for event in scan.events:
            event_type = _EVENT_TYPES.get(event.name)
            if event_type is None:
                continue
            ordinal = occurrences.get(event_type, 0)
            occurrences[event_type] = ordinal + 1
            if event.name not in self._event_names:
                continue
            fields = {**event.fields, "anchor_event": event.name, "adapter_version": ADAPTER_VERSION}
            instruction_index = event.log_index
            inner_index = -1
            if v2:
                origin = "log"
                if event.log_index < 0:
                    if event.instruction_index is None or event.inner_instruction_index < 0:
                        raise AnchorDecodeError("recovered event has no CPI coordinates")
                    origin = "cpi"
                    instruction_index = event.instruction_index
                    inner_index = event.inner_instruction_index
                fields[EVENT_IDENTITY_KEY] = {"version": 2, "ordinal": ordinal, "origin": origin}
            result.append(
                EventEnvelope(
                    source=source,
                    protocol=Protocol.PUMPSWAP,
                    event_type=event_type,
                    slot=slot,
                    signature=signature,
                    instruction_index=instruction_index,
                    inner_instruction_index=inner_index,
                    block_time=confirmed_at,
                    observed_at=observed,
                    mint=target_mint_for_pool(
                        fields.get("base_mint"), fields.get("quote_mint")
                    ),
                    pool_address=fields.get("pool"),
                    payload=fields,
                )
            )
        return decoded_transaction(result, block_time, scan)
