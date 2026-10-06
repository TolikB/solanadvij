"""Pump event adapter backed by the official vendored Anchor IDL."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any

from ...events import (
    EVENT_IDENTITY_KEY,
    ChainEventType,
    EventEnvelope,
    EventSource,
    Protocol,
    uses_event_id_v2,
)
from ..anchor import AnchorDecodeError, AnchorIdlDecoder, AnchorLogScan, EventContract, contract_events

PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
ADAPTER_VERSION = "pump-idl-cb188ce"

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
# Each reviewed truncated operation requires exactly its expected CPI event.
# Additional completion events or other operations still fail closed.
_TRUNCATION_INSTRUCTION_EVENTS: dict[str, EventContract] = {
    "create": "CreateEvent",
    "create_v2": "CreateEvent",
    "extend_account": "ExtendAccountEvent",
    "migrate": "CompletePumpAmmMigrationEvent",
    "migrate_v2": "CompletePumpAmmMigrationEvent",
    "init_user_volume_accumulator": "InitUserVolumeAccumulatorEvent",
    "sync_user_volume_accumulator": "SyncUserVolumeAccumulatorEvent",
    "close_user_volume_accumulator": "CloseUserVolumeAccumulatorEvent",
    "migrate_bonding_curve_creator": "MigrateBondingCurveCreatorEvent",
    "distribute_creator_fees": "DistributeCreatorFeesEvent",
    "collect_creator_fee": "CollectCreatorFeeEvent",
    "collect_creator_fee_v2": "CollectCreatorFeeEvent",
    "distribute_fee_to_holders": "DistributeFeeToHoldersEvent",
    "distribute_creator_fees_v2": "DistributeCreatorFeesEvent",
    "claim_cashback": "ClaimCashbackEvent",
    "claim_cashback_v2": "ClaimCashbackEvent",
    "claim_token_incentives": "ClaimTokenIncentivesEvent",
    "set_creator": "SetCreatorEvent",
    "set_metaplex_creator": "SetMetaplexCreatorEvent",
    "update_creator_fee_config": "UpdateCreatorFeeConfigEvent",
    "admin_cto": "AdminCtoEvent",
    "admin_set_idl_authority": "AdminSetIdlAuthorityEvent",
    "admin_update_token_incentives": "AdminUpdateTokenIncentivesEvent",
    "add_quote_control_mint": "AddQuoteControlMintEvent",
    "remove_quote_control_mint": "RemoveQuoteControlMintEvent",
    "set_params": "SetParamsEvent",
    "set_quote_control_admin": "SetQuoteControlAdminEvent",
    "update_global_authority": "UpdateGlobalAuthorityEvent",
    "sweep_creator_fee": "SweepBondingCurveFeeEvent",
    "sweep_protocol_fee": "SweepBondingCurveFeeEvent",
    "buy": "TradeEvent",
    "buy_exact_sol_in": "TradeEvent",
    "buy_v2": "TradeEvent",
    "buy_exact_quote_in_v2": "TradeEvent",
    "buy_exact_quote_in_v3": "TradeEvent",
    "buy_v3": "TradeEvent",
    "sell_v3": "TradeEvent",
    "sell": "TradeEvent",
    "sell_v2": "TradeEvent",
}
# Fee controls skip their event when there is nothing to move, so an ignored
# control may prove zero or one CPI event; consumed operations still need one.
_OPTIONAL_INSTRUCTIONS = frozenset(
    name for name, contract in _TRUNCATION_INSTRUCTION_EVENTS.items()
    if contract_events(contract) and PUMP_EVENT_NAMES.isdisjoint(contract_events(contract))
)
# These selectors are published by the officially recommended pump-rust-client
# 0.2.0. Its trade event schemas match the pinned IDL; the sweep event is
# absent from it and taken from the same crate (see SOURCE.md). Each still
# requires the full CPI proof; other operations remain closed.
_SUPPLEMENTAL_INSTRUCTIONS: Mapping[bytes, str] = MappingProxyType({
    bytes.fromhex("e1f7501ed5b38488"): "buy_exact_quote_in_v3",
    bytes.fromhex("07051dc4f5176550"): "buy_v3",
    bytes.fromhex("1c92de7726c469d5"): "sell_v3",
    bytes.fromhex("20f6bf3408c949ba"): "sweep_creator_fee",
    bytes.fromhex("0830be07b644b7e5"): "sweep_protocol_fee",
})
_SUPPLEMENTAL_EVENTS: Mapping[bytes, tuple[str, Mapping[str, Any]]] = MappingProxyType({
    bytes.fromhex("742b4dbd117a482b"): ("SweepBondingCurveFeeEvent", {"kind": "struct", "fields": [
        {"name": "timestamp", "type": "i64"},
        {"name": "mint", "type": "pubkey"},
        {"name": "bonding_curve", "type": "pubkey"},
        {"name": "quote_mint", "type": "pubkey"},
        {"name": "recipient", "type": "pubkey"},
        {"name": "amount", "type": "u64"},
        {"name": "bucket", "type": "u8"},
    ]}),
})


def _completion_follows(name: str, fields: dict[str, Any]) -> tuple[str, ...]:
    """A buy that empties the curve's real token reserves completes it, and the
    same operation then emits the completion as its next direct CPI."""
    if name == "TradeEvent" and fields["is_buy"] and fields["real_token_reserves"] == 0:
        return ("CompleteEvent",)
    return ()


# Last field of each consumed event as deployed at pump-public-docs 9c82f61.
# Pump extends events by appending fields, so older payloads end here and
# newer ones carry more; both decode.
PUMP_MINIMUM_FIELDS = {
    "CreateEvent": "virtual_quote_reserves",
    "TradeEvent": "real_quote_reserves",
    "CompleteEvent": "quote_mint",
    "CompletePumpAmmMigrationEvent": "quote_mint",
}
# Clock timestamps outside this range can only come from a misread layout.
_EARLIEST_TIMESTAMP = 1_577_836_800  # 2020-01-01
_LATEST_TIMESTAMP = 4_102_444_800  # 2100-01-01
# Every event of one transaction shares the slot's Clock; an RPC block time
# is that same value.
_TIMESTAMP_TOLERANCE_SECONDS = 60


@dataclass(frozen=True)
class DecodedTransaction:
    events: list[EventEnvelope]
    block_time: datetime | None
    # Hex discriminators of event types the vendored IDL does not know.
    unknown_discriminators: tuple[str, ...] = ()
    # Consumed events that carried fields appended after the vendored IDL.
    appended_events: tuple[str, ...] = ()


def decoded_transaction(
    events: list[EventEnvelope], block_time: datetime | None, scan: AnchorLogScan
) -> DecodedTransaction:
    return DecodedTransaction(
        events=events,
        block_time=block_time,
        unknown_discriminators=tuple(item.hex() for item in scan.unknown_discriminators),
        appended_events=scan.appended_events,
    )


def check_event_timestamps(
    transaction: dict[str, Any], scan: AnchorLogScan, *, extra_timestamps: list[int] | None = None,
) -> None:
    """Fail closed when decoded Clock timestamps cannot be real.

    Appended fields decode cleanly, but a field inserted mid-struct would
    shift everything after it; a timestamp that is implausible, disagrees
    with the other events of the transaction, or with the RPC block time
    exposes such a misread.
    """
    stamps = [
        int(event.fields["timestamp"])
        for event in scan.events
        if type(event.fields.get("timestamp")) is int
    ]
    if scan.timestamp is not None:
        stamps.append(int(scan.timestamp))
    stamps.extend(scan.verified_timestamps)
    stamps.extend(extra_timestamps or [])
    if not stamps:
        return
    if any(not _EARLIEST_TIMESTAMP <= stamp < _LATEST_TIMESTAMP for stamp in stamps):
        raise AnchorDecodeError("Anchor event timestamp is implausible")
    reference = transaction.get("blockTime")
    anchor = int(reference) if reference is not None else stamps[0]
    if any(abs(stamp - anchor) > _TIMESTAMP_TOLERANCE_SECONDS for stamp in stamps):
        raise AnchorDecodeError("Anchor event timestamps disagree with the block time")


class PumpDecoder:
    def __init__(
        self,
        idl_path: str | Path | None = None,
        *,
        event_names: frozenset[str] = PUMP_EVENT_NAMES,
    ) -> None:
        path = Path(idl_path) if idl_path else Path(__file__).with_name("idl.json")
        self._anchor = AnchorIdlDecoder(path, supplemental_events=_SUPPLEMENTAL_EVENTS)
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
        logs = list(_log_messages(transaction))
        slot = int(transaction.get("slot", 0))
        signature = _signature(transaction, required=False)
        v2 = uses_event_id_v2(slot, signature)
        # Count each event type before the caller's selection/admission filters.
        event_names = PUMP_EVENT_NAMES if v2 else self._event_names
        if "Log truncated" in logs:
            scan = self._anchor.scan_verified_cpi_events(
                transaction, logs, event_names=event_names,
                instruction_events=_TRUNCATION_INSTRUCTION_EVENTS,
                supplemental_instructions=_SUPPLEMENTAL_INSTRUCTIONS,
                minimum_fields=PUMP_MINIMUM_FIELDS, recover_missing=v2,
                optional_instructions=_OPTIONAL_INSTRUCTIONS,
                required_followers=_completion_follows,
            )
        else:
            scan = self._anchor.scan_logs(
                logs, event_names=event_names, minimum_fields=PUMP_MINIMUM_FIELDS,
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
            if event.name == "TradeEvent":
                event_type = (
                    ChainEventType.SWAP_BUY
                    if event.fields.get("is_buy")
                    else ChainEventType.SWAP_SELL
                )
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
                    protocol=Protocol.PUMP,
                    event_type=event_type,
                    slot=slot,
                    signature=signature,
                    instruction_index=instruction_index,
                    inner_instruction_index=inner_index,
                    block_time=confirmed_at,
                    observed_at=observed,
                    mint=fields.get("mint"),
                    pool_address=fields.get("pool") or fields.get("bonding_curve"),
                    payload=fields,
                )
            )
        return decoded_transaction(result, block_time, scan)


def _log_messages(transaction: dict[str, Any]) -> list[str]:
    meta = transaction.get("meta") or _inner_transaction(transaction).get("meta") or {}
    return list(meta.get("logMessages") or transaction.get("logs") or [])


def _inner_transaction(transaction: dict[str, Any]) -> dict[str, Any]:
    inner = transaction.get("transaction")
    return inner if isinstance(inner, dict) else {}


def _signature(transaction: dict[str, Any], *, required: bool = True) -> str:
    signature = transaction.get("signature")
    if signature:
        return str(signature)
    signatures = _inner_transaction(transaction).get("signatures") or []
    if signatures:
        return str(signatures[0])
    if required:
        raise ValueError("transaction signature is missing")
    return ""


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
