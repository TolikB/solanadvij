"""Token registry and executable PumpSwap pool-state calculations."""

from __future__ import annotations

from collections.abc import Collection
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field

from .events import ChainEventType, EventEnvelope

WSOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
SUPPORTED_QUOTE_MINTS = frozenset({WSOL_MINT, USDC_MINT})


def target_mint_for_pool(base_mint: str | None, quote_mint: str | None) -> str | None:
    if not base_mint or not quote_mint:
        return None
    base_is_quote = base_mint in SUPPORTED_QUOTE_MINTS
    quote_is_quote = quote_mint in SUPPORTED_QUOTE_MINTS
    if base_is_quote == quote_is_quote:
        return None
    return quote_mint if base_is_quote else base_mint


class TokenRecord(BaseModel):
    mint: str
    token_program: str | None = None
    name: str | None = None
    symbol: str | None = None
    decimals: int | None = None
    total_supply_raw: Decimal | None = None
    creator_address: str | None = None
    creation_signature: str | None = None
    creation_slot: int | None = None
    creation_time: datetime | None = None
    metadata_uri: str | None = None
    metadata_mutable: bool | None = None
    enrichment: dict[str, Any] = Field(default_factory=dict)
    enriched_at: datetime | None = None
    bonding_curve_address: str | None = None
    bonding_curve_complete: bool = False
    migration_time: datetime | None = None
    first_pool_address: str | None = None
    first_pool_time: datetime | None = None
    updated_at: datetime


class PoolRecord(BaseModel):
    pool_address: str
    base_mint: str
    quote_mint: str
    protocol: str = "pumpswap"
    base_vault: str | None = None
    quote_vault: str | None = None
    creation_signature: str
    creation_slot: int
    creation_time: datetime
    migration_signature: str | None = None
    status: str = "active"
    base_decimals: int
    quote_decimals: int
    source_orientation_reversed: bool = False
    updated_at: datetime


class PoolState(BaseModel):
    pool_address: str
    base_mint: str
    quote_mint: str
    raw_base_reserves: Decimal = Decimal("0")
    raw_quote_reserves: Decimal = Decimal("0")
    virtual_base_reserves: Decimal = Decimal("0")
    virtual_quote_reserves: Decimal = Decimal("0")
    effective_base_reserves: Decimal = Decimal("0")
    effective_quote_reserves: Decimal = Decimal("0")
    quote_reserve_usd: Decimal = Decimal("0")
    base_reserve_usd: Decimal = Decimal("0")
    marginal_price_usd: Decimal = Decimal("0")
    market_cap_estimate_usd: Decimal | None = None
    pool_age_seconds: Decimal = Decimal("0")
    last_trade_time: datetime | None = None
    last_update_time: datetime
    quote_price_updated_at: datetime
    base_supply_raw: Decimal | None = None
    source_orientation_reversed: bool = False
    data_quality_flags: list[str] = Field(default_factory=list)
    last_update_slot: int = 0
    # Total swap fee (LP + protocol + coin creator) from the latest trade.
    swap_fee_bps: Decimal | None = None


class QuoteAssetPrice(BaseModel):
    mint: str
    price_usd: Decimal = Field(gt=0)
    observed_at: datetime

    def is_stale(self, now: datetime, max_age_seconds: Decimal = Decimal("15")) -> bool:
        return Decimal(str((now - self.observed_at).total_seconds())) > max_age_seconds


class TokenRegistry:
    def __init__(self) -> None:
        self._tokens: dict[str, TokenRecord] = {}

    def __len__(self) -> int:
        return len(self._tokens)

    def get(self, mint: str) -> TokenRecord | None:
        return self._tokens.get(mint)

    def all(self) -> list[TokenRecord]:
        return list(self._tokens.values())

    def restore(self, record: TokenRecord) -> None:
        """Bring back a persisted token the registry no longer holds in memory."""
        self._tokens.setdefault(record.mint, record)

    def forget(self, mint: str) -> None:
        self._tokens.pop(mint, None)

    def sweep(self, cutoff: datetime, *, keep: Collection[str] = ()) -> int:
        """Drop tokens untouched since ``cutoff``; the database keeps them.

        Nearly every Pump token is created, never migrates and is never seen
        again, so a month of them must not stay resident.
        """
        stale = [
            mint
            for mint, record in self._tokens.items()
            if record.updated_at < cutoff and mint not in keep
        ]
        for mint in stale:
            del self._tokens[mint]
        return len(stale)

    def apply_enrichment(
        self, mint: str, payload: dict[str, Any], observed_at: datetime
    ) -> TokenRecord | None:
        token = self._tokens.get(mint)
        if token is None:
            return None
        token = token.model_copy(
            update={
                "name": token.name or payload.get("name"),
                "symbol": token.symbol or payload.get("symbol"),
                "enrichment": payload,
                "enriched_at": observed_at,
                "updated_at": max(token.updated_at, observed_at),
            }
        )
        self._tokens[mint] = token
        return token

    def apply_mint_state(
        self,
        mint: str,
        *,
        token_program: str,
        decimals: int,
        total_supply_raw: Decimal,
        observed_at: datetime,
    ) -> TokenRecord | None:
        token = self._tokens.get(mint)
        if token is None:
            return None
        token = token.model_copy(
            update={
                "token_program": token_program,
                "decimals": decimals,
                "total_supply_raw": total_supply_raw,
                "updated_at": max(token.updated_at, observed_at),
            }
        )
        self._tokens[mint] = token
        return token

    def apply(self, event: EventEnvelope) -> TokenRecord | None:
        return self.apply_tracked(event)[0]

    def apply_tracked(self, event: EventEnvelope) -> tuple[TokenRecord | None, bool]:
        """Apply one event; the flag says whether a persisted field changed."""
        payload = event.payload
        if event.event_type == ChainEventType.TOKEN_CREATED and event.mint:
            record = TokenRecord(
                mint=event.mint,
                token_program=_text(payload.get("token_program")),
                name=_text(payload.get("name")),
                symbol=_text(payload.get("symbol")),
                total_supply_raw=_decimal_or_none(payload.get("token_total_supply")),
                creator_address=_text(payload.get("creator") or payload.get("user")),
                creation_signature=event.signature,
                creation_slot=event.slot,
                creation_time=event.block_time,
                metadata_uri=_text(payload.get("uri")),
                bonding_curve_address=_text(payload.get("bonding_curve")),
                updated_at=event.observed_at,
            )
            self._tokens[event.mint] = record
            return record, True
        if event.mint is None:
            return None, False
        token_record = self._tokens.get(event.mint)
        created = token_record is None
        if token_record is None:
            token_record = TokenRecord(mint=event.mint, updated_at=event.observed_at)
        update: dict[str, Any] = {"updated_at": event.observed_at}
        if event.event_type == ChainEventType.BONDING_CURVE_COMPLETED:
            update["bonding_curve_complete"] = True
        elif event.event_type == ChainEventType.MIGRATION:
            update["migration_time"] = event.block_time
            update["first_pool_address"] = _text(payload.get("pool"))
            update["first_pool_time"] = event.block_time
        elif event.event_type == ChainEventType.POOL_CREATED and not token_record.first_pool_address:
            update["first_pool_address"] = event.pool_address
            update["first_pool_time"] = event.block_time
        changed = created or any(
            getattr(token_record, key) != value
            for key, value in update.items()
            if key != "updated_at"
        )
        token_record = token_record.model_copy(update=update)
        self._tokens[event.mint] = token_record
        return token_record, changed


class PoolStateTracker:
    def __init__(self) -> None:
        self._pools: dict[str, PoolRecord] = {}
        self._states: dict[str, PoolState] = {}
        self._quote_prices: dict[str, QuoteAssetPrice] = {}

    def set_quote_price(self, price: QuoteAssetPrice) -> None:
        self._quote_prices[price.mint] = price

    def quote_price(self, mint: str) -> QuoteAssetPrice | None:
        return self._quote_prices.get(mint)

    def pool(self, pool_address: str) -> PoolRecord | None:
        return self._pools.get(pool_address)

    def state(self, pool_address: str) -> PoolState | None:
        return self._states.get(pool_address)

    def pools(self) -> list[PoolRecord]:
        return list(self._pools.values())

    def __len__(self) -> int:
        return len(self._pools)

    def forget(self, pool_address: str) -> None:
        self._pools.pop(pool_address, None)
        self._states.pop(pool_address, None)

    def sweep(self, cutoff: datetime, *, keep: Collection[str] = ()) -> list[str]:
        """Drop pools untouched since ``cutoff`` and return their addresses."""
        stale = [
            address
            for address, record in self._pools.items()
            if record.updated_at < cutoff and address not in keep
        ]
        for address in stale:
            self.forget(address)
        return stale

    def apply_base_supply(
        self,
        pool_address: str,
        *,
        total_supply_raw: Decimal,
    ) -> PoolState | None:
        state = self._states.get(pool_address)
        pool = self._pools.get(pool_address)
        if state is None or pool is None:
            return None
        market_cap = (
            total_supply_raw
            / (Decimal(10) ** pool.base_decimals)
            * state.marginal_price_usd
            if state.marginal_price_usd > 0
            else None
        )
        updated = state.model_copy(
            update={
                "base_supply_raw": total_supply_raw,
                "market_cap_estimate_usd": market_cap,
            }
        )
        self._states[pool_address] = updated
        return updated

    def apply(self, event: EventEnvelope) -> PoolState | None:
        pool_address = event.pool_address
        if not pool_address:
            return None
        payload = event.payload
        if event.event_type == ChainEventType.POOL_CREATED:
            source_base_mint = _required_text(payload, "base_mint")
            source_quote_mint = _required_text(payload, "quote_mint")
            reversed_orientation = (
                source_base_mint in SUPPORTED_QUOTE_MINTS
                and source_quote_mint not in SUPPORTED_QUOTE_MINTS
            )
            base_mint = source_quote_mint if reversed_orientation else source_base_mint
            quote_mint = source_base_mint if reversed_orientation else source_quote_mint
            record = PoolRecord(
                pool_address=pool_address,
                base_mint=base_mint,
                quote_mint=quote_mint,
                protocol=event.protocol.value,
                base_vault=_text(
                    payload.get("quote_vault")
                    if reversed_orientation
                    else payload.get("base_vault")
                ),
                quote_vault=_text(
                    payload.get("base_vault")
                    if reversed_orientation
                    else payload.get("quote_vault")
                ),
                creation_signature=event.signature,
                creation_slot=event.slot,
                creation_time=event.block_time,
                base_decimals=int(
                    payload[
                        "quote_mint_decimals"
                        if reversed_orientation
                        else "base_mint_decimals"
                    ]
                ),
                quote_decimals=int(
                    payload[
                        "base_mint_decimals"
                        if reversed_orientation
                        else "quote_mint_decimals"
                    ]
                ),
                source_orientation_reversed=reversed_orientation,
                updated_at=event.observed_at,
            )
            self._pools[pool_address] = record
        pool_record = self._pools.get(pool_address)
        if pool_record is None:
            return None

        raw_base, raw_quote = _reserve_fields(
            event.event_type,
            payload,
            reversed_orientation=pool_record.source_orientation_reversed,
        )
        previous = self._states.get(pool_address)
        if raw_base is None and previous is not None:
            raw_base = previous.raw_base_reserves
        if raw_quote is None and previous is not None:
            raw_quote = previous.raw_quote_reserves
        source_virtual_quote = _decimal_or_none(payload.get("virtual_quote_reserves"))
        virtual_base: Decimal | None
        virtual_quote: Decimal | None
        if pool_record.source_orientation_reversed:
            virtual_base = source_virtual_quote
            virtual_quote = Decimal("0")
        else:
            virtual_base = Decimal("0")
            virtual_quote = source_virtual_quote
        if virtual_base is None and previous is not None:
            virtual_base = previous.virtual_base_reserves
        if virtual_quote is None and previous is not None:
            virtual_quote = previous.virtual_quote_reserves
        raw_base = raw_base or Decimal("0")
        raw_quote = raw_quote or Decimal("0")
        virtual_base = virtual_base or Decimal("0")
        virtual_quote = virtual_quote or Decimal("0")
        effective_base = raw_base + virtual_base
        effective_quote = raw_quote + virtual_quote
        flags: list[str] = []
        if effective_base < 0:
            flags.append("NEGATIVE_EFFECTIVE_BASE_RESERVES")
        if effective_quote < 0:
            flags.append("NEGATIVE_EFFECTIVE_QUOTE_RESERVES")
        quote_price = self._quote_prices.get(pool_record.quote_mint)
        if quote_price is None:
            flags.append("QUOTE_PRICE_UNAVAILABLE")
            quote_usd = Decimal("0")
            price_time = datetime.fromtimestamp(0, tz=timezone.utc)
        else:
            quote_usd = quote_price.price_usd
            price_time = quote_price.observed_at
            if quote_price.is_stale(event.observed_at):
                flags.append("STALE_QUOTE_ASSET_PRICE")
        if pool_record.quote_mint not in SUPPORTED_QUOTE_MINTS:
            flags.append("UNSUPPORTED_QUOTE_MINT")

        normalized_base = max(effective_base, Decimal("0")) / (
            Decimal(10) ** pool_record.base_decimals
        )
        normalized_quote = max(effective_quote, Decimal("0")) / (
            Decimal(10) ** pool_record.quote_decimals
        )
        marginal_price = (
            normalized_quote / normalized_base * quote_usd
            if normalized_base > 0 and quote_usd > 0
            else Decimal("0")
        )
        quote_reserve_usd = normalized_quote * quote_usd
        base_reserve_usd = normalized_base * marginal_price
        base_supply = (
            None
            if pool_record.source_orientation_reversed
            else _decimal_or_none(payload.get("base_supply"))
        )
        if base_supply is None and previous is not None:
            base_supply = previous.base_supply_raw
        market_cap = None
        if base_supply is not None and marginal_price > 0:
            market_cap = (
                base_supply / (Decimal(10) ** pool_record.base_decimals) * marginal_price
            )
        last_trade = previous.last_trade_time if previous else None
        swap_fee_bps = previous.swap_fee_bps if previous else None
        if event.event_type in {ChainEventType.SWAP_BUY, ChainEventType.SWAP_SELL}:
            last_trade = event.block_time
            swap_fee_bps = _swap_fee_bps(payload) or swap_fee_bps
        state = PoolState(
            pool_address=pool_address,
            base_mint=pool_record.base_mint,
            quote_mint=pool_record.quote_mint,
            raw_base_reserves=raw_base,
            raw_quote_reserves=raw_quote,
            virtual_base_reserves=virtual_base,
            virtual_quote_reserves=virtual_quote,
            effective_base_reserves=effective_base,
            effective_quote_reserves=effective_quote,
            quote_reserve_usd=quote_reserve_usd,
            base_reserve_usd=base_reserve_usd,
            marginal_price_usd=marginal_price,
            market_cap_estimate_usd=market_cap,
            pool_age_seconds=max(
                Decimal("0"),
                Decimal(
                    str((event.block_time - pool_record.creation_time).total_seconds())
                ),
            ),
            last_trade_time=last_trade,
            last_update_time=event.block_time,
            quote_price_updated_at=price_time,
            base_supply_raw=base_supply,
            source_orientation_reversed=pool_record.source_orientation_reversed,
            data_quality_flags=flags,
            last_update_slot=max(event.slot, previous.last_update_slot if previous else 0),
            swap_fee_bps=swap_fee_bps,
        )
        self._states[pool_address] = state
        self._pools[pool_address] = pool_record.model_copy(
            update={"updated_at": event.observed_at}
        )
        return state


def _reserve_fields(
    event_type: ChainEventType,
    payload: dict[str, Any],
    *,
    reversed_orientation: bool,
) -> tuple[Decimal | None, Decimal | None]:
    if event_type == ChainEventType.POOL_CREATED:
        source_base = _decimal_or_none(payload.get("pool_base_amount"))
        source_quote = _decimal_or_none(payload.get("pool_quote_amount"))
    else:
        source_base = _decimal_or_none(payload.get("pool_base_token_reserves"))
        source_quote = _decimal_or_none(payload.get("pool_quote_token_reserves"))
    return (
        (source_quote, source_base)
        if reversed_orientation
        else (source_base, source_quote)
    )


def _swap_fee_bps(payload: dict[str, Any]) -> Decimal | None:
    parts = [
        _decimal_or_none(payload.get(key))
        for key in (
            "lp_fee_basis_points",
            "protocol_fee_basis_points",
            "coin_creator_fee_basis_points",
        )
    ]
    known = [part for part in parts if part is not None]
    return sum(known, Decimal("0")) if known else None


def reserve_sell_value_usd(
    state: PoolState,
    pool: PoolRecord,
    token_amount_raw: Decimal,
    *,
    quote_price_usd: Decimal,
) -> Decimal | None:
    """Constant-product proceeds of selling ``token_amount_raw`` into the pool.

    The executable value a PumpSwap sell would get right now from the tracked
    reserves, after the pool's swap fee, in USD. ``None`` when the reserves or
    the quote-asset price cannot support a mark.
    """
    base = state.effective_base_reserves
    quote = state.effective_quote_reserves
    if base <= 0 or quote <= 0 or token_amount_raw <= 0 or quote_price_usd <= 0:
        return None
    quote_out_raw = quote * token_amount_raw / (base + token_amount_raw)
    fee = (state.swap_fee_bps or Decimal("0")) / Decimal("10000")
    quote_out = quote_out_raw * (Decimal("1") - fee) / (Decimal(10) ** pool.quote_decimals)
    return quote_out * quote_price_usd


def pool_evidence(state: PoolState | None, pool: PoolRecord | None) -> dict[str, str] | None:
    """The tracked pool state a fill can be re-priced from later."""
    if state is None or pool is None:
        return None
    return {
        "slot": str(state.last_update_slot),
        "updated_at": state.last_update_time.isoformat(),
        "base_reserves": str(state.effective_base_reserves),
        "quote_reserves": str(state.effective_quote_reserves),
        "base_decimals": str(pool.base_decimals),
        "quote_decimals": str(pool.quote_decimals),
        "quote_price_usd": str(
            state.quote_reserve_usd
            / (state.effective_quote_reserves / (Decimal(10) ** pool.quote_decimals))
            if state.effective_quote_reserves > 0
            else Decimal("0")
        ),
        "swap_fee_bps": str(state.swap_fee_bps) if state.swap_fee_bps is not None else "",
    }


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def _text(value: object) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result or None


def _required_text(payload: dict[str, Any], key: str) -> str:
    value = _text(payload.get(key))
    if value is None:
        raise ValueError(f"pool event is missing {key}")
    return value
