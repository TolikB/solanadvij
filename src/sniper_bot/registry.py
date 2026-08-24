"""Token registry and executable PumpSwap pool-state calculations."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field

from .events import ChainEventType, EventEnvelope

WSOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
SUPPORTED_QUOTE_MINTS = frozenset({WSOL_MINT, USDC_MINT})


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
    updated_at: datetime


class PoolState(BaseModel):
    pool_address: str
    base_mint: str
    quote_mint: str
    raw_base_reserves: Decimal = Decimal("0")
    raw_quote_reserves: Decimal = Decimal("0")
    virtual_quote_reserves: Decimal = Decimal("0")
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
    data_quality_flags: list[str] = Field(default_factory=list)


class QuoteAssetPrice(BaseModel):
    mint: str
    price_usd: Decimal = Field(gt=0)
    observed_at: datetime

    def is_stale(self, now: datetime, max_age_seconds: Decimal = Decimal("15")) -> bool:
        return Decimal(str((now - self.observed_at).total_seconds())) > max_age_seconds


class TokenRegistry:
    def __init__(self) -> None:
        self._tokens: dict[str, TokenRecord] = {}

    def get(self, mint: str) -> TokenRecord | None:
        return self._tokens.get(mint)

    def all(self) -> list[TokenRecord]:
        return list(self._tokens.values())

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
            return record
        if event.mint is None:
            return None
        token_record = self._tokens.get(event.mint)
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
        token_record = token_record.model_copy(update=update)
        self._tokens[event.mint] = token_record
        return token_record


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

    def apply(self, event: EventEnvelope) -> PoolState | None:
        pool_address = event.pool_address
        if not pool_address:
            return None
        payload = event.payload
        if event.event_type == ChainEventType.POOL_CREATED:
            base_mint = _required_text(payload, "base_mint")
            quote_mint = _required_text(payload, "quote_mint")
            record = PoolRecord(
                pool_address=pool_address,
                base_mint=base_mint,
                quote_mint=quote_mint,
                protocol=event.protocol.value,
                base_vault=_text(payload.get("base_vault")),
                quote_vault=_text(payload.get("quote_vault")),
                creation_signature=event.signature,
                creation_slot=event.slot,
                creation_time=event.block_time,
                base_decimals=int(payload["base_mint_decimals"]),
                quote_decimals=int(payload["quote_mint_decimals"]),
                updated_at=event.observed_at,
            )
            self._pools[pool_address] = record
        pool_record = self._pools.get(pool_address)
        if pool_record is None:
            return None

        raw_base, raw_quote = _reserve_fields(event.event_type, payload)
        previous = self._states.get(pool_address)
        if raw_base is None and previous is not None:
            raw_base = previous.raw_base_reserves
        if raw_quote is None and previous is not None:
            raw_quote = previous.raw_quote_reserves
        virtual_quote = _decimal_or_none(payload.get("virtual_quote_reserves"))
        if virtual_quote is None and previous is not None:
            virtual_quote = previous.virtual_quote_reserves
        raw_base = raw_base or Decimal("0")
        raw_quote = raw_quote or Decimal("0")
        virtual_quote = virtual_quote or Decimal("0")
        effective_quote = raw_quote + virtual_quote
        flags: list[str] = []
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

        normalized_base = raw_base / (Decimal(10) ** pool_record.base_decimals)
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
        base_supply = _decimal_or_none(payload.get("base_supply"))
        if base_supply is None and previous is not None:
            base_supply = previous.base_supply_raw
        market_cap = None
        if base_supply is not None and marginal_price > 0:
            market_cap = (
                base_supply / (Decimal(10) ** pool_record.base_decimals) * marginal_price
            )
        last_trade = previous.last_trade_time if previous else None
        if event.event_type in {ChainEventType.SWAP_BUY, ChainEventType.SWAP_SELL}:
            last_trade = event.block_time
        state = PoolState(
            pool_address=pool_address,
            base_mint=pool_record.base_mint,
            quote_mint=pool_record.quote_mint,
            raw_base_reserves=raw_base,
            raw_quote_reserves=raw_quote,
            virtual_quote_reserves=virtual_quote,
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
            data_quality_flags=flags,
        )
        self._states[pool_address] = state
        self._pools[pool_address] = pool_record.model_copy(
            update={"updated_at": event.observed_at}
        )
        return state


def _reserve_fields(
    event_type: ChainEventType, payload: dict[str, Any]
) -> tuple[Decimal | None, Decimal | None]:
    if event_type == ChainEventType.POOL_CREATED:
        return (
            _decimal_or_none(payload.get("pool_base_amount")),
            _decimal_or_none(payload.get("pool_quote_amount")),
        )
    return (
        _decimal_or_none(payload.get("pool_base_token_reserves")),
        _decimal_or_none(payload.get("pool_quote_token_reserves")),
    )


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
