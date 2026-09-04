"""Domain models for quotes, fills and positions."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class QuoteDirection(StrEnum):
    BUY = "buy"
    SELL = "sell"


class PositionStatus(StrEnum):
    OPEN = "open"
    CLOSED = "closed"


class FillType(StrEnum):
    ENTRY = "entry"
    EXIT = "exit"


class QuoteResponse(BaseModel):
    request_id: str = ""
    requested_at: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))
    received_at: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))
    latency_ms: int = 0
    token_in: str = Field(..., description="Input mint used by Jupiter")
    token_out: str = Field(..., description="Output mint returned by Jupiter")
    in_amount: Decimal
    out_amount: Decimal
    in_amount_usd: Decimal | None = None
    out_amount_usd: Decimal | None = None
    route: dict[str, Any] = Field(default_factory=dict)
    price_impact_pct: Decimal = Decimal("0")
    platform_fee_usd: Decimal = Field(default=Decimal("0"), ge=0)
    estimated_network_fee_usd: Decimal = Field(default=Decimal("0"), ge=0)
    expires_at: datetime | None = None
    raw: dict[str, Any] = Field(default_factory=dict)

    def is_stale(self, now: datetime | None = None) -> bool:
        current = now or datetime.now(tz=timezone.utc)
        return self.expires_at is not None and self.expires_at <= current


class RoundTripQuote(BaseModel):
    buy: QuoteResponse
    sell: QuoteResponse
    starting_usd: Decimal
    ending_usd: Decimal
    loss_pct: Decimal


class FillRecord(BaseModel):
    fill_id: str
    position_id: str | None = None
    token_mint: str
    order_id: str
    quote_id: str
    fill_type: FillType
    token_amount: Decimal
    usd_notional: Decimal
    cost_basis_usd: Decimal
    created_at: datetime
    exit_reason: str | None = None
    price_impact_pct: Decimal = Decimal("0")
    platform_fee_usd: Decimal = Field(default=Decimal("0"), ge=0)
    network_fee_usd: Decimal = Field(default=Decimal("0"), ge=0)
    other_cost_usd: Decimal = Field(default=Decimal("0"), ge=0)
    adverse_fill_bps: int = Field(default=0, ge=0, lt=10_000)
    quote_latency_ms: int = Field(default=0, ge=0)
    round_trip_cost_pct: Decimal | None = None


class PositionRecord(BaseModel):
    position_id: str
    token_mint: str
    open_fill_id: str
    entry_token_amount: Decimal
    entry_cost_usd: Decimal
    open_ratio: Decimal
    opened_at: datetime
    closed_at: datetime | None = None
    locked_usd: Decimal
    status: PositionStatus = PositionStatus.OPEN

    remaining_token_amount: Decimal
    remaining_cost_usd: Decimal

    realized_pnl_usd: Decimal = Decimal("0")
    peak_unrealized_usd: Decimal = Decimal("0")
    mfe_pct: Decimal = Decimal("0")
    mae_pct: Decimal = Decimal("0")
    highest_executable_value_usd: Decimal = Decimal("0")
    lowest_executable_value_usd: Decimal | None = None
    last_executable_value_usd: Decimal = Decimal("0")
    tp1_taken: bool = False
    tp2_taken: bool = False
    last_new_high_at: datetime | None = None
    final_exit_reason: str | None = None
    candidate_id: str | None = None
    pool_address: str | None = None
    entry_score: Decimal | None = None
    entry_liquidity_usd: Decimal | None = None
    entry_pool_age_seconds: Decimal | None = None
    strategy_version: str | None = None
