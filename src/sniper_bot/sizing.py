"""Risk-budget position sizing for the fixed 500 USDC paper account."""

from __future__ import annotations

from decimal import ROUND_DOWN, Decimal
from enum import StrEnum

from pydantic import BaseModel, Field


class SizingRejectReason(StrEnum):
    POSITION_TOO_SMALL_AFTER_COSTS = "POSITION_TOO_SMALL_AFTER_COSTS"
    NO_DAILY_RISK_BUDGET = "NO_DAILY_RISK_BUDGET"
    INVALID_EFFECTIVE_LOSS = "INVALID_EFFECTIVE_LOSS"


class PositionSizingInput(BaseModel):
    current_equity_usd: Decimal = Field(gt=0)
    daily_pnl_usd: Decimal
    quote_liquidity_usd: Decimal = Field(gt=0)
    estimated_round_trip_cost_pct: Decimal = Field(ge=0)
    score: Decimal = Field(ge=0, le=100)
    hard_stop_pct: Decimal = Decimal("0.15")
    adverse_execution_buffer_pct: Decimal = Decimal("0.01")
    daily_loss_limit_usd: Decimal = Decimal("10")
    maximum_position_usd: Decimal = Decimal("20")
    minimum_position_usd: Decimal = Decimal("8")


class PositionSizingResult(BaseModel):
    allowed: bool
    position_size_usd: Decimal
    risk_budget_usd: Decimal
    effective_loss_pct: Decimal
    score_multiplier: Decimal
    reject_reason: SizingRejectReason | None = None


def calculate_position_size(value: PositionSizingInput) -> PositionSizingResult:
    remaining_daily = max(
        Decimal("0"),
        value.daily_loss_limit_usd + min(value.daily_pnl_usd, Decimal("0")),
    )
    risk_budget = min(value.current_equity_usd * Decimal("0.005"), remaining_daily)
    effective_loss = (
        value.hard_stop_pct
        + value.estimated_round_trip_cost_pct
        + value.adverse_execution_buffer_pct
    )
    multiplier = _score_multiplier(value.score)
    if risk_budget <= 0:
        return PositionSizingResult(
            allowed=False,
            position_size_usd=Decimal("0"),
            risk_budget_usd=risk_budget,
            effective_loss_pct=effective_loss,
            score_multiplier=multiplier,
            reject_reason=SizingRejectReason.NO_DAILY_RISK_BUDGET,
        )
    if effective_loss <= 0:
        return PositionSizingResult(
            allowed=False,
            position_size_usd=Decimal("0"),
            risk_budget_usd=risk_budget,
            effective_loss_pct=effective_loss,
            score_multiplier=multiplier,
            reject_reason=SizingRejectReason.INVALID_EFFECTIVE_LOSS,
        )
    size = min(
        risk_budget / effective_loss,
        value.current_equity_usd * Decimal("0.04"),
        value.quote_liquidity_usd * Decimal("0.0025"),
        value.maximum_position_usd,
    ) * multiplier
    size = size.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    if size < value.minimum_position_usd:
        return PositionSizingResult(
            allowed=False,
            position_size_usd=size,
            risk_budget_usd=risk_budget,
            effective_loss_pct=effective_loss,
            score_multiplier=multiplier,
            reject_reason=SizingRejectReason.POSITION_TOO_SMALL_AFTER_COSTS,
        )
    return PositionSizingResult(
        allowed=True,
        position_size_usd=size,
        risk_budget_usd=risk_budget,
        effective_loss_pct=effective_loss,
        score_multiplier=multiplier,
    )


def _score_multiplier(score: Decimal) -> Decimal:
    if score >= Decimal("92"):
        return Decimal("1")
    if score >= Decimal("85"):
        return Decimal("0.90")
    if score >= Decimal("80"):
        return Decimal("0.75")
    return Decimal("0")
