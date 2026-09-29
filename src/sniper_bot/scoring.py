"""Explainable 0-100 scoring with category caps from the strategy specification."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field

from .config import ScoreAnchor, ScoringConfig
from .features import FeatureSnapshot


class DeveloperHistory(BaseModel):
    known: bool = False
    previous_rugs: int = 0
    previous_dev_dumps_5m: int = 0
    tokens_created_7d: int = 0
    successful_tokens: int = 0


class ScoreContext(BaseModel):
    features: FeatureSnapshot
    round_trip_loss_pct: Decimal = Field(ge=0)
    buy_price_impact_pct: Decimal = Field(ge=0)
    sell_price_impact_pct: Decimal = Field(ge=0)
    sell_route_reliability: Decimal = Field(ge=0, le=1)
    developer_history: DeveloperHistory = Field(default_factory=DeveloperHistory)
    vwap_reclaimed: bool = False


class ScoreBreakdown(BaseModel):
    total_score: Decimal = Field(ge=0, le=100)
    organic_score: Decimal = Field(ge=0, le=25)
    distribution_score: Decimal = Field(ge=0, le=20)
    execution_score: Decimal = Field(ge=0, le=20)
    liquidity_score: Decimal = Field(ge=0, le=15)
    developer_score: Decimal = Field(ge=0, le=10)
    price_structure_score: Decimal = Field(ge=0, le=10)
    explanations: dict[str, dict[str, str]]


class ScoringEngine:
    def __init__(self, config: ScoringConfig | None = None) -> None:
        self.config = config or ScoringConfig()

    def score(self, context: ScoreContext) -> ScoreBreakdown:
        f = context.features
        a = self.config
        organic_parts = {
            "unique_buyers_60s": _anchored(Decimal(f.unique_buyers_60s), a.unique_buyers_60s),
            "buyer_acceleration": _anchored(f.buyer_acceleration, a.buyer_acceleration),
            "unique_buyer_ratio": _anchored(f.unique_buyer_ratio, a.unique_buyer_ratio),
            "buy_sell_volume_ratio": _anchored(f.buy_sell_volume_ratio, a.buy_sell_volume_ratio),
            "transactions_per_trader": _anchored(f.transactions_per_trader, a.transactions_per_trader),
        }
        distribution_parts = {
            "top_10_holders": _anchored(f.top_10_holders_pct, a.top_10_holders),
            "largest_related_cluster": _anchored(f.largest_related_cluster_pct, a.largest_related_cluster),
            "dev_cluster": _anchored(f.dev_cluster_holding_pct, a.dev_cluster),
            "top_5_buyers_share": _anchored(f.top_5_buyer_volume_share, a.top_5_buyers_share),
        }
        execution_parts = {
            "round_trip_loss": _anchored(context.round_trip_loss_pct, a.round_trip_loss),
            "price_impact": _anchored(
                max(context.buy_price_impact_pct, context.sell_price_impact_pct),
                a.price_impact,
            ),
            "sell_route_reliability": context.sell_route_reliability * a.sell_route_points,
        }
        liquidity_parts = {
            "quote_liquidity": _anchored(f.quote_liquidity_usd, a.quote_liquidity),
            "liquidity_stability": _anchored(f.quote_liquidity_change_30s, a.liquidity_stability),
            "market_cap_to_liquidity": _anchored(f.market_cap_to_quote_liquidity, a.market_cap_to_liquidity),
        }
        developer_parts = {"history": _developer_score(context.developer_history)}
        price_parts = {
            "controlled_pullback": (
                a.pullback_points
                if a.pullback_min <= f.drawdown_from_local_high <= a.pullback_max
                else Decimal("0")
            ),
            "vwap_reclaim": a.vwap_reclaim_points if context.vwap_reclaimed else Decimal("0"),
            "not_overextended": (
                a.overextension_points
                if f.return_since_pool_creation <= a.overextension_max_return
                else Decimal("0")
            ),
        }
        organic = _sum_cap(organic_parts, Decimal("25"))
        distribution = _sum_cap(distribution_parts, Decimal("20"))
        execution = _sum_cap(execution_parts, Decimal("20"))
        liquidity = _sum_cap(liquidity_parts, Decimal("15"))
        developer = _sum_cap(developer_parts, Decimal("10"))
        price = _sum_cap(price_parts, Decimal("10"))
        total = organic + distribution + execution + liquidity + developer + price
        return ScoreBreakdown(
            total_score=_q(total),
            organic_score=_q(organic),
            distribution_score=_q(distribution),
            execution_score=_q(execution),
            liquidity_score=_q(liquidity),
            developer_score=_q(developer),
            price_structure_score=_q(price),
            explanations={
                "organic": _explain(organic_parts),
                "distribution": _explain(distribution_parts),
                "execution": _explain(execution_parts),
                "liquidity": _explain(liquidity_parts),
                "developer": _explain(developer_parts),
                "price_structure": _explain(price_parts),
            },
        )


def _developer_score(history: DeveloperHistory) -> Decimal:
    if not history.known:
        return Decimal("5")
    if history.previous_rugs >= 2:
        return Decimal("0")
    score = Decimal("5")
    score += min(Decimal(history.successful_tokens), Decimal("3"))
    score -= min(Decimal(history.previous_dev_dumps_5m * 2), Decimal("4"))
    if history.tokens_created_7d >= 10:
        score -= Decimal("3")
    return _clamp(score, Decimal("0"), Decimal("10"))


def _anchored(value: Decimal, anchor: ScoreAnchor) -> Decimal:
    if anchor.full > anchor.zero:
        return _linear(value, anchor.zero, anchor.full, anchor.points)
    return _inverse(value, anchor.full, anchor.zero, anchor.points)


def _linear(value: Decimal, low: Decimal, high: Decimal, maximum: Decimal) -> Decimal:
    if high <= low:
        raise ValueError("linear score high must exceed low")
    return _clamp((value - low) / (high - low) * maximum, Decimal("0"), maximum)


def _inverse(value: Decimal, good: Decimal, bad: Decimal, maximum: Decimal) -> Decimal:
    if bad <= good:
        raise ValueError("inverse score bad must exceed good")
    return _clamp((bad - value) / (bad - good) * maximum, Decimal("0"), maximum)


def _clamp(value: Decimal, low: Decimal, high: Decimal) -> Decimal:
    return max(low, min(value, high))


def _sum_cap(parts: dict[str, Decimal], maximum: Decimal) -> Decimal:
    return min(sum(parts.values(), Decimal("0")), maximum)


def _q(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"))


def _explain(parts: dict[str, Decimal]) -> dict[str, str]:
    return {name: str(_q(value)) for name, value in parts.items()}
