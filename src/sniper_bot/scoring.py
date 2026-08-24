"""Explainable 0-100 scoring with category caps from the strategy specification."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field

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
    def score(self, context: ScoreContext) -> ScoreBreakdown:
        f = context.features
        organic_parts = {
            "unique_buyers_60s": _linear(Decimal(f.unique_buyers_60s), Decimal("5"), Decimal("25"), Decimal("8")),
            "buyer_acceleration": _linear(f.buyer_acceleration, Decimal("0.8"), Decimal("1.5"), Decimal("6")),
            "unique_buyer_ratio": _linear(f.unique_buyer_ratio, Decimal("0.2"), Decimal("0.75"), Decimal("4")),
            "buy_sell_volume_ratio": _linear(f.buy_sell_volume_ratio, Decimal("1"), Decimal("3"), Decimal("4")),
            "transactions_per_trader": _inverse(f.transactions_per_trader, Decimal("2"), Decimal("6"), Decimal("3")),
        }
        distribution_parts = {
            "top_10_holders": _inverse(f.top_10_holders_pct, Decimal("0.15"), Decimal("0.30"), Decimal("6")),
            "largest_related_cluster": _inverse(f.largest_related_cluster_pct, Decimal("0.05"), Decimal("0.15"), Decimal("6")),
            "dev_cluster": _inverse(f.dev_cluster_holding_pct, Decimal("0.02"), Decimal("0.05"), Decimal("4")),
            "top_5_buyers_share": _inverse(f.top_5_buyer_volume_share, Decimal("0.35"), Decimal("0.70"), Decimal("4")),
        }
        execution_parts = {
            "round_trip_loss": _inverse(context.round_trip_loss_pct, Decimal("0.03"), Decimal("0.08"), Decimal("8")),
            "price_impact": _inverse(
                max(context.buy_price_impact_pct, context.sell_price_impact_pct),
                Decimal("0.01"),
                Decimal("0.035"),
                Decimal("6"),
            ),
            "sell_route_reliability": context.sell_route_reliability * Decimal("6"),
        }
        liquidity_parts = {
            "quote_liquidity": _linear(f.quote_liquidity_usd, Decimal("40000"), Decimal("100000"), Decimal("5")),
            "liquidity_stability": _linear(f.quote_liquidity_change_30s, Decimal("-0.03"), Decimal("0"), Decimal("6")),
            "market_cap_to_liquidity": _inverse(f.market_cap_to_quote_liquidity, Decimal("5"), Decimal("15"), Decimal("4")),
        }
        developer_parts = {"history": _developer_score(context.developer_history)}
        price_parts = {
            "controlled_pullback": Decimal("4") if Decimal("0.10") <= f.drawdown_from_local_high <= Decimal("0.25") else Decimal("0"),
            "vwap_reclaim": Decimal("4") if context.vwap_reclaimed else Decimal("0"),
            "not_overextended": Decimal("2") if f.return_since_pool_creation <= Decimal("2.5") else Decimal("0"),
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
