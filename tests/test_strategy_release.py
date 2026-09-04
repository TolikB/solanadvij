from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from sniper_bot.registry import SUPPORTED_QUOTE_MINTS
from sniper_bot.security import (
    SPL_TOKEN_PROGRAM_ID,
    ExecutionChecks,
    HolderMetrics,
    MintInfo,
    RejectReason,
    SecurityContext,
    SecurityEngine,
)


def _valid_context(now: datetime) -> SecurityContext:
    return SecurityContext(
        mint=MintInfo(
            mint="TOKEN",
            token_program=SPL_TOKEN_PROGRAM_ID,
            decimals=6,
            total_supply_raw=Decimal("1000000"),
            observed_at=now,
        ),
        holders=HolderMetrics(
            largest_holder_pct=Decimal("0.01"),
            top_5_holders_pct=Decimal("0.05"),
            top_10_holders_pct=Decimal("0.10"),
            dev_holding_pct=Decimal("0.01"),
            dev_cluster_holding_pct=Decimal("0.02"),
            related_cluster_holding_pct=Decimal("0.03"),
            unknown_owner_supply_pct=Decimal("0"),
            holder_count=100,
        ),
        holders_observed_at=now,
        execution=ExecutionChecks(
            buy_route_available=True,
            sell_route_available=True,
            round_trip_loss_pct=Decimal("0.01"),
            buy_price_impact_pct=Decimal("0.01"),
            sell_price_impact_pct=Decimal("0.01"),
            quote_observed_at=now,
        ),
        quote_mint=next(iter(SUPPORTED_QUOTE_MINTS)),
        quote_liquidity_usd=Decimal("100000"),
        liquidity_change_30s=Decimal("0"),
        pool_age_seconds=Decimal("90"),
        external_successful_sellers=10,
        stream_observed_at=now,
        developer_history_known=True,
    )


@pytest.mark.parametrize(
    ("updates", "reason"),
    [
        ({"dev_sold": True}, RejectReason.DEV_SOLD),
        (
            {"liquidity_change_30s": Decimal("-0.04")},
            RejectReason.LIQUIDITY_DECLINING,
        ),
        ({"wash_trading_pattern": True}, RejectReason.WASH_TRADING_PATTERN),
        (
            {"return_since_pool_creation": Decimal("3")},
            RejectReason.OVEREXTENDED_PRICE,
        ),
    ],
)
def test_release_strategy_rejects_adversarial_scenarios(
    updates: dict[str, object],
    reason: RejectReason,
) -> None:
    now = datetime(2026, 9, 1, tzinfo=timezone.utc)
    context = _valid_context(now).model_copy(update=updates)

    result = SecurityEngine().evaluate(context, now=now)

    assert result.hard_reject is True
    assert reason in result.reject_reasons