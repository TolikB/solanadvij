from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from solders.pubkey import Pubkey

from sniper_bot.events import ChainEventType, EventEnvelope, EventSource, Protocol
from sniper_bot.protocols.pumpswap.decoder import PUMPSWAP_PROGRAM_ID
from sniper_bot.registry import WSOL_MINT, PoolStateTracker, QuoteAssetPrice
from sniper_bot.security import (
    SPL_TOKEN_PROGRAM_ID,
    TOKEN_2022_PROGRAM_ID,
    ExecutionChecks,
    HolderBalance,
    HolderMetrics,
    MintInfo,
    RejectReason,
    SecurityContext,
    SecurityEngine,
    aggregate_holders,
)


def _now() -> datetime:
    return datetime(2026, 8, 24, 12, 1, tzinfo=timezone.utc)


def test_pumpswap_effective_quote_reserves_include_virtual_value() -> None:
    now = _now()
    tracker = PoolStateTracker()
    tracker.set_quote_price(QuoteAssetPrice(mint=WSOL_MINT, price_usd=Decimal("200"), observed_at=now))
    event = EventEnvelope(
        source=EventSource.REPLAY,
        protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.POOL_CREATED,
        slot=1,
        signature="sig",
        instruction_index=1,
        block_time=now,
        observed_at=now,
        mint="TOKEN",
        pool_address="POOL",
        payload={
            "base_mint": "TOKEN",
            "quote_mint": WSOL_MINT,
            "base_mint_decimals": 6,
            "quote_mint_decimals": 9,
            "pool_base_amount": 100_000_000,
            "pool_quote_amount": 200_000_000_000,
            "virtual_quote_reserves": 50_000_000_000,
        },
    )

    state = tracker.apply(event)

    assert state is not None
    assert state.effective_quote_reserves == Decimal("250000000000")
    assert state.quote_reserve_usd == Decimal("50000")
    assert state.marginal_price_usd == Decimal("500")


def test_holder_balances_are_aggregated_by_owner_and_system_accounts_excluded() -> None:
    metrics = aggregate_holders(
        [
            HolderBalance(token_account="a1", owner="owner-a", amount_raw=Decimal("100")),
            HolderBalance(token_account="a2", owner="owner-a", amount_raw=Decimal("50")),
            HolderBalance(token_account="b", owner="owner-b", amount_raw=Decimal("80")),
            HolderBalance(token_account="vault", owner="pool", amount_raw=Decimal("500")),
            HolderBalance(token_account="unknown", owner=None, amount_raw=Decimal("20")),
        ],
        total_supply_raw=Decimal("1000"),
        dev_wallet="owner-b",
        system_addresses={"pool", "vault"},
    )

    assert metrics.holder_count == 2
    assert metrics.largest_holder_pct == Decimal("0.15")
    assert metrics.dev_holding_pct == Decimal("0.08")
    assert metrics.unknown_owner_supply_pct == Decimal("0.02")


def test_off_curve_program_owner_is_excluded_from_holder_concentration() -> None:
    program = Pubkey.from_string(PUMPSWAP_PROGRAM_ID)
    program_owner, _ = Pubkey.find_program_address([b"fee-vault"], program)
    metrics = aggregate_holders(
        [
            HolderBalance(
                token_account="program-vault",
                owner=str(program_owner),
                amount_raw=Decimal("900"),
            ),
            HolderBalance(
                token_account="user-account",
                owner="user-owner",
                amount_raw=Decimal("100"),
            ),
        ],
        total_supply_raw=Decimal("1000"),
    )

    assert metrics.holder_count == 1
    assert metrics.largest_holder_pct == Decimal("0.1")


def _safe_context(now: datetime) -> SecurityContext:
    return SecurityContext(
        mint=MintInfo(
            mint="TOKEN",
            token_program=SPL_TOKEN_PROGRAM_ID,
            decimals=6,
            total_supply_raw=Decimal("1000000"),
            observed_at=now,
        ),
        holders=HolderMetrics(
            largest_holder_pct=Decimal("0.05"),
            top_5_holders_pct=Decimal("0.18"),
            top_10_holders_pct=Decimal("0.25"),
            dev_holding_pct=Decimal("0.01"),
            dev_cluster_holding_pct=Decimal("0.03"),
            related_cluster_holding_pct=Decimal("0.10"),
            unknown_owner_supply_pct=Decimal("0.01"),
            holder_count=30,
        ),
        holders_observed_at=now,
        execution=ExecutionChecks(
            buy_route_available=True,
            sell_route_available=True,
            round_trip_loss_pct=Decimal("0.05"),
            buy_price_impact_pct=Decimal("0.01"),
            sell_price_impact_pct=Decimal("0.02"),
            quote_observed_at=now,
        ),
        quote_mint=WSOL_MINT,
        quote_liquidity_usd=Decimal("50000"),
        liquidity_change_30s=Decimal("0"),
        pool_age_seconds=Decimal("60"),
        external_successful_sellers=5,
        stream_observed_at=now,
    )


def test_security_engine_accepts_complete_safe_context() -> None:
    now = _now()
    result = SecurityEngine(maximum_pool_age_seconds=Decimal("180")).evaluate(_safe_context(now), now=now)
    assert result.hard_reject is False
    assert result.reject_reasons == []


def test_token_2022_and_stale_data_are_always_rejected() -> None:
    now = _now()
    context = _safe_context(now).model_copy(deep=True)
    context.mint.token_program = TOKEN_2022_PROGRAM_ID
    context.stream_observed_at = now - timedelta(seconds=4)

    result = SecurityEngine().evaluate(context, now=now)

    assert RejectReason.TOKEN_2022 in result.reject_reasons
    assert RejectReason.STALE_DATA in result.reject_reasons
