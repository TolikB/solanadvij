"""Fail-closed token, holder, execution, and data-quality checks."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, Field
from solders.pubkey import Pubkey

from .registry import SUPPORTED_QUOTE_MINTS

SPL_TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
BURN_AND_SERVICE_ADDRESSES = frozenset(
    {
        "11111111111111111111111111111111",
        "1nc1nerator11111111111111111111111111111111",
        "SysvarRent111111111111111111111111111111111",
    }
)


class RejectReason(StrEnum):
    INVALID_TOKEN_PROGRAM = "INVALID_TOKEN_PROGRAM"
    MINT_AUTHORITY_ACTIVE = "MINT_AUTHORITY_ACTIVE"
    FREEZE_AUTHORITY_ACTIVE = "FREEZE_AUTHORITY_ACTIVE"
    TOKEN_2022 = "TOKEN_2022"
    INVALID_SUPPLY = "INVALID_SUPPLY"
    SUPPLY_CHANGED = "SUPPLY_CHANGED"
    INVALID_DECIMALS = "INVALID_DECIMALS"
    UNSUPPORTED_QUOTE_MINT = "UNSUPPORTED_QUOTE_MINT"
    LOW_QUOTE_LIQUIDITY = "LOW_QUOTE_LIQUIDITY"
    NO_BUY_ROUTE = "NO_BUY_ROUTE"
    NO_SELL_ROUTE = "NO_SELL_ROUTE"
    ROUND_TRIP_TOO_EXPENSIVE = "ROUND_TRIP_TOO_EXPENSIVE"
    HIGH_BUY_PRICE_IMPACT = "HIGH_BUY_PRICE_IMPACT"
    HIGH_SELL_PRICE_IMPACT = "HIGH_SELL_PRICE_IMPACT"
    INSUFFICIENT_EXTERNAL_SELLERS = "INSUFFICIENT_EXTERNAL_SELLERS"
    HIGH_HOLDER_CONCENTRATION = "HIGH_HOLDER_CONCENTRATION"
    DEV_CLUSTER_TOO_LARGE = "DEV_CLUSTER_TOO_LARGE"
    RELATED_CLUSTER_TOO_LARGE = "RELATED_CLUSTER_TOO_LARGE"
    DEV_HOLDING_TOO_LARGE = "DEV_HOLDING_TOO_LARGE"
    UNKNOWN_OWNER_SUPPLY = "UNKNOWN_OWNER_SUPPLY"
    HOLDER_DATA_UNAVAILABLE = "HOLDER_DATA_UNAVAILABLE"
    HOLDER_DATA_STALE = "HOLDER_DATA_STALE"
    DEV_SOLD = "DEV_SOLD"
    POOL_TOO_OLD = "POOL_TOO_OLD"
    POOL_TOO_NEW = "POOL_TOO_NEW"
    LIQUIDITY_DECLINING = "LIQUIDITY_DECLINING"
    STALE_DATA = "STALE_DATA"
    STALE_QUOTE = "STALE_QUOTE"
    UNKNOWN_PROTOCOL_LAYOUT = "UNKNOWN_PROTOCOL_LAYOUT"
    API_UNAVAILABLE = "API_UNAVAILABLE"
    PREVIOUS_RUGS = "PREVIOUS_RUGS"
    WASH_TRADING_PATTERN = "WASH_TRADING_PATTERN"
    OVEREXTENDED_PRICE = "OVEREXTENDED_PRICE"
    SCORE_TOO_LOW = "SCORE_TOO_LOW"
    ENTRY_WINDOW_EXPIRED = "ENTRY_WINDOW_EXPIRED"
    DAILY_RISK_LIMIT = "DAILY_RISK_LIMIT"
    MAX_OPEN_POSITIONS = "MAX_OPEN_POSITIONS"
    POSITION_TOO_SMALL_AFTER_COSTS = "POSITION_TOO_SMALL_AFTER_COSTS"
    RISK_MANAGER_BLOCKED = "RISK_MANAGER_BLOCKED"


class MintInfo(BaseModel):
    mint: str
    token_program: str
    decimals: int
    total_supply_raw: Decimal
    mint_authority: str | None = None
    freeze_authority: str | None = None
    observed_at: datetime
    supply_changed: bool = False


class HolderBalance(BaseModel):
    token_account: str
    owner: str | None
    amount_raw: Decimal = Field(ge=0)
    is_system: bool = False


class HolderMetrics(BaseModel):
    largest_holder_pct: Decimal = Decimal("0")
    top_5_holders_pct: Decimal = Decimal("0")
    top_10_holders_pct: Decimal = Decimal("0")
    dev_holding_pct: Decimal = Decimal("0")
    dev_cluster_holding_pct: Decimal = Decimal("0")
    early_buyers_holding_pct: Decimal = Decimal("0")
    related_cluster_holding_pct: Decimal = Decimal("0")
    unknown_owner_supply_pct: Decimal = Decimal("0")
    holder_count: int = 0


class ExecutionChecks(BaseModel):
    buy_route_available: bool
    sell_route_available: bool
    round_trip_loss_pct: Decimal
    buy_price_impact_pct: Decimal
    sell_price_impact_pct: Decimal
    quote_observed_at: datetime


class SecurityContext(BaseModel):
    mint: MintInfo
    holders: HolderMetrics | None
    holders_observed_at: datetime | None
    execution: ExecutionChecks
    quote_mint: str
    quote_liquidity_usd: Decimal
    liquidity_change_30s: Decimal
    pool_age_seconds: Decimal
    external_successful_sellers: int
    stream_observed_at: datetime
    dev_sold: bool = False
    previous_rugs: int = 0
    previous_dev_dumps_5m: int = 0
    developer_tokens_created_7d: int = 0
    developer_successful_tokens: int = 0
    developer_history_known: bool = False
    protocol_layout_known: bool = True
    critical_api_available: bool = True
    wash_trading_pattern: bool = False
    return_since_pool_creation: Decimal = Decimal("0")


class SecurityResult(BaseModel):
    checked_at: datetime
    hard_reject: bool
    reject_reasons: list[RejectReason]
    holder_metrics: HolderMetrics | None = None


def aggregate_holders(
    holders: list[HolderBalance],
    *,
    total_supply_raw: Decimal,
    dev_wallet: str | None = None,
    dev_cluster: set[str] | None = None,
    related_cluster: set[str] | None = None,
    early_buyers: set[str] | None = None,
    system_addresses: set[str] | None = None,
) -> HolderMetrics:
    if total_supply_raw <= 0:
        raise ValueError("total supply must be positive")
    excluded = system_addresses or set()
    balances: dict[str, Decimal] = {}
    unknown = Decimal("0")
    for account in holders:
        if (
            account.is_system
            or account.token_account in excluded
            or account.owner in excluded
            or _is_program_or_burn_owner(account.owner)
        ):
            continue
        if account.owner is None:
            unknown += account.amount_raw
            continue
        balances[account.owner] = balances.get(account.owner, Decimal("0")) + account.amount_raw
    denominator = total_supply_raw
    ranked = sorted(balances.values(), reverse=True)

    def share(amount: Decimal) -> Decimal:
        return amount / denominator if denominator > 0 else Decimal("0")

    def wallet_share(wallets: set[str] | None) -> Decimal:
        return share(sum((balances.get(wallet, Decimal("0")) for wallet in wallets or set()), Decimal("0")))

    return HolderMetrics(
        largest_holder_pct=share(ranked[0]) if ranked else Decimal("0"),
        top_5_holders_pct=share(sum(ranked[:5], Decimal("0"))),
        top_10_holders_pct=share(sum(ranked[:10], Decimal("0"))),
        dev_holding_pct=share(balances.get(dev_wallet, Decimal("0"))) if dev_wallet else Decimal("0"),
        dev_cluster_holding_pct=wallet_share(dev_cluster),
        early_buyers_holding_pct=wallet_share(early_buyers),
        related_cluster_holding_pct=wallet_share(related_cluster),
        unknown_owner_supply_pct=share(unknown),
        holder_count=len([amount for amount in balances.values() if amount > 0]),
    )


def _is_program_or_burn_owner(owner: str | None) -> bool:
    if owner is None:
        return False
    if owner in BURN_AND_SERVICE_ADDRESSES:
        return True
    try:
        return not Pubkey.from_string(owner).is_on_curve()
    except ValueError:
        return False


class SecurityEngine:
    def __init__(
        self,
        *,
        minimum_quote_liquidity_usd: Decimal = Decimal("40000"),
        minimum_pool_age_seconds: Decimal = Decimal("45"),
        maximum_pool_age_seconds: Decimal = Decimal("180"),
        maximum_round_trip_loss_pct: Decimal = Decimal("0.08"),
        maximum_buy_price_impact_pct: Decimal = Decimal("0.025"),
        maximum_sell_price_impact_pct: Decimal = Decimal("0.035"),
        minimum_external_sellers: int = 5,
        maximum_largest_holder_pct: Decimal = Decimal("0.07"),
        maximum_top_5_pct: Decimal = Decimal("0.22"),
        maximum_top_10_pct: Decimal = Decimal("0.30"),
        maximum_dev_holding_pct: Decimal = Decimal("0.02"),
        maximum_dev_cluster_pct: Decimal = Decimal("0.05"),
        maximum_related_cluster_pct: Decimal = Decimal("0.15"),
        maximum_unknown_supply_pct: Decimal = Decimal("0.05"),
        maximum_liquidity_drop_pct: Decimal = Decimal("0.03"),
        maximum_return_since_creation: Decimal = Decimal("2.50"),
        maximum_stream_age_seconds: Decimal = Decimal("3"),
        maximum_quote_age_seconds: Decimal = Decimal("1.5"),
    ) -> None:
        self.minimum_quote_liquidity_usd = minimum_quote_liquidity_usd
        self.minimum_pool_age_seconds = minimum_pool_age_seconds
        self.maximum_pool_age_seconds = maximum_pool_age_seconds
        self.maximum_round_trip_loss_pct = maximum_round_trip_loss_pct
        self.maximum_buy_price_impact_pct = maximum_buy_price_impact_pct
        self.maximum_sell_price_impact_pct = maximum_sell_price_impact_pct
        self.minimum_external_sellers = minimum_external_sellers
        self.maximum_largest_holder_pct = maximum_largest_holder_pct
        self.maximum_top_5_pct = maximum_top_5_pct
        self.maximum_top_10_pct = maximum_top_10_pct
        self.maximum_dev_holding_pct = maximum_dev_holding_pct
        self.maximum_dev_cluster_pct = maximum_dev_cluster_pct
        self.maximum_related_cluster_pct = maximum_related_cluster_pct
        self.maximum_unknown_supply_pct = maximum_unknown_supply_pct
        self.maximum_liquidity_drop_pct = maximum_liquidity_drop_pct
        self.maximum_return_since_creation = maximum_return_since_creation
        self.maximum_stream_age_seconds = maximum_stream_age_seconds
        self.maximum_quote_age_seconds = maximum_quote_age_seconds

    def evaluate(self, context: SecurityContext, *, now: datetime | None = None) -> SecurityResult:
        now = now or datetime.now(tz=timezone.utc)
        reasons: list[RejectReason] = []
        mint = context.mint
        holders = context.holders
        execution = context.execution

        if mint.token_program == TOKEN_2022_PROGRAM_ID:
            reasons.append(RejectReason.TOKEN_2022)
        elif mint.token_program != SPL_TOKEN_PROGRAM_ID:
            reasons.append(RejectReason.INVALID_TOKEN_PROGRAM)
        if mint.mint_authority is not None:
            reasons.append(RejectReason.MINT_AUTHORITY_ACTIVE)
        if mint.freeze_authority is not None:
            reasons.append(RejectReason.FREEZE_AUTHORITY_ACTIVE)
        if mint.total_supply_raw <= 0:
            reasons.append(RejectReason.INVALID_SUPPLY)
        if mint.supply_changed:
            reasons.append(RejectReason.SUPPLY_CHANGED)
        if mint.decimals < 0 or mint.decimals > 18:
            reasons.append(RejectReason.INVALID_DECIMALS)
        if context.quote_mint not in SUPPORTED_QUOTE_MINTS:
            reasons.append(RejectReason.UNSUPPORTED_QUOTE_MINT)
        if context.quote_liquidity_usd < self.minimum_quote_liquidity_usd:
            reasons.append(RejectReason.LOW_QUOTE_LIQUIDITY)
        if not execution.buy_route_available:
            reasons.append(RejectReason.NO_BUY_ROUTE)
        if not execution.sell_route_available:
            reasons.append(RejectReason.NO_SELL_ROUTE)
        if execution.round_trip_loss_pct > self.maximum_round_trip_loss_pct:
            reasons.append(RejectReason.ROUND_TRIP_TOO_EXPENSIVE)
        if execution.buy_price_impact_pct > self.maximum_buy_price_impact_pct:
            reasons.append(RejectReason.HIGH_BUY_PRICE_IMPACT)
        if execution.sell_price_impact_pct > self.maximum_sell_price_impact_pct:
            reasons.append(RejectReason.HIGH_SELL_PRICE_IMPACT)
        if context.external_successful_sellers < self.minimum_external_sellers:
            reasons.append(RejectReason.INSUFFICIENT_EXTERNAL_SELLERS)
        if holders is None:
            reasons.append(RejectReason.HOLDER_DATA_UNAVAILABLE)
        else:
            if context.holders_observed_at is None or _age(now, context.holders_observed_at) > Decimal("15"):
                reasons.append(RejectReason.HOLDER_DATA_STALE)
            if (
                holders.largest_holder_pct > self.maximum_largest_holder_pct
                or holders.top_5_holders_pct > self.maximum_top_5_pct
                or holders.top_10_holders_pct > self.maximum_top_10_pct
            ):
                reasons.append(RejectReason.HIGH_HOLDER_CONCENTRATION)
            if holders.dev_holding_pct > self.maximum_dev_holding_pct:
                reasons.append(RejectReason.DEV_HOLDING_TOO_LARGE)
            if holders.dev_cluster_holding_pct > self.maximum_dev_cluster_pct:
                reasons.append(RejectReason.DEV_CLUSTER_TOO_LARGE)
            if holders.related_cluster_holding_pct > self.maximum_related_cluster_pct:
                reasons.append(RejectReason.RELATED_CLUSTER_TOO_LARGE)
            if holders.unknown_owner_supply_pct > self.maximum_unknown_supply_pct:
                reasons.append(RejectReason.UNKNOWN_OWNER_SUPPLY)
        if context.dev_sold:
            reasons.append(RejectReason.DEV_SOLD)
        if context.pool_age_seconds < self.minimum_pool_age_seconds:
            reasons.append(RejectReason.POOL_TOO_NEW)
        if context.pool_age_seconds > self.maximum_pool_age_seconds:
            reasons.append(RejectReason.POOL_TOO_OLD)
        if context.liquidity_change_30s < -self.maximum_liquidity_drop_pct:
            reasons.append(RejectReason.LIQUIDITY_DECLINING)
        if _age(now, context.stream_observed_at) > self.maximum_stream_age_seconds:
            reasons.append(RejectReason.STALE_DATA)
        if _age(now, execution.quote_observed_at) > self.maximum_quote_age_seconds:
            reasons.append(RejectReason.STALE_QUOTE)
        if not context.protocol_layout_known:
            reasons.append(RejectReason.UNKNOWN_PROTOCOL_LAYOUT)
        if not context.critical_api_available:
            reasons.append(RejectReason.API_UNAVAILABLE)
        if context.previous_rugs >= 2:
            reasons.append(RejectReason.PREVIOUS_RUGS)
        if context.wash_trading_pattern:
            reasons.append(RejectReason.WASH_TRADING_PATTERN)
        if context.return_since_pool_creation > self.maximum_return_since_creation:
            reasons.append(RejectReason.OVEREXTENDED_PRICE)

        unique_reasons = list(dict.fromkeys(reasons))
        return SecurityResult(
            checked_at=now,
            hard_reject=bool(unique_reasons),
            reject_reasons=unique_reasons,
            holder_metrics=holders,
        )


def _age(now: datetime, observed_at: datetime) -> Decimal:
    if now.tzinfo is None or observed_at.tzinfo is None:
        raise ValueError("security timestamps must be timezone-aware")
    return max(Decimal("0"), Decimal(str((now - observed_at).total_seconds())))
