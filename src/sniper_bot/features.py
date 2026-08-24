"""Deterministic event-time feature windows without look-ahead."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, Field, field_validator

FEATURE_VERSION = "confirmation-v1"


class TradeSide(StrEnum):
    BUY = "buy"
    SELL = "sell"


class TradeObservation(BaseModel):
    event_id: str
    pool_address: str
    event_time: datetime
    side: TradeSide
    wallet: str
    volume_usd: Decimal = Field(ge=0)
    price_usd: Decimal = Field(gt=0)
    external: bool = True
    same_funder_cluster: bool = False

    @field_validator("event_time")
    @classmethod
    def aware_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("trade event_time must be timezone-aware")
        return value.astimezone(timezone.utc)


class LiquidityObservation(BaseModel):
    event_id: str
    pool_address: str
    event_time: datetime
    quote_liquidity_usd: Decimal = Field(ge=0)
    market_cap_usd: Decimal | None = Field(default=None, ge=0)


class HolderObservation(BaseModel):
    event_id: str
    pool_address: str
    event_time: datetime
    holder_count: int = Field(ge=0)
    top_10_holders_pct: Decimal = Field(ge=0, le=1)
    dev_cluster_holding_pct: Decimal = Field(ge=0, le=1)
    largest_related_cluster_pct: Decimal = Field(ge=0, le=1)


class FeatureSnapshot(BaseModel):
    feature_version: str = FEATURE_VERSION
    pool_address: str
    snapshot_time: datetime
    pool_age_seconds: Decimal
    buy_transactions_15s: int = 0
    buy_transactions_30s: int = 0
    buy_transactions_60s: int = 0
    unique_buyers_15s: int = 0
    unique_buyers_30s: int = 0
    unique_buyers_60s: int = 0
    new_buyers_15s: int = 0
    new_buyers_30s: int = 0
    sell_transactions_15s: int = 0
    sell_transactions_30s: int = 0
    sell_transactions_60s: int = 0
    unique_sellers_15s: int = 0
    unique_sellers_30s: int = 0
    unique_sellers_60s: int = 0
    external_successful_sellers: int = 0
    buy_volume_usd_15s: Decimal = Decimal("0")
    buy_volume_usd_30s: Decimal = Decimal("0")
    buy_volume_usd_60s: Decimal = Decimal("0")
    sell_volume_usd_15s: Decimal = Decimal("0")
    sell_volume_usd_30s: Decimal = Decimal("0")
    sell_volume_usd_60s: Decimal = Decimal("0")
    net_buy_volume_usd_30s: Decimal = Decimal("0")
    net_buy_volume_usd_60s: Decimal = Decimal("0")
    top_1_buyer_volume_share: Decimal = Decimal("0")
    top_5_buyer_volume_share: Decimal = Decimal("0")
    buyer_volume_hhi: Decimal = Decimal("0")
    same_funder_buy_share: Decimal = Decimal("0")
    transactions_per_trader: Decimal = Decimal("0")
    unique_buyer_ratio: Decimal = Decimal("0")
    buyer_acceleration: Decimal = Decimal("0")
    buy_sell_volume_ratio: Decimal = Decimal("0")
    quote_liquidity_usd: Decimal = Decimal("0")
    quote_liquidity_change_10s: Decimal = Decimal("0")
    quote_liquidity_change_30s: Decimal = Decimal("0")
    quote_liquidity_change_since_pool_creation: Decimal = Decimal("0")
    volume_to_quote_liquidity_30s: Decimal = Decimal("0")
    volume_to_quote_liquidity_60s: Decimal = Decimal("0")
    market_cap_to_quote_liquidity: Decimal = Decimal("0")
    current_price_usd: Decimal = Decimal("0")
    return_since_pool_creation: Decimal = Decimal("0")
    return_15s: Decimal = Decimal("0")
    return_30s: Decimal = Decimal("0")
    return_60s: Decimal = Decimal("0")
    rolling_vwap_30s: Decimal = Decimal("0")
    rolling_vwap_60s: Decimal = Decimal("0")
    local_high: Decimal = Decimal("0")
    drawdown_from_local_high: Decimal = Decimal("0")
    distance_from_vwap_30s: Decimal = Decimal("0")
    holder_count: int = 0
    holder_growth_30s: int = 0
    holder_growth_60s: int = 0
    top_10_holders_pct: Decimal = Decimal("0")
    dev_cluster_holding_pct: Decimal = Decimal("0")
    largest_related_cluster_pct: Decimal = Decimal("0")


class EventTimeFeatureEngine:
    def __init__(self) -> None:
        self._pool_created: dict[str, datetime] = {}
        self._trades: dict[str, list[TradeObservation]] = {}
        self._liquidity: dict[str, list[LiquidityObservation]] = {}
        self._holders: dict[str, list[HolderObservation]] = {}
        self._seen: set[str] = set()

    def register_pool(self, pool_address: str, created_at: datetime) -> None:
        if created_at.tzinfo is None:
            raise ValueError("pool creation time must be timezone-aware")
        current = self._pool_created.get(pool_address)
        created = created_at.astimezone(timezone.utc)
        if current is None or created < current:
            self._pool_created[pool_address] = created

    def ingest_trade(self, event: TradeObservation) -> bool:
        if not self._accept(event.event_id):
            return False
        self._trades.setdefault(event.pool_address, []).append(event)
        self._trades[event.pool_address].sort(key=lambda item: (item.event_time, item.event_id))
        return True

    def trades(
        self, pool_address: str, *, at: datetime | None = None
    ) -> tuple[TradeObservation, ...]:
        events = self._trades.get(pool_address, [])
        if at is not None:
            events = [event for event in events if event.event_time <= at]
        return tuple(events)

    def ingest_liquidity(self, event: LiquidityObservation) -> bool:
        if not self._accept(event.event_id):
            return False
        self._liquidity.setdefault(event.pool_address, []).append(event)
        self._liquidity[event.pool_address].sort(key=lambda item: (item.event_time, item.event_id))
        return True

    def ingest_holders(self, event: HolderObservation) -> bool:
        if not self._accept(event.event_id):
            return False
        self._holders.setdefault(event.pool_address, []).append(event)
        self._holders[event.pool_address].sort(key=lambda item: (item.event_time, item.event_id))
        return True

    def snapshot(self, pool_address: str, at: datetime) -> FeatureSnapshot:
        if at.tzinfo is None:
            raise ValueError("snapshot time must be timezone-aware")
        at = at.astimezone(timezone.utc)
        created = self._pool_created.get(pool_address, at)
        all_trades = [event for event in self._trades.get(pool_address, []) if event.event_time <= at]
        buys = [event for event in all_trades if event.side == TradeSide.BUY]
        sells = [event for event in all_trades if event.side == TradeSide.SELL]

        def window(events: list[TradeObservation], seconds: int) -> list[TradeObservation]:
            cutoff = at - timedelta(seconds=seconds)
            return [event for event in events if cutoff < event.event_time <= at]

        buy15, buy30, buy60 = (window(buys, seconds) for seconds in (15, 30, 60))
        sell15, sell30, sell60 = (window(sells, seconds) for seconds in (15, 30, 60))
        all60 = window(all_trades, 60)
        previous_buyers = {
            event.wallet
            for event in buys
            if at - timedelta(seconds=60) < event.event_time <= at - timedelta(seconds=30)
        }
        current_buyers = {event.wallet for event in buy30}
        buyer_acceleration = Decimal(len(current_buyers)) / Decimal(max(len(previous_buyers), 1))
        buyer_volumes: dict[str, Decimal] = {}
        for event in buy60:
            buyer_volumes[event.wallet] = buyer_volumes.get(event.wallet, Decimal("0")) + event.volume_usd
        ranked_buy_volume = sorted(buyer_volumes.values(), reverse=True)
        buy_total60 = sum((event.volume_usd for event in buy60), Decimal("0"))
        shares = [amount / buy_total60 for amount in ranked_buy_volume] if buy_total60 > 0 else []
        unique_traders = {event.wallet for event in all60}
        first_buy_by_wallet: dict[str, datetime] = {}
        for event in buys:
            first_buy_by_wallet.setdefault(event.wallet, event.event_time)
        new15 = sum(1 for timestamp in first_buy_by_wallet.values() if at - timedelta(seconds=15) < timestamp <= at)
        new30 = sum(1 for timestamp in first_buy_by_wallet.values() if at - timedelta(seconds=30) < timestamp <= at)

        liquidity_events = [event for event in self._liquidity.get(pool_address, []) if event.event_time <= at]
        current_liquidity = liquidity_events[-1] if liquidity_events else None
        quote_liquidity = current_liquidity.quote_liquidity_usd if current_liquidity else Decimal("0")
        initial_liquidity = liquidity_events[0].quote_liquidity_usd if liquidity_events else Decimal("0")
        market_cap = current_liquidity.market_cap_usd if current_liquidity else None
        price_current = all_trades[-1].price_usd if all_trades else Decimal("0")
        price_initial = all_trades[0].price_usd if all_trades else Decimal("0")
        local_high = max((event.price_usd for event in all_trades), default=Decimal("0"))
        vwap30 = _vwap(window(all_trades, 30))
        vwap60 = _vwap(all60)
        holder_events = [event for event in self._holders.get(pool_address, []) if event.event_time <= at]
        current_holders = holder_events[-1] if holder_events else None

        buy_volume15 = _volume(buy15)
        buy_volume30 = _volume(buy30)
        sell_volume15 = _volume(sell15)
        sell_volume30 = _volume(sell30)
        sell_volume60 = _volume(sell60)
        total_volume30 = buy_volume30 + sell_volume30
        total_volume60 = buy_total60 + sell_volume60
        return FeatureSnapshot(
            pool_address=pool_address,
            snapshot_time=at,
            pool_age_seconds=max(Decimal("0"), Decimal(str((at - created).total_seconds()))),
            buy_transactions_15s=len(buy15),
            buy_transactions_30s=len(buy30),
            buy_transactions_60s=len(buy60),
            unique_buyers_15s=len({event.wallet for event in buy15}),
            unique_buyers_30s=len(current_buyers),
            unique_buyers_60s=len({event.wallet for event in buy60}),
            new_buyers_15s=new15,
            new_buyers_30s=new30,
            sell_transactions_15s=len(sell15),
            sell_transactions_30s=len(sell30),
            sell_transactions_60s=len(sell60),
            unique_sellers_15s=len({event.wallet for event in sell15}),
            unique_sellers_30s=len({event.wallet for event in sell30}),
            unique_sellers_60s=len({event.wallet for event in sell60}),
            external_successful_sellers=len({event.wallet for event in sells if event.external}),
            buy_volume_usd_15s=buy_volume15,
            buy_volume_usd_30s=buy_volume30,
            buy_volume_usd_60s=buy_total60,
            sell_volume_usd_15s=sell_volume15,
            sell_volume_usd_30s=sell_volume30,
            sell_volume_usd_60s=sell_volume60,
            net_buy_volume_usd_30s=buy_volume30 - sell_volume30,
            net_buy_volume_usd_60s=buy_total60 - sell_volume60,
            top_1_buyer_volume_share=shares[0] if shares else Decimal("0"),
            top_5_buyer_volume_share=sum(shares[:5], Decimal("0")),
            buyer_volume_hhi=sum((share * share for share in shares), Decimal("0")),
            same_funder_buy_share=(
                sum((event.volume_usd for event in buy60 if event.same_funder_cluster), Decimal("0")) / buy_total60
                if buy_total60 > 0 else Decimal("0")
            ),
            transactions_per_trader=Decimal(len(all60)) / Decimal(max(len(unique_traders), 1)),
            unique_buyer_ratio=Decimal(len({event.wallet for event in buy60})) / Decimal(max(len(buy60), 1)),
            buyer_acceleration=buyer_acceleration,
            buy_sell_volume_ratio=buy_total60 / max(sell_volume60, Decimal("0.01")),
            quote_liquidity_usd=quote_liquidity,
            quote_liquidity_change_10s=_liquidity_change(liquidity_events, at, 10),
            quote_liquidity_change_30s=_liquidity_change(liquidity_events, at, 30),
            quote_liquidity_change_since_pool_creation=_change(quote_liquidity, initial_liquidity),
            volume_to_quote_liquidity_30s=_ratio(total_volume30, quote_liquidity),
            volume_to_quote_liquidity_60s=_ratio(total_volume60, quote_liquidity),
            market_cap_to_quote_liquidity=_ratio(market_cap or Decimal("0"), quote_liquidity),
            current_price_usd=price_current,
            return_since_pool_creation=_change(price_current, price_initial),
            return_15s=_price_return(all_trades, at, 15),
            return_30s=_price_return(all_trades, at, 30),
            return_60s=_price_return(all_trades, at, 60),
            rolling_vwap_30s=vwap30,
            rolling_vwap_60s=vwap60,
            local_high=local_high,
            drawdown_from_local_high=(Decimal("1") - price_current / local_high if local_high > 0 else Decimal("0")),
            distance_from_vwap_30s=(price_current / vwap30 - Decimal("1") if vwap30 > 0 else Decimal("0")),
            holder_count=current_holders.holder_count if current_holders else 0,
            holder_growth_30s=_holder_growth(holder_events, at, 30),
            holder_growth_60s=_holder_growth(holder_events, at, 60),
            top_10_holders_pct=current_holders.top_10_holders_pct if current_holders else Decimal("0"),
            dev_cluster_holding_pct=current_holders.dev_cluster_holding_pct if current_holders else Decimal("0"),
            largest_related_cluster_pct=current_holders.largest_related_cluster_pct if current_holders else Decimal("0"),
        )

    def _accept(self, event_id: str) -> bool:
        if event_id in self._seen:
            return False
        self._seen.add(event_id)
        return True


def _volume(events: list[TradeObservation]) -> Decimal:
    return sum((event.volume_usd for event in events), Decimal("0"))


def _vwap(events: list[TradeObservation]) -> Decimal:
    volume = _volume(events)
    if volume <= 0:
        return Decimal("0")
    return sum((event.price_usd * event.volume_usd for event in events), Decimal("0")) / volume


def _change(current: Decimal, previous: Decimal) -> Decimal:
    return current / previous - Decimal("1") if previous > 0 else Decimal("0")


def _ratio(numerator: Decimal, denominator: Decimal) -> Decimal:
    return numerator / denominator if denominator > 0 else Decimal("0")


def _liquidity_change(events: list[LiquidityObservation], at: datetime, seconds: int) -> Decimal:
    if not events:
        return Decimal("0")
    cutoff = at - timedelta(seconds=seconds)
    previous = next((event for event in reversed(events) if event.event_time <= cutoff), events[0])
    return _change(events[-1].quote_liquidity_usd, previous.quote_liquidity_usd)


def _price_return(events: list[TradeObservation], at: datetime, seconds: int) -> Decimal:
    if not events:
        return Decimal("0")
    cutoff = at - timedelta(seconds=seconds)
    previous = next((event for event in reversed(events) if event.event_time <= cutoff), events[0])
    return _change(events[-1].price_usd, previous.price_usd)


def _holder_growth(events: list[HolderObservation], at: datetime, seconds: int) -> int:
    if not events:
        return 0
    cutoff = at - timedelta(seconds=seconds)
    previous = next((event for event in reversed(events) if event.event_time <= cutoff), events[0])
    return events[-1].holder_count - previous.holder_count
