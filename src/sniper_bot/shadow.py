"""Signal-level shadow trades for entries the account's risk rules blocked.

The account stops trading after losing streaks, at its daily loss or trade
limit and while its position slots are full. Each such pause drops signals
from the sample and makes which signals become trades depend on earlier
outcomes. A shadow trade takes exactly the entry the account refused, with
the same fill model (execution delay, Jupiter quote, adverse fill, slippage
tolerance, network fees) and the same exit rules, in a separate book that
never touches the account. Account trades plus shadow trades are the
signal-level sample; the account alone is the portfolio result.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol

from .config import AppConfig
from .exit_engine import ExitDecision, ExitPolicy, evaluate_exit
from .features import FeatureSnapshot
from .models import PositionRecord, PositionStatus, QuoteResponse

if TYPE_CHECKING:
    from .database import Database

logger = logging.getLogger(__name__)

# Blocks that come from the account's state, not from the signal itself.
SHADOW_BLOCK_REASONS = frozenset(
    {
        "HALTED_BY_RISK_MANAGER",
        "CONSECUTIVE_LOSS_PAUSE",
        "CONSECUTIVE_LOSSES",
        "CONSECUTIVE_LOSS_DAILY_HALT",
        "DAILY_LOSS_LIMIT",
        "MAX_TRADES_PER_DAY",
        "MAX_OPEN_POSITIONS_LIMIT",
        "MAX_EXPOSURE_LIMIT",
        "INSUFFICIENT_CASH",
        "ALL_TIME_DRAWDOWN_LIMIT",
        "NO_DAILY_RISK_BUDGET",
        "ATOMIC_RISK_LIMIT",
    }
)


class _Quotes(Protocol):
    async def get_buy_quote(
        self, quote_token: str, token: str, usdc_amount: Decimal
    ) -> QuoteResponse: ...

    async def get_sell_quote(
        self, token: str, quote_token: str, token_amount: Decimal
    ) -> QuoteResponse: ...

    async def get_sell_quote_mark_to_market(
        self, token: str, quote_token: str, token_amount: Decimal
    ) -> QuoteResponse: ...


@dataclass
class ShadowEntry:
    candidate_id: str
    mint: str
    pool_address: str
    size_usd: Decimal
    block_reason: str
    tokens_per_usd: Decimal | None = None


def apply_mark(position: PositionRecord, value_usd: Decimal, at: datetime) -> None:
    """The ledger's per-position mark update, for a book without a ledger."""
    position.last_executable_value_usd = value_usd
    unrealized = value_usd - position.remaining_cost_usd
    position.peak_unrealized_usd = max(position.peak_unrealized_usd, unrealized)
    if value_usd > position.highest_executable_value_usd:
        position.highest_executable_value_usd = value_usd
        position.last_new_high_at = at
    position.lowest_executable_value_usd = (
        value_usd
        if position.lowest_executable_value_usd is None
        else min(position.lowest_executable_value_usd, value_usd)
    )
    if position.remaining_cost_usd > 0:
        current_return = unrealized / position.remaining_cost_usd
        position.mfe_pct = max(position.mfe_pct, current_return)
        position.mae_pct = min(position.mae_pct, current_return)


class ShadowBook:
    def __init__(
        self,
        *,
        config: AppConfig,
        quote_provider: _Quotes,
        database: "Callable[[], Database | None]",
        id_factory: Callable[[], str],
        pool_evidence: Callable[[str], dict[str, str] | None],
        reserve_mark: Callable[[PositionRecord, datetime], Decimal | None],
        on_hold: Callable[[str], None],
        on_release: Callable[[str], None],
        metrics: Any | None = None,
    ) -> None:
        self.config = config
        self.quote_provider = quote_provider
        self._database = database
        self._id_factory = id_factory
        self._pool_evidence = pool_evidence
        self._reserve_mark = reserve_mark
        self._on_hold = on_hold
        self._on_release = on_release
        self._metrics = metrics
        self.positions: dict[str, PositionRecord] = {}
        self._block_reasons: dict[str, str] = {}
        self._momentum_windows: dict[str, int] = {}
        self._mark_failures_since: dict[str, datetime] = {}
        self._adverse_factor = Decimal("1") - Decimal(config.paper.adverse_fill_bps) / Decimal("10000")
        self._delay_seconds = config.paper.execution_delay_ms / 1000
        self._clock: Callable[[], datetime] = lambda: datetime.now(tz=timezone.utc)
        self._sleep: Callable[[float], Awaitable[None]] = asyncio.sleep

    def set_clock(
        self, clock: Callable[[], datetime], sleep: Callable[[float], Awaitable[None]]
    ) -> None:
        self._clock = clock
        self._sleep = sleep

    @property
    def database(self) -> "Database | None":
        """The runtime's current database (it may be swapped after start)."""
        return self._database()

    @property
    def open_positions(self) -> list[PositionRecord]:
        return [
            position
            for position in self.positions.values()
            if position.status == PositionStatus.OPEN
        ]

    def holds(self, mint: str) -> bool:
        return any(position.token_mint == mint for position in self.open_positions)

    async def restore(self) -> list[str]:
        """Reload open shadow positions after a restart; returns their pools."""
        if self.database is None:
            return []
        restored = await self.database.load_open_shadow_positions(
            strategy_version_id=self.config.strategy_version
        )
        for position, block_reason in restored:
            self.positions[position.position_id] = position
            self._block_reasons[position.position_id] = block_reason
            if position.pool_address:
                self._on_hold(position.pool_address)
        return [position.pool_address for position, _ in restored if position.pool_address]

    async def open(self, entry: ShadowEntry) -> PositionRecord | None:
        """Take the refused entry in the shadow book; None when it fails."""
        if self.holds(entry.mint):
            return None
        decision_pool = self._pool_evidence(entry.pool_address)
        self._on_hold(entry.pool_address)
        await self._sleep(self._delay_seconds)
        try:
            quote = await self.quote_provider.get_buy_quote(
                self.config.base_quote_mint, entry.mint, entry.size_usd
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("shadow entry quote unavailable", exc_info=True)
            self._on_release(entry.pool_address)
            return None
        opened_at = self._clock()
        evidence: dict[str, Any] = {
            "execution_delay_ms": self.config.paper.execution_delay_ms,
            "decision_pool": decision_pool,
            "fill_pool": self._pool_evidence(entry.pool_address),
            "quote_out_amount": str(quote.out_amount),
        }
        fee = quote.estimated_network_fee_usd
        position_id = self._id_factory()
        slippage_bps: Decimal | None = None
        if entry.tokens_per_usd:
            expected = entry.tokens_per_usd * entry.size_usd
            slippage_bps = (
                (Decimal("1") - quote.out_amount / expected) * Decimal("10000")
            ).quantize(Decimal("0.01"))
            evidence["slippage_bps"] = str(slippage_bps)
        max_slippage = self.config.paper.max_entry_slippage_bps
        if slippage_bps is not None and max_slippage > 0 and slippage_bps > max_slippage:
            self._on_release(entry.pool_address)
            if self.database is not None:
                await self.database.record_shadow_entry(
                    position_id=position_id,
                    candidate_id=entry.candidate_id,
                    mint=entry.mint,
                    pool_address=entry.pool_address,
                    notional_usd=entry.size_usd,
                    block_reason=entry.block_reason,
                    status="FAILED",
                    opened_at=opened_at,
                    token_amount=Decimal("0"),
                    network_fee_usd=fee,
                    adverse_fill_bps=self.config.paper.adverse_fill_bps,
                    strategy_version_id=self.config.strategy_version,
                    config_hash=self.config.config_hash,
                    evidence=evidence,
                )
            self._count("failed_slippage")
            return None
        tokens = quote.out_amount * self._adverse_factor
        cost = entry.size_usd + fee
        position = PositionRecord(
            position_id=position_id,
            token_mint=entry.mint,
            open_fill_id=f"shadow-entry:{position_id}",
            entry_token_amount=tokens,
            entry_cost_usd=cost,
            open_ratio=Decimal("1"),
            opened_at=opened_at,
            locked_usd=cost,
            remaining_token_amount=tokens,
            remaining_cost_usd=cost,
            highest_executable_value_usd=cost,
            lowest_executable_value_usd=cost,
            last_executable_value_usd=cost,
            last_new_high_at=opened_at,
            candidate_id=entry.candidate_id,
            pool_address=entry.pool_address,
            strategy_version=self.config.strategy_version,
        )
        if self.database is not None:
            await self.database.record_shadow_entry(
                position_id=position_id,
                candidate_id=entry.candidate_id,
                mint=entry.mint,
                pool_address=entry.pool_address,
                notional_usd=entry.size_usd,
                block_reason=entry.block_reason,
                status="OPEN",
                opened_at=opened_at,
                token_amount=tokens,
                network_fee_usd=fee,
                adverse_fill_bps=self.config.paper.adverse_fill_bps,
                strategy_version_id=self.config.strategy_version,
                config_hash=self.config.config_hash,
                evidence=evidence,
            )
        self.positions[position_id] = position
        self._block_reasons[position_id] = entry.block_reason
        self._count("opened")
        return position

    async def evaluate_exits(
        self,
        now: datetime,
        *,
        policy: ExitPolicy,
        features: Callable[[str, datetime], FeatureSnapshot | None],
        dev_sold: Callable[[PositionRecord, datetime], bool],
    ) -> list[ExitDecision]:
        decisions: list[ExitDecision] = []
        for position in list(self.open_positions):
            value = await self._mark(position, now)
            if value is None:
                continue
            apply_mark(position, value, now)
            feature = features(position.pool_address or "", now)
            momentum_now = bool(
                feature
                and feature.buy_sell_volume_ratio < Decimal("0.8")
                and feature.unique_sellers_30s > feature.unique_buyers_30s
            )
            windows = self._momentum_windows.get(position.position_id, 0) + 1 if momentum_now else 0
            self._momentum_windows[position.position_id] = windows
            # Signal-level: only market emergencies, never the account's limits.
            emergency = bool(
                dev_sold(position, now)
                or (
                    feature is not None
                    and feature.quote_liquidity_change_30s
                    <= -self.config.liquidity.emergency_liquidity_drop_pct
                )
            )
            price = (
                value / position.remaining_token_amount
                if position.remaining_token_amount > 0
                else Decimal("0")
            )
            decision = evaluate_exit(
                position,
                price,
                now,
                policy=policy,
                momentum_exit=windows >= self.config.exits.momentum_exit_windows,
                emergency_exit=emergency,
            )
            decisions.append(decision)
            if decision.should_exit:
                await self._close(
                    position,
                    position.remaining_token_amount * decision.close_fraction,
                    decision.reason.value,
                )
        return decisions

    async def _mark(self, position: PositionRecord, now: datetime) -> Decimal | None:
        reserve = self._reserve_mark(position, now)
        if self.config.exits.mark_source == "reserves" and reserve is not None:
            return reserve
        try:
            quote = await self.quote_provider.get_sell_quote_mark_to_market(
                position.token_mint, self.config.base_quote_mint, position.remaining_token_amount
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            first = self._mark_failures_since.setdefault(position.position_id, now)
            if (now - first).total_seconds() >= self.config.paper.exit_retry_timeout_seconds:
                await self._close(
                    position, position.remaining_token_amount, "UNRECOVERABLE", proceeds=Decimal("0")
                )
            return None
        self._mark_failures_since.pop(position.position_id, None)
        return quote.out_amount_usd if quote.out_amount_usd else quote.out_amount

    async def _close(
        self,
        position: PositionRecord,
        token_amount: Decimal,
        reason: str,
        *,
        proceeds: Decimal | None = None,
    ) -> None:
        if token_amount <= 0:
            return
        token_amount = min(token_amount, position.remaining_token_amount)
        decision_pool = self._pool_evidence(position.pool_address or "")
        fee = Decimal("0")
        quote: QuoteResponse | None = None
        if proceeds is None:
            await self._sleep(self._delay_seconds)
            try:
                quote = await self.quote_provider.get_sell_quote(
                    position.token_mint, self.config.base_quote_mint, token_amount
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("shadow exit quote unavailable; retrying", exc_info=True)
                return
            gross = quote.out_amount_usd if quote.out_amount_usd else quote.out_amount
            proceeds = gross * self._adverse_factor
            fee = quote.estimated_network_fee_usd
        closed_at = self._clock()
        fraction = token_amount / position.remaining_token_amount
        proportional_cost = position.remaining_cost_usd * fraction
        net = max(Decimal("0"), proceeds - fee)
        realized = net - proportional_cost
        position.remaining_cost_usd -= proportional_cost
        position.remaining_token_amount -= token_amount
        position.realized_pnl_usd += realized
        position.highest_executable_value_usd *= Decimal("1") - fraction
        position.last_executable_value_usd *= Decimal("1") - fraction
        position.peak_unrealized_usd *= Decimal("1") - fraction
        if reason == "TP1":
            position.tp1_taken = True
        elif reason == "TP2":
            position.tp2_taken = True
        final = position.remaining_token_amount <= 0 or reason == "UNRECOVERABLE"
        if final:
            position.status = PositionStatus.CLOSED
            position.closed_at = closed_at
            position.final_exit_reason = reason
        if self.database is not None:
            await self.database.record_shadow_exit(
                position=position,
                fill_id=self._id_factory(),
                token_amount=token_amount,
                gross_usd=proceeds,
                network_fee_usd=fee,
                realized_pnl_usd=realized,
                exit_reason=reason,
                closed=final,
                filled_at=closed_at,
                evidence={
                    "execution_delay_ms": self.config.paper.execution_delay_ms,
                    "decision_pool": decision_pool,
                    "fill_pool": self._pool_evidence(position.pool_address or ""),
                    "quote_out_amount": str(quote.out_amount) if quote is not None else None,
                },
            )
        if final:
            self._momentum_windows.pop(position.position_id, None)
            self._block_reasons.pop(position.position_id, None)
            self.positions.pop(position.position_id, None)
            if position.pool_address and not any(
                other.pool_address == position.pool_address for other in self.open_positions
            ):
                self._on_release(position.pool_address)
            self._count("closed")

    def _count(self, status: str) -> None:
        if self._metrics is not None:
            self._metrics.shadow_trades.labels(status=status).inc()
