"""Paper execution engine with deterministic virtual fills."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Awaitable, Callable
from uuid import uuid4

from .database import Database
from .errors import EntrySlippageExceededError, ExecutionBlockedError
from .jupiter import JupiterQuoteProvider
from .ledger import PaperLedger
from .models import FillType, QuoteResponse
from .risk import RiskDecision, RiskManager

# Pool state a fill can later be re-priced from: (pool address) -> evidence.
PoolEvidenceLookup = Callable[[str], "dict[str, str] | None"]


@dataclass(frozen=True)
class FillReference:
    """What the entry decision was priced at, before the execution delay."""

    tokens_per_usd: Decimal
    quoted_at: datetime


def _quote_evidence(quote: QuoteResponse) -> dict[str, str]:
    return {
        "request_id": quote.request_id,
        "received_at": quote.received_at.isoformat(),
        "in_amount": str(quote.in_amount),
        "out_amount": str(quote.out_amount),
        "in_amount_usd": str(quote.in_amount_usd or ""),
        "out_amount_usd": str(quote.out_amount_usd or ""),
        "price_impact_pct": str(quote.price_impact_pct),
        "network_fee_usd": str(quote.estimated_network_fee_usd),
    }


@dataclass
class PaperExecutionResult:
    position_id: str
    order_id: str
    token_mint: str
    fill_id: str
    usd_notional: Decimal
    token_amount: Decimal
    direction: str


class PaperBroker:
    def __init__(
        self,
        quote_provider: JupiterQuoteProvider,
        ledger: PaperLedger,
        risk_manager: RiskManager,
        *,
        base_quote_mint: str = "So11111111111111111111111111111111111111112",
        id_factory: Callable[[], str] | None = None,
        execution_delay_ms: int = 1200,
        adverse_fill_bps: int = 50,
        database: Database | None = None,
        account_id: str = "paper-main",
        strategy_version: str = "",
        config_hash: str = "",
        exit_retry_interval_ms: int = 1000,
        exit_retry_timeout_seconds: int = 30,
        max_entry_slippage_bps: int = 0,
        pool_evidence: PoolEvidenceLookup | None = None,
        metrics: Any | None = None,
    ) -> None:
        self._quote_provider = quote_provider
        self._ledger = ledger
        self._risk = risk_manager
        self._base_quote_mint = base_quote_mint
        self._id_factory = id_factory or (lambda: str(uuid4()))
        if execution_delay_ms < 0:
            raise ValueError("execution_delay_ms must be non-negative")
        if adverse_fill_bps < 0 or adverse_fill_bps >= 10_000:
            raise ValueError("adverse_fill_bps must be in range 0..9999")
        self._execution_delay_seconds = execution_delay_ms / 1000
        self._adverse_factor = Decimal("1") - Decimal(adverse_fill_bps) / Decimal("10000")
        self._adverse_fill_bps = adverse_fill_bps
        self._database = database
        self._account_id = account_id
        self._strategy_version = strategy_version
        self._config_hash = config_hash
        self._exit_retry_interval_seconds = max(0, exit_retry_interval_ms) / 1000
        self._exit_retry_timeout_seconds = max(0, exit_retry_timeout_seconds)
        self._max_entry_slippage_bps = max(0, max_entry_slippage_bps)
        self._pool_evidence = pool_evidence
        self._metrics = metrics
        self._clock: Callable[[], datetime] = lambda: datetime.now(tz=timezone.utc)
        self._sleep: Callable[[float], Awaitable[None]] = asyncio.sleep

    def set_clock(
        self,
        clock: Callable[[], datetime],
        sleep: Callable[[float], Awaitable[None]],
    ) -> None:
        self._clock = clock
        self._sleep = sleep
        risk_clock = getattr(self._risk, "set_clock", None)
        if callable(risk_clock):
            risk_clock(clock)

    async def open(
        self,
        token_mint: str,
        usdc_amount: Decimal,
        *,
        order_id: str | None = None,
        candidate_id: str | None = None,
        pool_address: str | None = None,
        alert_text: str | None = None,
        entry_score: Decimal | None = None,
        entry_liquidity_usd: Decimal | None = None,
        entry_pool_age_seconds: Decimal | None = None,
        round_trip_cost_pct: Decimal | None = None,
        reference: FillReference | None = None,
    ) -> PaperExecutionResult:
        if order_id is not None:
            existing_fill = self._ledger.get_fill_by_order(order_id)
            if existing_fill and existing_fill.fill_type == FillType.ENTRY:
                existing_position = self._ledger.get_position_by_order(order_id)
                if existing_position is None:
                    raise ExecutionBlockedError("inconsistent ledger state for idempotent order")
                return PaperExecutionResult(
                    position_id=existing_position.position_id,
                    order_id=order_id,
                    token_mint=token_mint,
                    fill_id=existing_fill.fill_id,
                    usd_notional=existing_fill.usd_notional,
                    token_amount=existing_fill.token_amount,
                    direction="entry",
                )

        decision = self._risk.evaluate_entry(usdc_amount, token_mint)
        if decision.decision != RiskDecision.ALLOW:
            raise ExecutionBlockedError(decision.reason or "blocked")

        decision_pool = self._pool_state(pool_address)
        # Confirmation delay to emulate asynchronous and adverse execution.
        await self._sleep(self._execution_delay_seconds)
        quote = await self._quote_provider.get_buy_quote(self._base_quote_mint, token_mint, usdc_amount)
        order_id = order_id or self._id_factory()
        evidence: dict[str, Any] = {
            "execution_delay_ms": int(self._execution_delay_seconds * 1000),
            "decision_pool": decision_pool,
            "fill_pool": self._pool_state(pool_address),
            "quote": _quote_evidence(quote),
        }
        if reference is not None and reference.tokens_per_usd > 0:
            expected_tokens = reference.tokens_per_usd * usdc_amount
            slippage_bps = (
                (Decimal("1") - quote.out_amount / expected_tokens) * Decimal("10000")
            ).quantize(Decimal("0.01"))
            evidence["reference"] = {
                "tokens_per_usd": str(reference.tokens_per_usd),
                "quoted_at": reference.quoted_at.isoformat(),
            }
            evidence["slippage_bps"] = str(slippage_bps)
            if self._metrics is not None:
                self._metrics.paper_fill_slippage_bps.observe(float(slippage_bps))
            if (
                self._max_entry_slippage_bps > 0
                and slippage_bps > self._max_entry_slippage_bps
            ):
                await self._fail_entry(
                    token_mint=token_mint,
                    usdc_amount=usdc_amount,
                    order_id=order_id,
                    candidate_id=candidate_id,
                    quote=quote,
                    evidence=evidence,
                )
                raise EntrySlippageExceededError(
                    f"entry fill moved {slippage_bps} bps against the decision "
                    f"(limit {self._max_entry_slippage_bps})"
                )
        token_amount = quote.out_amount * self._adverse_factor
        quote_id = f"quote-{order_id}"
        position_id = self._id_factory()
        fill_id = self._id_factory()
        filled_at = self._clock()
        if self._database is not None:
            if not candidate_id or not pool_address or not self._strategy_version:
                raise ExecutionBlockedError("persistent paper entry context is incomplete")
            limits = self._risk.atomic_entry_limits(filled_at)
            try:
                committed = await self._database.commit_paper_entry(
                    account_id=self._account_id,
                    strategy_version_id=self._strategy_version,
                    config_hash=self._config_hash,
                    candidate_id=candidate_id,
                    pool_address=pool_address,
                    mint=token_mint,
                    order_id=order_id,
                    position_id=position_id,
                    fill_id=fill_id,
                    quote=quote,
                    requested_usd=usdc_amount,
                    filled_token_amount=token_amount,
                    adverse_fill_bps=self._adverse_fill_bps,
                    filled_at=filled_at,
                    outbox_text=alert_text
                    or f"paper entry | mint={token_mint} | size={usdc_amount}",
                    max_position_usdc=limits.max_position_usdc,
                    max_exposure_usdc=limits.max_exposure_usdc,
                    max_open_positions=limits.max_open_positions,
                    daily_loss_limit_usdc=limits.daily_loss_limit_usdc,
                    max_trades_per_day=limits.max_trades_per_day,
                    risk_day_key=limits.day_key,
                    risk_day_start=limits.day_start,
                    risk_day_end=limits.day_end,
                    evidence=evidence,
                )
            except RuntimeError as error:
                if str(error).startswith("risk limit:"):
                    raise ExecutionBlockedError(str(error)) from error
                raise
            position_id = str(committed["position_id"])
            fill_id = str(committed["fill_id"])
            quote_id = str(committed["quote_id"])
            filled_at = committed["filled_at"]
        if isinstance(self._ledger, PaperLedger):
            position = self._ledger.open_position(
                token_mint=token_mint,
                usd_amount=usdc_amount,
                token_amount=token_amount,
                order_id=order_id,
                quote_id=quote_id,
                position_id=position_id,
                fill_id=fill_id,
                created_at=filled_at,
                candidate_id=candidate_id,
                pool_address=pool_address,
                entry_score=entry_score,
                entry_liquidity_usd=entry_liquidity_usd,
                entry_pool_age_seconds=entry_pool_age_seconds,
                strategy_version=self._strategy_version,
                price_impact_pct=quote.price_impact_pct,
                platform_fee_usd=quote.platform_fee_usd,
                network_fee_usd=quote.estimated_network_fee_usd,
                other_cost_usd=(
                    usdc_amount * Decimal(self._adverse_fill_bps) / Decimal("10000")
                ),
                adverse_fill_bps=self._adverse_fill_bps,
                quote_latency_ms=quote.latency_ms,
                round_trip_cost_pct=round_trip_cost_pct,
            )
        else:
            position = self._ledger.open_position(
                token_mint=token_mint,
                usd_amount=usdc_amount,
                token_amount=token_amount,
                order_id=order_id,
                quote_id=quote_id,
                position_id=position_id,
                fill_id=fill_id,
                created_at=filled_at,
            )
        return PaperExecutionResult(
            position_id=position.position_id,
            order_id=order_id,
            token_mint=token_mint,
            fill_id=fill_id,
            usd_notional=usdc_amount,
            token_amount=token_amount,
            direction="entry",
        )

    def _pool_state(self, pool_address: str | None) -> dict[str, str] | None:
        if self._pool_evidence is None or not pool_address:
            return None
        return self._pool_evidence(pool_address)

    async def _fail_entry(
        self,
        *,
        token_mint: str,
        usdc_amount: Decimal,
        order_id: str,
        candidate_id: str | None,
        quote: QuoteResponse,
        evidence: dict[str, Any],
    ) -> None:
        """Book a failed swap: no position, only the network fee it burned."""
        fee = quote.estimated_network_fee_usd
        failed_at = self._clock()
        if self._metrics is not None:
            self._metrics.paper_orders.labels(status="failed_slippage").inc()
        if self._database is not None:
            await self._database.record_failed_entry(
                account_id=self._account_id,
                candidate_id=candidate_id,
                mint=token_mint,
                order_id=order_id,
                quote=quote,
                requested_usd=usdc_amount,
                network_fee_usd=fee,
                failed_at=failed_at,
                evidence=evidence,
            )
        if isinstance(self._ledger, PaperLedger):
            self._ledger.charge_trading_cost(fee, at=failed_at)

    async def close_half(
        self,
        token_mint: str,
        *,
        order_id: str | None = None,
        exit_reason: str | None = None,
    ) -> PaperExecutionResult:
        positions = [position for position in self._ledger.open_positions if position.token_mint == token_mint]
        if not positions:
            raise ValueError("no position to close")
        position = positions[0]
        close_amount = position.remaining_token_amount / 2
        return await self.close(token_mint, close_amount, order_id=order_id, exit_reason=exit_reason)

    async def close(
        self,
        token_mint: str,
        token_amount: Decimal,
        *,
        order_id: str | None = None,
        exit_reason: str | None = None,
    ) -> PaperExecutionResult:
        if token_amount <= 0:
            raise ValueError("token_amount must be greater 0")

        if order_id is not None:
            existing_fill = self._ledger.get_fill_by_order(order_id)
            if existing_fill and existing_fill.fill_type == FillType.EXIT:
                existing_position = self._ledger.get_position_by_order(order_id)
                if existing_position is None:
                    raise ExecutionBlockedError("inconsistent ledger state for idempotent order")
                return PaperExecutionResult(
                    position_id=existing_position.position_id,
                    order_id=order_id,
                    token_mint=token_mint,
                    fill_id=existing_fill.fill_id,
                    usd_notional=existing_fill.usd_notional,
                    token_amount=existing_fill.token_amount,
                    direction="exit",
                )

        positions = [position for position in self._ledger.open_positions if position.token_mint == token_mint]
        if not positions:
            raise ValueError("no position to close")
        position = positions[0]
        if token_amount > position.remaining_token_amount:
            raise ValueError("token_amount exceeds remaining position")

        decision_pool = self._pool_state(position.pool_address)
        # Exits land after the same latency as entries: the decision was made
        # on an earlier mark, the sell is priced when it would execute.
        await self._sleep(self._execution_delay_seconds)
        elapsed = 0.0
        quote = None
        while quote is None:
            try:
                quote = await self._quote_provider.get_sell_quote(
                    token=token_mint,
                    quote_token=self._base_quote_mint,
                    token_amount=token_amount,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                if elapsed >= self._exit_retry_timeout_seconds:
                    break
                await self._sleep(self._exit_retry_interval_seconds)
                elapsed += self._exit_retry_interval_seconds
        quoted_usd = (
            quote.out_amount_usd
            if quote is not None and quote.out_amount_usd and quote.out_amount_usd > 0
            else (quote.out_amount if quote is not None else Decimal("0"))
        )
        sell_price_usd = quoted_usd * self._adverse_factor
        order_id = order_id or self._id_factory()
        quote_id = f"quote-{order_id}"
        close_fill_id = self._id_factory()
        filled_at = self._clock()
        final_reason = "UNRECOVERABLE" if quote is None else (exit_reason or "UNKNOWN_EXIT")
        exit_evidence: dict[str, Any] = {
            "execution_delay_ms": int(self._execution_delay_seconds * 1000),
            "decision_pool": decision_pool,
            "fill_pool": self._pool_state(position.pool_address),
            "quote": _quote_evidence(quote) if quote is not None else None,
        }
        if self._database is not None:
            committed = await self._database.commit_paper_exit(
                account_id=self._account_id,
                strategy_version_id=self._strategy_version,
                config_hash=self._config_hash,
                mint=token_mint,
                order_id=order_id,
                fill_id=close_fill_id,
                token_amount=token_amount,
                usd_received=sell_price_usd,
                quote=quote,
                adverse_fill_bps=self._adverse_fill_bps,
                exit_reason=final_reason,
                filled_at=filled_at,
                outbox_text=(
                    f"paper exit | mint={token_mint} | reason={final_reason} | value={sell_price_usd}"
                ),
                evidence=exit_evidence,
            )
            close_fill_id = str(committed["fill_id"])
            quote_id = str(committed["quote_id"])
            filled_at = committed["filled_at"]
        if isinstance(self._ledger, PaperLedger):
            closed = self._ledger.close_position(
                token_mint=token_mint,
                usd_received=sell_price_usd,
                token_closed=token_amount,
                order_id=order_id,
                quote_id=quote_id,
                exit_reason=final_reason,
                close_fill_id=close_fill_id,
                created_at=filled_at,
                price_impact_pct=quote.price_impact_pct if quote else Decimal("1"),
                platform_fee_usd=quote.platform_fee_usd if quote else Decimal("0"),
                network_fee_usd=(
                    quote.estimated_network_fee_usd if quote else Decimal("0")
                ),
                other_cost_usd=max(Decimal("0"), quoted_usd - sell_price_usd),
                adverse_fill_bps=self._adverse_fill_bps,
                quote_latency_ms=quote.latency_ms if quote else 0,
            )
        else:
            closed = self._ledger.close_position(
                token_mint=token_mint,
                usd_received=sell_price_usd,
                token_closed=token_amount,
                order_id=order_id,
                quote_id=quote_id,
                exit_reason=final_reason,
                close_fill_id=close_fill_id,
            )
        return PaperExecutionResult(
            position_id=closed.position_id,
            order_id=order_id,
            token_mint=token_mint,
            fill_id=close_fill_id,
            usd_notional=sell_price_usd,
            token_amount=token_amount,
            direction="exit",
        )
