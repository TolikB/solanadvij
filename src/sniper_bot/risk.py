"""Risk checks for paper mode."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import StrEnum
from typing import Protocol

from .errors import ExecutionBlockedError
from .models import FillRecord, FillType, PositionRecord


class RiskDecision(StrEnum):
    ALLOW = "allow"
    BLOCK = "block"


@dataclass
class RiskDecisionResult:
    decision: RiskDecision
    reason: str | None = None


@dataclass(frozen=True)
class AtomicEntryLimits:
    max_position_usdc: Decimal
    max_exposure_usdc: Decimal
    max_open_positions: int
    daily_loss_limit_usdc: Decimal
    max_trades_per_day: int
    day_key: str
    day_start: datetime
    day_end: datetime


class RiskManager:
    def __init__(self, risk: "RiskConfig", ledger: "AnyLedger") -> None:
        self._risk = risk
        self._ledger = ledger

    def evaluate_entry(self, notional_usdc: Decimal, token_mint: str | None = None) -> RiskDecisionResult:
        if not self._ledger:
            raise ExecutionBlockedError("ledger unavailable")
        if notional_usdc <= 0:
            raise ExecutionBlockedError("notional must be positive")

        now = datetime.now(tz=timezone.utc)
        today = (
            self._ledger.current_date_key(now)
            if hasattr(self._ledger, "current_date_key")
            else now.strftime("%Y-%m-%d")
        )
        if hasattr(self._ledger, "clear_expired_daily_controls"):
            self._ledger.clear_expired_daily_controls(today, now)
        if self._ledger.state.is_halted:
            return RiskDecisionResult(RiskDecision.BLOCK, "HALTED_BY_RISK_MANAGER")
        pause_until = getattr(self._ledger.state, "pause_until", None)
        if pause_until is not None and pause_until > now:
            return RiskDecisionResult(RiskDecision.BLOCK, "CONSECUTIVE_LOSS_PAUSE")
        if notional_usdc > self._risk.max_position_usdc:
            return RiskDecisionResult(RiskDecision.BLOCK, "MAX_POSITION_LIMIT")
        if self._ledger.state.total_exposure_usd + notional_usdc > self._risk.max_exposure_usdc:
            return RiskDecisionResult(RiskDecision.BLOCK, "MAX_EXPOSURE_LIMIT")
        if len(self._ledger.open_positions) >= self._risk.max_open_positions:
            return RiskDecisionResult(RiskDecision.BLOCK, "MAX_OPEN_POSITIONS_LIMIT")
        if self._ledger.cash_available_usd < notional_usdc:
            return RiskDecisionResult(RiskDecision.BLOCK, "INSUFFICIENT_CASH")
        if self._ledger.daily_loss_exceeded(self._risk.daily_loss_limit_usdc):
            if hasattr(self._ledger, "set_daily_halt"):
                self._ledger.set_daily_halt(today, "DAILY_LOSS_LIMIT")
            return RiskDecisionResult(RiskDecision.BLOCK, "DAILY_LOSS_LIMIT")
        if self.trades_today() >= self._risk.max_trades_per_day:
            return RiskDecisionResult(RiskDecision.BLOCK, "MAX_TRADES_PER_DAY")
        if self._ledger.drawdown_exceeded(self._risk.all_time_drawdown_limit_pct):
            self.set_hard_halt("ALL_TIME_DRAWDOWN_LIMIT")
            return RiskDecisionResult(RiskDecision.BLOCK, "ALL_TIME_DRAWDOWN_LIMIT")
        consecutive_losses = self.consecutive_losses()
        if consecutive_losses >= self._risk.daily_halt_after_consecutive_losses:
            if hasattr(self._ledger, "set_daily_halt"):
                self._ledger.set_daily_halt(today, "CONSECUTIVE_LOSSES")
            return RiskDecisionResult(RiskDecision.BLOCK, "CONSECUTIVE_LOSS_DAILY_HALT")
        if consecutive_losses >= self._risk.max_consecutive_losses:
            if hasattr(self._ledger, "set_pause_until"):
                self._ledger.set_pause_until(now + timedelta(minutes=self._risk.pause_minutes))
            return RiskDecisionResult(RiskDecision.BLOCK, "CONSECUTIVE_LOSSES")
        if token_mint is not None and not self._ledger.can_open(token_mint):
            return RiskDecisionResult(RiskDecision.BLOCK, "DUPLICATE_TOKEN")
        return RiskDecisionResult(RiskDecision.ALLOW, None)

    def set_halt(self, reason: str) -> None:
        setter = getattr(self._ledger, "set_halt", None)
        if callable(setter):
            setter(reason)
        else:
            self._ledger.state.set_halt(reason)

    def set_hard_halt(self, reason: str) -> None:
        setter = getattr(self._ledger, "set_halt", None)
        if callable(setter):
            setter(reason, hard=True)
            return
        state = self._ledger.state
        if hasattr(state, "set_hard_halt"):
            state.set_hard_halt(reason)
        else:
            state.set_halt(f"hard:{reason}")

    def clear_halt(self) -> bool:
        ledger_clear = getattr(self._ledger, "clear_halt", None)
        if callable(ledger_clear):
            return bool(ledger_clear())
        state = self._ledger.state
        clear = getattr(state, "clear_halt", None)
        if clear is None:
            return False
        return bool(clear())

    def trades_today(self, date_key: str | None = None) -> int:
        if not hasattr(self._ledger, "iter_fills"):
            return 0
        fills = self._ledger.iter_fills()
        target = date_key or self._today_key()
        return sum(
            1
            for fill in fills
            if fill.fill_type == FillType.ENTRY
            and self._date_key(fill.created_at) == target
        )

    def consecutive_losses(self) -> int:
        if not hasattr(self._ledger, "iter_fills"):
            return 0
        fills = self._ledger.iter_fills()
        consecutive = 0
        for fill in reversed(list(fills)):
            if fill.fill_type != FillType.EXIT:
                continue
            realized_delta = fill.usd_notional - (fill.cost_basis_usd * fill.token_amount)
            if realized_delta < 0:
                consecutive += 1
                continue
            break
        return consecutive

    @staticmethod
    def _utc_today_key() -> str:
        return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    def _today_key(self) -> str:
        current_date_key = getattr(self._ledger, "current_date_key", None)
        if callable(current_date_key):
            return str(current_date_key())
        return self._utc_today_key()

    def _date_key(self, at: datetime) -> str:
        current_date_key = getattr(self._ledger, "current_date_key", None)
        if callable(current_date_key):
            return str(current_date_key(at))
        return at.astimezone(timezone.utc).date().isoformat()

    def atomic_entry_limits(self, at: datetime) -> AtomicEntryLimits:
        day_bounds = getattr(self._ledger, "day_bounds_utc", None)
        if callable(day_bounds):
            day_start, day_end = day_bounds(at)
        else:
            day_start = at.replace(hour=0, minute=0, second=0, microsecond=0)
            day_end = day_start + timedelta(days=1)
        return AtomicEntryLimits(
            max_position_usdc=self._risk.max_position_usdc,
            max_exposure_usdc=self._risk.max_exposure_usdc,
            max_open_positions=self._risk.max_open_positions,
            daily_loss_limit_usdc=self._risk.daily_loss_limit_usdc,
            max_trades_per_day=self._risk.max_trades_per_day,
            day_key=self._date_key(at),
            day_start=day_start,
            day_end=day_end,
        )


class RiskConfig(Protocol):
    # Structural typing compatibility for IDE and dependency-free circular imports.
    max_position_usdc: Decimal
    max_exposure_usdc: Decimal
    max_open_positions: int
    daily_loss_limit_usdc: Decimal
    all_time_drawdown_limit_pct: Decimal
    max_trades_per_day: int
    max_consecutive_losses: int
    daily_halt_after_consecutive_losses: int
    pause_minutes: int


class LedgerState(Protocol):
    @property
    def is_halted(self) -> bool: ...

    @property
    def total_exposure_usd(self) -> Decimal: ...

    @property
    def pause_until(self) -> datetime | None: ...

    def set_halt(self, reason: str) -> None: ...
    def clear_halt(self, *, force: bool = False) -> bool: ...


class AnyLedger(Protocol):
    @property
    def state(self) -> LedgerState: ...

    @property
    def open_positions(self) -> Sequence[PositionRecord]: ...

    @property
    def cash_available_usd(self) -> Decimal: ...

    def can_open(self, token_mint: str) -> bool: ...
    def daily_loss_exceeded(self, limit: Decimal) -> bool: ...
    def drawdown_exceeded(self, limit: Decimal) -> bool: ...
    def iter_fills(self) -> Iterable[FillRecord]: ...
    def current_date_key(self, at: datetime | None = None) -> str: ...
    def day_bounds_utc(self, at: datetime) -> tuple[datetime, datetime]: ...
    def clear_expired_daily_controls(self, date_key: str, now: datetime) -> None: ...
    def set_daily_halt(self, date_key: str, reason: str) -> None: ...
    def set_pause_until(self, until: datetime) -> None: ...
