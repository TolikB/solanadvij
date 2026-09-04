from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from .models import PositionRecord, PositionStatus


class ExitReason(StrEnum):
    NO_EXIT = "NO_EXIT"
    TP1 = "TP1"
    TP2 = "TP2"
    STOP_LOSS = "STOP_LOSS"
    TRAILING_STOP = "TRAILING_STOP"
    MOMENTUM_EXIT = "MOMENTUM_EXIT"
    TIME_STOP = "TIME_STOP"
    EMERGENCY = "EMERGENCY"


@dataclass
class ExitDecision:
    should_exit: bool
    reason: ExitReason
    executable_pnl_usd: Decimal
    executable_return_pct: Decimal
    close_fraction: Decimal = Decimal("0")


@dataclass
class ExitPolicy:
    tp1_return: Decimal = Decimal("0.30")
    tp1_size: Decimal = Decimal("0.50")
    tp2_return: Decimal = Decimal("0.60")
    tp2_size_of_initial: Decimal = Decimal("0.25")
    stop_loss_return: Decimal = Decimal("-0.15")
    trailing_stop_pct: Decimal = Decimal("0.15")
    max_hold_seconds: int | None = 600
    no_new_high_seconds: int | None = 120


def evaluate_exit(
    position: PositionRecord,
    executable_price_usd: Decimal,
    now: datetime,
    *,
    policy: ExitPolicy | None = None,
    momentum_exit: bool = False,
    emergency_exit: bool = False,
) -> ExitDecision:
    policy = policy or ExitPolicy()
    if position.status != PositionStatus.OPEN:
        return ExitDecision(False, ExitReason.NO_EXIT, Decimal("0"), Decimal("0"))

    if position.remaining_cost_usd <= 0:
        return ExitDecision(False, ExitReason.NO_EXIT, Decimal("0"), Decimal("0"))

    executable_mark = position.remaining_token_amount * executable_price_usd
    executable_pnl = executable_mark - position.remaining_cost_usd
    executable_return = executable_pnl / position.remaining_cost_usd

    if emergency_exit:
        return ExitDecision(True, ExitReason.EMERGENCY, executable_pnl, executable_return, Decimal("1"))

    if executable_return <= policy.stop_loss_return:
        return ExitDecision(True, ExitReason.STOP_LOSS, executable_pnl, executable_return, Decimal("1"))

    if not position.tp1_taken and executable_return >= policy.tp1_return:
        fraction = min(Decimal("1"), max(Decimal("0"), policy.tp1_size))
        return ExitDecision(
            True,
            ExitReason.TP1,
            executable_pnl,
            executable_return,
            fraction,
        )
    if position.tp1_taken and not position.tp2_taken and executable_return >= policy.tp2_return:
        remaining_of_initial = (
            position.remaining_token_amount / position.entry_token_amount
            if position.entry_token_amount > 0
            else Decimal("1")
        )
        fraction = min(
            Decimal("1"),
            max(
                Decimal("0"),
                policy.tp2_size_of_initial
                / max(remaining_of_initial, Decimal("0.00000001")),
            ),
        )
        return ExitDecision(True, ExitReason.TP2, executable_pnl, executable_return, fraction)

    if position.tp1_taken and (
        position.highest_executable_value_usd > 0
        or position.peak_unrealized_usd > 0
    ):
        executable_high = max(
            position.highest_executable_value_usd,
            position.remaining_cost_usd + position.peak_unrealized_usd,
        )
        trailing_floor = max(
            position.remaining_cost_usd,
            executable_high * (Decimal("1") - policy.trailing_stop_pct),
        )
        if executable_mark <= trailing_floor:
            return ExitDecision(True, ExitReason.TRAILING_STOP, executable_pnl, executable_return, Decimal("1"))

    if momentum_exit:
        return ExitDecision(True, ExitReason.MOMENTUM_EXIT, executable_pnl, executable_return, Decimal("1"))

    if policy.max_hold_seconds is not None and position.opened_at is not None:
        age_seconds = int((now - position.opened_at).total_seconds())
        if age_seconds >= policy.max_hold_seconds:
            return ExitDecision(True, ExitReason.TIME_STOP, executable_pnl, executable_return, Decimal("1"))

    if policy.no_new_high_seconds is not None and position.last_new_high_at is not None:
        stale_high_seconds = int((now - position.last_new_high_at).total_seconds())
        if stale_high_seconds >= policy.no_new_high_seconds:
            return ExitDecision(True, ExitReason.TIME_STOP, executable_pnl, executable_return, Decimal("1"))

    return ExitDecision(False, ExitReason.NO_EXIT, executable_pnl, executable_return)
