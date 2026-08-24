from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sniper_bot.exit_engine import ExitPolicy, ExitReason, evaluate_exit
from sniper_bot.models import PositionRecord, PositionStatus


def _position(
    *,
    token_amount: Decimal = Decimal("10"),
    cost_usd: Decimal = Decimal("100"),
    open_at: datetime | None = None,
    opened_at_offset_sec: int = 0,
    peak_unrealized: Decimal = Decimal("0"),
    status: PositionStatus = PositionStatus.OPEN,
) -> PositionRecord:
    opened_at = open_at or datetime(2026, 8, 18, tzinfo=timezone.utc)
    if opened_at_offset_sec:
        opened_at = opened_at + timedelta(seconds=opened_at_offset_sec)
    return PositionRecord(
        position_id="pos-1",
        token_mint="TOKEN",
        open_fill_id="fill-open",
        entry_token_amount=token_amount,
        entry_cost_usd=cost_usd,
        open_ratio=cost_usd / token_amount,
        opened_at=opened_at,
        locked_usd=cost_usd,
        status=status,
        remaining_token_amount=token_amount,
        remaining_cost_usd=cost_usd,
        realized_pnl_usd=Decimal("0"),
        peak_unrealized_usd=peak_unrealized,
    )


def test_exit_engine_emits_no_exit_below_thresholds() -> None:
    position = _position()
    decision = evaluate_exit(position, executable_price_usd=Decimal("10.02"), now=position.opened_at)

    assert decision.should_exit is False
    assert decision.reason == ExitReason.NO_EXIT


def test_exit_engine_tp1_triggered() -> None:
    position = _position()
    decision = evaluate_exit(position, executable_price_usd=Decimal("13.10"), now=position.opened_at)

    assert decision.should_exit is True
    assert decision.reason == ExitReason.TP1


def test_exit_engine_tp1_is_not_skipped_when_price_already_above_tp2() -> None:
    position = _position()
    decision = evaluate_exit(position, executable_price_usd=Decimal("16.10"), now=position.opened_at)

    assert decision.should_exit is True
    assert decision.reason == ExitReason.TP1


def test_exit_engine_tp2_after_tp1() -> None:
    position = _position()
    position.tp1_taken = True
    decision = evaluate_exit(position, executable_price_usd=Decimal("16.20"), now=position.opened_at)

    assert decision.should_exit is True
    assert decision.reason == ExitReason.TP2


def test_exit_engine_trailing_stop_beats_time_stop() -> None:
    opened = datetime(2026, 8, 18, tzinfo=timezone.utc)
    position = _position(
        peak_unrealized=Decimal("40"),
        open_at=opened,
    )
    position.tp1_taken = True
    decision = evaluate_exit(
        position,
        executable_price_usd=Decimal("10.31"),
        now=opened + timedelta(seconds=100000),
        policy=ExitPolicy(max_hold_seconds=10, tp1_return=Decimal("0.50"), tp2_return=Decimal("1.00")),
    )

    assert decision.should_exit is True
    assert decision.reason == ExitReason.TRAILING_STOP


def test_exit_engine_stop_loss_triggered() -> None:
    position = _position()
    decision = evaluate_exit(
        position,
        executable_price_usd=Decimal("8.90"),
        now=position.opened_at,
        policy=ExitPolicy(stop_loss_return=Decimal("-0.10")),
    )

    assert decision.should_exit is True
    assert decision.reason == ExitReason.STOP_LOSS


def test_exit_engine_time_stop_when_expired() -> None:
    opened = datetime(2026, 8, 18, tzinfo=timezone.utc)
    position = _position(open_at=opened)
    decision = evaluate_exit(
        position,
        executable_price_usd=Decimal("10.02"),
        now=opened + timedelta(seconds=900),
        policy=ExitPolicy(max_hold_seconds=600),
    )

    assert decision.should_exit is True
    assert decision.reason == ExitReason.TIME_STOP
