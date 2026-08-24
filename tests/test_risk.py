from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sniper_bot.config import AppConfig
from sniper_bot.models import FillRecord, FillType
from sniper_bot.risk import RiskDecision, RiskManager


class _State:
    def __init__(self, is_halted: bool = False) -> None:
        self.halt_reason: str | None = None
        if is_halted:
            self.halt_reason = "manual:initial"
        self.total_exposure_usd: Decimal = Decimal("0")

    @property
    def is_halted(self) -> bool:
        return self.halt_reason is not None

    @property
    def is_hard_halted(self) -> bool:
        return self.halt_reason is not None and self.halt_reason.startswith("hard:")

    def set_halt(self, reason: str) -> None:
        self.halt_reason = f"manual:{reason}"

    def set_hard_halt(self, reason: str) -> None:
        self.halt_reason = f"hard:{reason}"

    def clear_halt(self, *, force: bool = False) -> bool:
        if self.is_hard_halted and not force:
            return False
        self.halt_reason = None
        return True


class _LedgerForRisk:
    def __init__(
        self,
        *,
        is_halted: bool = False,
        open_positions: list[object] | None = None,
        cash_available_usd: Decimal = Decimal("500"),
        state: _State | None = None,
        can_open_return: bool = True,
        daily_loss_exceeded: bool = False,
        drawdown_exceeded: bool = False,
        fills: list[FillRecord] | None = None,
    ) -> None:
        self.state = state or _State(is_halted=is_halted)
        self._open_positions = list(open_positions or [])
        self._cash_available_usd = cash_available_usd
        self._can_open_return = can_open_return
        self._daily_loss_exceeded = daily_loss_exceeded
        self._drawdown_exceeded = drawdown_exceeded
        self._fills = list(fills or [])

    @property
    def open_positions(self) -> list[object]:
        return self._open_positions

    @property
    def cash_available_usd(self) -> Decimal:
        return self._cash_available_usd

    def can_open(self, token_mint: str) -> bool:
        return self._can_open_return

    def daily_loss_exceeded(self, limit: Decimal) -> bool:  # noqa: ARG002
        return self._daily_loss_exceeded

    def drawdown_exceeded(self, limit: Decimal) -> bool:  # noqa: ARG002
        return self._drawdown_exceeded

    def iter_fills(self) -> list[FillRecord]:
        return list(self._fills)


def _build_risk(
    *,
    trade_limit: int,
    loss_limit: int,
    fills: list[FillRecord] | None = None,
    drawdown_exceeded: bool = False,
    state: _State | None = None,
) -> RiskManager:
    cfg = AppConfig(
        APP_MODE="paper",
        HELIUS_API_KEY="helius",
        JUPITER_API_KEY="jupiter",
        POSTGRES_DSN="postgresql://user:pass@localhost:5432/db",
        TELEGRAM_BOT_TOKEN="tg",
        TELEGRAM_ADMIN_CHAT_ID=123456,
        STARTING_EQUITY_USD=Decimal("500"),
    )
    cfg.risk.max_trades_per_day = trade_limit
    cfg.risk.max_consecutive_losses = loss_limit

    return RiskManager(
        cfg.risk,
        _LedgerForRisk(
            state=state,
            drawdown_exceeded=drawdown_exceeded,
            fills=fills or [],
        ),
    )


def _entry_fill(created_at: datetime) -> FillRecord:
    return FillRecord(
        fill_id="entry-fill-id",
        token_mint="TOKEN",
        order_id="order-id",
        quote_id="quote-id",
        fill_type=FillType.ENTRY,
        token_amount=Decimal("1"),
        usd_notional=Decimal("10"),
        cost_basis_usd=Decimal("10"),
        created_at=created_at,
    )


def _loss_fill(created_at: datetime) -> FillRecord:
    return FillRecord(
        fill_id="loss-fill-id",
        token_mint="TOKEN",
        order_id="order-id",
        quote_id="quote-id",
        fill_type=FillType.EXIT,
        token_amount=Decimal("1"),
        usd_notional=Decimal("1"),
        cost_basis_usd=Decimal("2"),
        created_at=created_at,
    )


def _profit_fill(created_at: datetime) -> FillRecord:
    return FillRecord(
        fill_id="profit-fill-id",
        token_mint="TOKEN",
        order_id="order-id",
        quote_id="quote-id",
        fill_type=FillType.EXIT,
        token_amount=Decimal("1"),
        usd_notional=Decimal("3"),
        cost_basis_usd=Decimal("2"),
        created_at=created_at,
    )


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def test_risk_blocks_when_daily_entry_limit_reached() -> None:
    now = _now_utc().replace(hour=10, minute=0, second=0, microsecond=0)
    yesterday = now - timedelta(days=1)
    fills = [_entry_fill(yesterday), _entry_fill(now), _entry_fill(now)]

    risk_manager = _build_risk(
        trade_limit=2,
        loss_limit=3,
        fills=fills,
    )
    result = risk_manager.evaluate_entry(Decimal("5"))

    assert result.decision == RiskDecision.BLOCK
    assert result.reason == "MAX_TRADES_PER_DAY"


def test_risk_allows_within_daily_entry_limit() -> None:
    now = _now_utc().replace(hour=10, minute=0, second=0, microsecond=0)
    fills = [_entry_fill(now)]

    risk_manager = _build_risk(
        trade_limit=3,
        loss_limit=3,
        fills=fills,
    )
    result = risk_manager.evaluate_entry(Decimal("5"))

    assert result.decision == RiskDecision.ALLOW


def test_risk_blocks_when_consecutive_losses_reach_limit() -> None:
    now = _now_utc().replace(hour=10, minute=0, second=0, microsecond=0)
    fills = [_loss_fill(now), _loss_fill(now), _loss_fill(now)]

    risk_manager = _build_risk(
        trade_limit=10,
        loss_limit=3,
        fills=fills,
    )
    result = risk_manager.evaluate_entry(Decimal("5"))

    assert result.decision == RiskDecision.BLOCK
    assert result.reason == "CONSECUTIVE_LOSSES"


def test_risk_allows_after_positive_fill_breaks_loss_streak() -> None:
    now = _now_utc().replace(hour=10, minute=0, second=0, microsecond=0)
    fills = [_loss_fill(now), _profit_fill(now)]

    risk_manager = _build_risk(
        trade_limit=10,
        loss_limit=1,
        fills=fills,
    )
    result = risk_manager.evaluate_entry(Decimal("5"))

    assert result.decision == RiskDecision.ALLOW


def test_risk_hard_drawdown_halt_is_not_cleared_by_resume() -> None:
    state = _State()
    risk_manager = _build_risk(
        trade_limit=10,
        loss_limit=3,
        drawdown_exceeded=True,
        state=state,
        fills=[],
    )

    result = risk_manager.evaluate_entry(Decimal("5"))

    assert result.decision == RiskDecision.BLOCK
    assert result.reason == "ALL_TIME_DRAWDOWN_LIMIT"
    assert state.is_hard_halted
    assert risk_manager.clear_halt() is False


def test_risk_manual_halt_can_be_resumed() -> None:
    state = _State()
    risk_manager = _build_risk(
        trade_limit=10,
        loss_limit=3,
        state=state,
        fills=[],
    )
    risk_manager.set_halt("operator_pause")

    assert state.is_halted
    assert risk_manager.clear_halt() is True
    assert not state.is_halted
