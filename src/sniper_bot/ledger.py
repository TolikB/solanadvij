"""Paper ledger with deterministic recovery and simple PnL accounting."""

from __future__ import annotations

import json
import logging
import os
import threading
from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4
from zoneinfo import ZoneInfo

from .models import FillRecord, FillType, PositionRecord, PositionStatus

logger = logging.getLogger(__name__)


class LedgerState:
    """Serializable runtime state for paper trading."""

    def __init__(self, starting_equity_usd: Decimal, strategy_version: str, config_hash: str) -> None:
        self.equity_usd = Decimal(starting_equity_usd)
        self.peak_equity_usd = Decimal(starting_equity_usd)
        self.realized_pnl_usd = Decimal("0")
        self.unrealized_pnl_usd = Decimal("0")
        self.locked_capital_usd = Decimal("0")
        self.strategy_version = strategy_version
        self.config_hash = config_hash
        self.positions: dict[str, PositionRecord] = {}
        self.fills: list[FillRecord] = []
        self.daily_pnl: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
        self.daily_equity_baseline: dict[str, Decimal] = {}
        self.halt_reason: str | None = None
        self.pause_until: datetime | None = None
        self.daily_halt_date: str | None = None
        self._order_id_index: dict[str, str] = {}
        self._close_order_id_index: dict[str, str] = {}
        self._close_fill_id_index: dict[str, str] = {}
        self._token_index: dict[str, str] = {}

    @property
    def open_positions(self) -> list[PositionRecord]:
        return [position for position in self.positions.values() if position.status == PositionStatus.OPEN]

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
        if (
            self.halt_reason is not None
            and (
                self.halt_reason.startswith("hard:")
                or self.halt_reason.startswith("daily:")
            )
            and not force
        ):
            return False
        self.halt_reason = None
        return True

    def clear_hard_halt(self) -> bool:
        return self.clear_halt(force=True)

    @property
    def total_exposure_usd(self) -> Decimal:
        return self.locked_capital_usd

    @property
    def open_positions_count(self) -> int:
        return len(self.open_positions)

    @property
    def open_positions_notional_usd(self) -> Decimal:
        return sum(
            (position.remaining_cost_usd for position in self.open_positions),
            Decimal("0"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "equity_usd": str(self.equity_usd),
            "peak_equity_usd": str(self.peak_equity_usd),
            "realized_pnl_usd": str(self.realized_pnl_usd),
            "unrealized_pnl_usd": str(self.unrealized_pnl_usd),
            "locked_capital_usd": str(self.locked_capital_usd),
            "strategy_version": self.strategy_version,
            "config_hash": self.config_hash,
            "positions": [position.model_dump(mode="json") for position in self.positions.values()],
            "fills": [fill.model_dump(mode="json") for fill in self.fills],
            "daily_pnl": {k: str(v) for k, v in self.daily_pnl.items()},
            "daily_equity_baseline": {
                k: str(v) for k, v in self.daily_equity_baseline.items()
            },
            "halt_reason": self.halt_reason,
            "pause_until": self.pause_until.isoformat() if self.pause_until else None,
            "daily_halt_date": self.daily_halt_date,
            "order_id_index": self._order_id_index,
            "close_order_id_index": self._close_order_id_index,
            "close_fill_id_index": self._close_fill_id_index,
            "token_index": self._token_index,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LedgerState":
        state = cls(
            starting_equity_usd=Decimal(data["equity_usd"]),
            strategy_version=data.get("strategy_version", ""),
            config_hash=data.get("config_hash", ""),
        )
        state.peak_equity_usd = Decimal(data.get("peak_equity_usd", "0"))
        state.realized_pnl_usd = Decimal(data.get("realized_pnl_usd", "0"))
        state.unrealized_pnl_usd = Decimal(data.get("unrealized_pnl_usd", "0"))
        state.locked_capital_usd = Decimal(data.get("locked_capital_usd", "0"))
        state.halt_reason = data.get("halt_reason")
        pause_until = data.get("pause_until")
        state.pause_until = datetime.fromisoformat(pause_until) if pause_until else None
        state.daily_halt_date = data.get("daily_halt_date")
        state.daily_pnl = defaultdict(lambda: Decimal("0"), {k: Decimal(v) for k, v in (data.get("daily_pnl") or {}).items()})
        state.daily_equity_baseline = {
            k: Decimal(v)
            for k, v in (data.get("daily_equity_baseline") or {}).items()
        }
        state._order_id_index = dict(data.get("order_id_index", {}))
        state._close_order_id_index = dict(data.get("close_order_id_index", {}))
        state._close_fill_id_index = dict(data.get("close_fill_id_index", {}))
        state._token_index = dict(data.get("token_index", {}))

        for position_data in data.get("positions", []):
            position = PositionRecord(**position_data)
            state.positions[position.position_id] = position
        for fill_data in data.get("fills", []):
            fill = FillRecord(**fill_data)
            state.fills.append(fill)
        return state


class PaperLedger:
    """Simple persistence-based ledger for MVP paper-mode accounting."""

    def __init__(
        self,
        storage_path: Path,
        starting_equity_usd: Decimal,
        strategy_version: str,
        config_hash: str,
        *,
        id_factory: Callable[[], str] | None = None,
        time_zone: str = "UTC",
    ):
        self.storage_path = storage_path
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._id_factory = id_factory or (lambda: str(uuid4()))
        self._time_zone = ZoneInfo(time_zone)
        self._state = self._load_or_init(starting_equity_usd, strategy_version, config_hash)

    def _load_or_init(self, starting_equity_usd: Decimal, strategy_version: str, config_hash: str) -> LedgerState:
        if self.storage_path.exists():
            try:
                with self.storage_path.open("r", encoding="utf-8") as file:
                    raw = json.load(file)
                state = LedgerState.from_dict(raw)
                state.strategy_version = strategy_version
                state.config_hash = config_hash
                return state
            except (OSError, ValueError, TypeError, KeyError):
                logger.exception(
                    "local paper ledger mirror is unreadable; awaiting authoritative recovery"
                )
        return LedgerState(starting_equity_usd=starting_equity_usd, strategy_version=strategy_version, config_hash=config_hash)

    def _persist(self) -> None:
        temporary = self.storage_path.with_suffix(self.storage_path.suffix + ".tmp")
        with temporary.open("w", encoding="utf-8") as file:
            json.dump(self._state.to_dict(), file, ensure_ascii=False, indent=2)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, self.storage_path)

    @property
    def state(self) -> LedgerState:
        with self._lock:
            return self._state

    @property
    def cash_available_usd(self) -> Decimal:
        with self._lock:
            return (
                self._state.equity_usd
                - self._state.unrealized_pnl_usd
                - self._state.locked_capital_usd
            )

    @property
    def open_positions(self) -> list[PositionRecord]:
        with self._lock:
            return list(self._state.open_positions)

    def mark_to_market(
        self,
        prices_usd_per_token: dict[str, Decimal],
        *,
        observed_at: datetime | None = None,
    ) -> tuple[Decimal, Decimal]:
        """Recompute unrealized PnL from prices and return (unrealized, equity)."""
        with self._lock:
            unrealized = Decimal("0")
            realized_component = self.realized_base_equity()
            mark_time = observed_at or datetime.now(tz=timezone.utc)
            today = self.current_date_key(mark_time)
            for position in self._state.positions.values():
                if position.status != PositionStatus.OPEN:
                    continue
                price = prices_usd_per_token.get(position.token_mint)
                if price is None:
                    continue
                mark = position.remaining_token_amount * price
                position.last_executable_value_usd = mark
                unrealized += mark - position.remaining_cost_usd
                position.peak_unrealized_usd = max(position.peak_unrealized_usd, mark - position.remaining_cost_usd)
                if mark > position.highest_executable_value_usd:
                    position.highest_executable_value_usd = mark
                    position.last_new_high_at = mark_time
                position.lowest_executable_value_usd = (
                    mark
                    if position.lowest_executable_value_usd is None
                    else min(position.lowest_executable_value_usd, mark)
                )
                if position.remaining_cost_usd > 0:
                    current_return = (mark - position.remaining_cost_usd) / position.remaining_cost_usd
                    position.mfe_pct = max(position.mfe_pct, current_return)
                    position.mae_pct = min(position.mae_pct, current_return)
            self._state.unrealized_pnl_usd = unrealized
            equity = realized_component + unrealized
            self._state.equity_usd = equity
            self._state.daily_equity_baseline.setdefault(today, equity)
            self._state.peak_equity_usd = max(self._state.peak_equity_usd, self._state.equity_usd)
            self._persist()
            return unrealized, equity

    def realized_base_equity(self) -> Decimal:
        # Equity excludes unrealized PnL and contains locked capital + realized PnL changes.
        return self._state.equity_usd - self._state.unrealized_pnl_usd

    def daily_pnl(self, date_key: str) -> Decimal:
        with self._lock:
            return self._state.daily_pnl.get(date_key, Decimal("0"))

    def can_open(self, token_mint: str) -> bool:
        with self._lock:
            return self._state._token_index.get(token_mint) is None

    def set_pause_until(self, until: datetime | None) -> None:
        with self._lock:
            self._state.pause_until = until
            self._persist()

    def set_halt(self, reason: str, *, hard: bool = False) -> None:
        with self._lock:
            if hard:
                self._state.set_hard_halt(reason)
            else:
                self._state.set_halt(reason)
            self._persist()

    def clear_halt(self, *, force: bool = False) -> bool:
        with self._lock:
            cleared = self._state.clear_halt(force=force)
            if cleared:
                self._persist()
            return cleared

    def set_daily_halt(self, date_key: str, reason: str) -> None:
        with self._lock:
            self._state.daily_halt_date = date_key
            self._state.halt_reason = f"daily:{date_key}:{reason}"
            self._persist()

    def clear_expired_daily_controls(self, date_key: str, now: datetime) -> None:
        with self._lock:
            changed = False
            if self._state.pause_until is not None and self._state.pause_until <= now:
                self._state.pause_until = None
                changed = True
            if self._state.daily_halt_date and self._state.daily_halt_date != date_key:
                if self._state.halt_reason and self._state.halt_reason.startswith("daily:"):
                    self._state.halt_reason = None
                self._state.daily_halt_date = None
                changed = True
            if changed:
                self._persist()

    def get_fill_by_order(self, order_id: str) -> FillRecord | None:
        with self._lock:
            for fill in reversed(self._state.fills):
                if fill.order_id == order_id:
                    return fill
            return None

    def get_position_by_order(self, order_id: str) -> PositionRecord | None:
        with self._lock:
            position_id = self._state._order_id_index.get(order_id)
            if position_id:
                return self._state.positions[position_id]
            position_id = self._state._close_order_id_index.get(order_id)
            if position_id:
                return self._state.positions[position_id]
            return None

    def open_position(
        self,
        token_mint: str,
        usd_amount: Decimal,
        token_amount: Decimal,
        order_id: str,
        quote_id: str,
        *,
        position_id: str | None = None,
        fill_id: str | None = None,
        created_at: datetime | None = None,
        candidate_id: str | None = None,
        pool_address: str | None = None,
        entry_score: Decimal | None = None,
        entry_liquidity_usd: Decimal | None = None,
        entry_pool_age_seconds: Decimal | None = None,
        strategy_version: str | None = None,
        price_impact_pct: Decimal = Decimal("0"),
        platform_fee_usd: Decimal = Decimal("0"),
        network_fee_usd: Decimal = Decimal("0"),
        other_cost_usd: Decimal = Decimal("0"),
        adverse_fill_bps: int = 0,
        quote_latency_ms: int = 0,
        round_trip_cost_pct: Decimal | None = None,
    ) -> PositionRecord:
        if usd_amount <= 0:
            raise ValueError("usd_amount must be positive")
        if token_amount <= 0:
            raise ValueError("token_amount must be positive")

        with self._lock:
            if order_id in self._state._order_id_index:
                return self._state.positions[self._state._order_id_index[order_id]]
            existing_position = self._state._token_index.get(token_mint)
            if existing_position:
                return self._state.positions[existing_position]

            position_id = position_id or self._id_factory()
            now = created_at or datetime.now(tz=timezone.utc)
            fill_id = fill_id or self._id_factory()
            entry_cost = usd_amount + network_fee_usd
            entry_fill = FillRecord(
                fill_id=fill_id,
                position_id=position_id,
                token_mint=token_mint,
                order_id=order_id,
                quote_id=quote_id,
                fill_type=FillType.ENTRY,
                token_amount=token_amount,
                usd_notional=entry_cost,
                cost_basis_usd=entry_cost / token_amount if token_amount else Decimal("0"),
                created_at=now,
                price_impact_pct=price_impact_pct,
                platform_fee_usd=platform_fee_usd,
                network_fee_usd=network_fee_usd,
                other_cost_usd=other_cost_usd,
                adverse_fill_bps=adverse_fill_bps,
                quote_latency_ms=quote_latency_ms,
                round_trip_cost_pct=round_trip_cost_pct,
            )
            position = PositionRecord(
                position_id=position_id,
                token_mint=token_mint,
                open_fill_id=fill_id,
                entry_token_amount=token_amount,
                entry_cost_usd=entry_cost,
                open_ratio=entry_fill.cost_basis_usd,
                opened_at=now,
                locked_usd=entry_cost,
                status=PositionStatus.OPEN,
                remaining_token_amount=token_amount,
                remaining_cost_usd=entry_cost,
                candidate_id=candidate_id,
                pool_address=pool_address,
                entry_score=entry_score,
                entry_liquidity_usd=entry_liquidity_usd,
                entry_pool_age_seconds=entry_pool_age_seconds,
                strategy_version=strategy_version,
            )
            self._state.fills.append(entry_fill)
            self._state.positions[position_id] = position
            self._state._order_id_index[order_id] = position_id
            self._state._token_index[token_mint] = position_id
            self._state.locked_capital_usd += entry_cost
            self._persist()
            return position

    def close_position(
        self,
        token_mint: str,
        usd_received: Decimal,
        token_closed: Decimal,
        order_id: str,
        quote_id: str,
        exit_reason: str | None = None,
        close_fill_id: str | None = None,
        created_at: datetime | None = None,
        price_impact_pct: Decimal = Decimal("0"),
        platform_fee_usd: Decimal = Decimal("0"),
        network_fee_usd: Decimal = Decimal("0"),
        other_cost_usd: Decimal = Decimal("0"),
        adverse_fill_bps: int = 0,
        quote_latency_ms: int = 0,
    ) -> PositionRecord:
        if usd_received < 0 or token_closed < 0:
            raise ValueError("usd_received and token_closed must be non-negative")
        if token_closed == 0:
            raise ValueError("token_closed must be positive")

        with self._lock:
            if order_id in self._state._close_order_id_index:
                _ = self._state._close_fill_id_index.get(order_id)
                return self._state.positions[self._state._close_order_id_index[order_id]]
            position_id = self._state._token_index.get(token_mint)
            if not position_id:
                raise ValueError("position not found for token")
            position = self._state.positions[position_id]
            if position.status != PositionStatus.OPEN:
                return position
            if token_closed > position.remaining_token_amount:
                raise ValueError("token_closed exceeds remaining position amount")

            now = created_at or datetime.now(tz=timezone.utc)
            close_fill_id = close_fill_id or self._id_factory()
            net_received = max(Decimal("0"), usd_received - network_fee_usd)
            close_fill = FillRecord(
                fill_id=close_fill_id,
                position_id=position_id,
                token_mint=token_mint,
                order_id=order_id,
                quote_id=quote_id,
                fill_type=FillType.EXIT,
                token_amount=token_closed,
                usd_notional=net_received,
                cost_basis_usd=position.remaining_cost_usd / position.remaining_token_amount if position.remaining_token_amount else Decimal("0"),
                created_at=now,
                exit_reason=exit_reason,
                price_impact_pct=price_impact_pct,
                platform_fee_usd=platform_fee_usd,
                network_fee_usd=network_fee_usd,
                other_cost_usd=other_cost_usd,
                adverse_fill_bps=adverse_fill_bps,
                quote_latency_ms=quote_latency_ms,
            )
            self._state.fills.append(close_fill)

            closed_fraction = token_closed / position.remaining_token_amount
            remaining_fraction = Decimal("1") - closed_fraction
            proportional_cost = position.remaining_cost_usd * closed_fraction
            marked_unrealized = (
                position.last_executable_value_usd - position.remaining_cost_usd
                if position.last_executable_value_usd > 0
                else Decimal("0")
            )
            closed_unrealized = marked_unrealized * closed_fraction
            position.remaining_cost_usd -= proportional_cost
            position.remaining_token_amount -= token_closed
            realized_delta = net_received - proportional_cost
            position.realized_pnl_usd += realized_delta
            position.last_executable_value_usd *= remaining_fraction
            position.highest_executable_value_usd *= remaining_fraction
            if position.lowest_executable_value_usd is not None:
                position.lowest_executable_value_usd *= remaining_fraction
            position.peak_unrealized_usd *= remaining_fraction
            if exit_reason == "TP1":
                position.tp1_taken = True
            elif exit_reason == "TP2":
                position.tp2_taken = True

            self._state.realized_pnl_usd += realized_delta
            self._state.unrealized_pnl_usd -= closed_unrealized
            self._state.locked_capital_usd -= proportional_cost

            if position.remaining_token_amount <= 0:
                position.status = PositionStatus.CLOSED
                position.closed_at = now
                self._state._token_index.pop(token_mint, None)
                position.final_exit_reason = exit_reason or "UNKNOWN_EXIT"

            self._state.equity_usd += realized_delta - closed_unrealized
            today = self.current_date_key(now)
            self._state.daily_pnl[today] += realized_delta
            self._state.peak_equity_usd = max(self._state.peak_equity_usd, self._state.equity_usd)
            self._state.positions[position_id] = position
            self._state._close_order_id_index[order_id] = position_id
            self._state._close_fill_id_index[order_id] = close_fill_id
            self._persist()
            return position

    def daily_loss_exceeded(
        self, limit: Decimal, *, at: datetime | None = None
    ) -> bool:
        today = self.current_date_key(at)
        baseline = self._state.daily_equity_baseline.setdefault(
            today, self._state.equity_usd
        )
        return self._state.equity_usd - baseline <= -abs(limit)

    def set_daily_equity_baseline(self, date_key: str, equity: Decimal) -> None:
        with self._lock:
            self._state.daily_equity_baseline[date_key] = Decimal(equity)
            self._persist()

    def current_date_key(self, at: datetime | None = None) -> str:
        current = at or datetime.now(tz=timezone.utc)
        return current.astimezone(self._time_zone).date().isoformat()

    def day_bounds_utc(self, at: datetime) -> tuple[datetime, datetime]:
        local = at.astimezone(self._time_zone)
        start = datetime(local.year, local.month, local.day, tzinfo=self._time_zone)
        end = start + timedelta(days=1)
        return start.astimezone(timezone.utc), end.astimezone(timezone.utc)

    def drawdown_exceeded(self, drawdown_limit_pct: Decimal) -> bool:
        peak = self._state.peak_equity_usd
        if peak <= 0:
            return False
        current = self._state.equity_usd
        if current >= peak:
            return False
        drawdown_pct = (peak - current) / peak * Decimal("100")
        return drawdown_pct >= abs(drawdown_limit_pct)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return self._state.to_dict()

    def reconcile(self) -> dict[str, object]:
        with self._lock:
            realized = sum(
                (
                    fill.usd_notional - (fill.cost_basis_usd * fill.token_amount)
                    for fill in self._state.fills
                    if fill.fill_type == FillType.EXIT
                ),
                Decimal("0"),
            )
            total_exit_notional = sum(
                (
                    fill.usd_notional
                    for fill in self._state.fills
                    if fill.fill_type == FillType.EXIT
                ),
                Decimal("0"),
            )
            return {
                "equity_usd": self._state.equity_usd,
                "realized_pnl_usd": self._state.realized_pnl_usd,
                "expected_realized_pnl_usd": realized,
                "open_positions_count": self._state.open_positions_count,
                "open_positions_notional_usd": self._state.open_positions_notional_usd,
                "total_exit_notional_usd": total_exit_notional,
                "is_reconciled": realized == self._state.realized_pnl_usd,
            }

    def iter_fills(self) -> Iterable[FillRecord]:
        with self._lock:
            return list(self._state.fills)

    def restore_from_database(self, payload: dict[str, Any]) -> None:
        """Replace the local mirror with a previously committed database state."""
        account = payload["account"]
        orders = {item["id"]: item for item in payload.get("orders", [])}
        fills = payload.get("fills", [])
        positions = payload.get("positions", [])
        with self._lock:
            state = LedgerState(
                Decimal(str(account["starting_equity"])),
                self._state.strategy_version,
                self._state.config_hash,
            )
            state.equity_usd = Decimal(str(account["equity"]))
            state.peak_equity_usd = Decimal(str(account["peak_equity"]))
            state.realized_pnl_usd = Decimal(str(account["realized_pnl"]))
            state.unrealized_pnl_usd = Decimal(str(account["unrealized_pnl"]))
            state.locked_capital_usd = Decimal(str(account["locked_capital"]))
            state.halt_reason = account.get("halt_reason")
            state.pause_until = account.get("pause_until")
            state.daily_halt_date = account.get("daily_halt_date")
            for item in positions:
                position = PositionRecord(
                    position_id=item["id"],
                    token_mint=item["mint"],
                    open_fill_id=item["open_fill_id"],
                    entry_token_amount=Decimal(str(item["initial_token_amount_raw"])),
                    entry_cost_usd=Decimal(str(item["initial_cost_usd"])),
                    open_ratio=(
                        Decimal(str(item["initial_cost_usd"]))
                        / Decimal(str(item["initial_token_amount_raw"]))
                    ),
                    opened_at=item["entry_time"],
                    closed_at=item["closed_at"],
                    locked_usd=Decimal(str(item["remaining_cost_usd"])),
                    status=(
                        PositionStatus.OPEN
                        if item["status"] in {"OPEN", "PARTIAL"}
                        else PositionStatus.CLOSED
                    ),
                    remaining_token_amount=Decimal(str(item["token_amount_raw"])),
                    remaining_cost_usd=Decimal(str(item["remaining_cost_usd"])),
                    realized_pnl_usd=Decimal(str(item["realized_pnl"])),
                    mfe_pct=Decimal(str(item["mfe_pct"])),
                    mae_pct=Decimal(str(item["mae_pct"])),
                    highest_executable_value_usd=Decimal(str(item["highest_executable_value"])),
                    lowest_executable_value_usd=Decimal(str(item["lowest_executable_value"])),
                    last_executable_value_usd=(
                        Decimal(str(item["remaining_cost_usd"]))
                        + Decimal(str(item.get("unrealized_pnl", "0")))
                    ),
                    tp1_taken=bool(item["tp1_taken"]),
                    tp2_taken=bool(item["tp2_taken"]),
                    last_new_high_at=item["last_new_high_at"],
                    final_exit_reason=item["exit_reason"],
                    candidate_id=item.get("candidate_id"),
                    pool_address=item.get("pool_address"),
                    entry_score=(
                        Decimal(str(item["entry_score"]))
                        if item.get("entry_score") is not None
                        else None
                    ),
                    entry_liquidity_usd=(
                        Decimal(str(item["entry_liquidity_usd"]))
                        if item.get("entry_liquidity_usd") is not None
                        else None
                    ),
                    entry_pool_age_seconds=(
                        Decimal(str(item["entry_pool_age_seconds"]))
                        if item.get("entry_pool_age_seconds") is not None
                        else None
                    ),
                    strategy_version=item.get("strategy_version"),
                )
                state.positions[position.position_id] = position
                if position.status == PositionStatus.OPEN:
                    state._token_index[position.token_mint] = position.position_id
            mint_by_position = {item["id"]: item["mint"] for item in positions}
            for item in fills:
                order = orders[item["order_id"]]
                is_entry = item["side"] == "BUY"
                token_mint = mint_by_position.get(order["position_id"], "unknown")
                fill = FillRecord(
                    fill_id=item["id"], token_mint=token_mint,
                    position_id=order["position_id"],
                    order_id=item["order_id"], quote_id=order["quote_request_id"] or "unrecoverable",
                    fill_type=FillType.ENTRY if is_entry else FillType.EXIT,
                    token_amount=Decimal(str(
                        item["output_raw_amount"] if is_entry else item["input_raw_amount"]
                    )),
                    usd_notional=Decimal(str(item["input_usd"] if is_entry else item["output_usd"])),
                    cost_basis_usd=Decimal(str(item["cost_basis_usd"])),
                    created_at=item["filled_at"], exit_reason=item["exit_reason"],
                    price_impact_pct=Decimal(str(item["price_impact_pct"])),
                    platform_fee_usd=Decimal(str(item["platform_fee_usd"])),
                    network_fee_usd=Decimal(str(item["network_fee_usd"])),
                    other_cost_usd=Decimal(str(item["other_cost_usd"])),
                    adverse_fill_bps=int(item["adverse_fill_bps"]),
                    quote_latency_ms=int(item["quote_latency_ms"]),
                )
                state.fills.append(fill)
                if is_entry:
                    state._order_id_index[item["order_id"]] = order["position_id"]
                else:
                    state._close_order_id_index[item["order_id"]] = order["position_id"]
                    state._close_fill_id_index[item["order_id"]] = item["id"]
                    day_key = self.current_date_key(item["filled_at"])
                    state.daily_pnl[day_key] += Decimal(
                        str(item["realized_pnl_usd"])
                    )
            self._state = state
            self._persist()
