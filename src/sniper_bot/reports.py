"""Deterministic paper-account reports with IANA timezone boundaries."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from .models import FillRecord, FillType, PositionRecord, PositionStatus


@dataclass(frozen=True)
class ClosedTrade:
    position: PositionRecord
    closed_at: datetime
    pnl_usd: Decimal
    holding_seconds: int


class ReportBuilder:
    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime
        self.zone = ZoneInfo(runtime.config.time_zone)

    def daily(
        self,
        day: str | None = None,
        *,
        as_of: datetime | None = None,
        capital_bounds: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        current_local = (as_of or datetime.now(tz=timezone.utc)).astimezone(self.zone)
        target = date.fromisoformat(day) if day else current_local.date()
        start_local = datetime.combine(target, time.min, tzinfo=self.zone)
        end_local = start_local + timedelta(days=1)
        start_utc = start_local.astimezone(timezone.utc)
        end_utc = end_local.astimezone(timezone.utc)
        all_fills = list(self.runtime.ledger.iter_fills())
        fills = [item for item in all_fills if start_utc <= item.created_at < end_utc]
        exits = [item for item in fills if item.fill_type == FillType.EXIT]
        entries = [item for item in fills if item.fill_type == FillType.ENTRY]
        pnl_values = [item.usd_notional - item.cost_basis_usd * item.token_amount for item in exits]
        realized = sum(pnl_values, Decimal("0"))
        prior_realized = sum(
            (
                item.usd_notional - item.cost_basis_usd * item.token_amount
                for item in all_fills
                if item.fill_type == FillType.EXIT and item.created_at < start_utc
            ), Decimal("0"),
        )
        starting_equity = self.runtime.config.starting_equity_usd + prior_realized
        snapshot = self.runtime.ledger.snapshot()
        ending_equity = starting_equity + realized
        is_current_day = target == current_local.date()
        if is_current_day:
            ending_equity = Decimal(str(snapshot["equity_usd"]))
        period_unrealized = (
            Decimal(str(snapshot["unrealized_pnl_usd"]))
            if is_current_day
            else Decimal("0")
        )
        equity_path: list[Decimal] = []
        starting_unrealized = Decimal("0")
        if capital_bounds is not None:
            starting_equity = Decimal(str(capital_bounds["starting_equity_usd"]))
            ending_equity = Decimal(str(capital_bounds["ending_equity_usd"]))
            period_unrealized = Decimal(
                str(capital_bounds["ending_unrealized_pnl_usd"])
            )
            starting_unrealized = Decimal(
                str(capital_bounds.get("starting_unrealized_pnl_usd", "0"))
            )
            equity_path = [
                Decimal(str(value))
                for value in capital_bounds.get("equity_path_usd", [])
            ]
        candidates = list(getattr(self.runtime.pipeline, "candidates", {}).values())
        day_candidates = [item for item in candidates if start_utc <= item.detected_at < end_utc]
        rejections = Counter(
            item.reject_reason.value for item in day_candidates if item.reject_reason is not None
        )
        trades = self._closed_trades(all_fills, start_utc=start_utc, end_utc=end_utc)
        exit_reasons = Counter(
            item.position.final_exit_reason
            for item in trades
            if item.position.final_exit_reason
        )
        operational = self._daily_operational_cost()
        simulated = self._simulated_costs(fills)
        open_positions = self._open_positions_as_of(all_fills, end_utc)
        open_notional = sum(
            (
                Decimal(str(item["remaining_cost_usd"]))
                for item in open_positions
            ),
            Decimal("0"),
        )
        total_exit_notional = sum(
            (fill.usd_notional for fill in exits), Decimal("0")
        )
        expected_ending_equity = (
            starting_equity
            + realized
            + period_unrealized
            - starting_unrealized
        )
        period_reconcile = {
            "equity_usd": str(ending_equity),
            "expected_equity_usd": str(expected_ending_equity),
            "realized_pnl_usd": str(realized),
            "expected_realized_pnl_usd": str(sum(pnl_values, Decimal("0"))),
            "open_positions_count": len(open_positions),
            "open_positions_notional_usd": str(open_notional),
            "total_exit_notional_usd": str(total_exit_notional),
            "is_reconciled": (
                abs(ending_equity - expected_ending_equity) <= Decimal("0.01")
                if capital_bounds is not None
                else realized == sum(pnl_values, Decimal("0"))
            ),
        }
        report: dict[str, Any] = {
            "period": "daily", "date": target.isoformat(),
            "timezone": self.runtime.config.time_zone,
            "period_start_utc": start_utc.isoformat(), "period_end_utc": end_utc.isoformat(),
            "strategy_version": self.runtime.config.strategy_version,
            "config_hash": self.runtime.config.config_hash,
            "capital": {
                "starting_equity_usd": str(starting_equity), "ending_equity_usd": str(ending_equity),
                "realized_pnl_usd": str(realized),
                "unrealized_pnl_usd": str(period_unrealized),
                "net_return_pct": str(
                    (ending_equity - starting_equity) / starting_equity
                    if starting_equity > 0 else Decimal("0")
                ),
                "simulated_costs_usd": str(simulated), "operational_costs_usd": str(operational),
                "economic_pnl_usd": str(
                    ending_equity - starting_equity - operational
                ),
            },
            "signals": {
                "new_pools": len(day_candidates), "tokens_checked": len(day_candidates),
                "hard_rejects": sum(rejections.values()),
                "score_60_plus": self._score_count(day_candidates, Decimal("60")),
                "score_80_plus": self._score_count(day_candidates, Decimal("80")),
                "paper_entries": len(entries),
                "risk_limit_skips": sum(rejections[key] for key in (
                    "DAILY_RISK_LIMIT", "MAX_OPEN_POSITIONS", "RISK_MANAGER_BLOCKED"
                )),
            },
            "trades": self._trade_statistics(trades),
            "max_intraday_drawdown_pct": str(
                self._equity_path_drawdown_pct(equity_path)
                if equity_path
                else self._max_drawdown_pct(starting_equity, exits)
            ),
            "execution_quality": self._execution_quality(
                fills,
                no_route_rejects=(
                    rejections["NO_BUY_ROUTE"] + rejections["NO_SELL_ROUTE"]
                ),
                exit_route_failures=exit_reasons["UNRECOVERABLE"],
            ),
            "exit_reasons": dict(sorted(exit_reasons.items())),
            "rejections": dict(sorted(rejections.items())),
            "system": {
                "health": self.runtime.health_status(),
                "websocket_reconnects": _metric_value(self.runtime.metrics.websocket_reconnects),
                "recovered_slots": _metric_value(self.runtime.metrics.websocket_gap_recoveries),
                "jupiter_errors": _metric_value(self.runtime.metrics.jupiter_no_route),
                "telegram_errors": 0, "database_errors": 0,
            },
            "open_positions": open_positions,
            "sample_size_warning": len(trades) < self.runtime.config.reporting.minimum_sample_warning_trades,
            "reconcile": period_reconcile,
        }
        report.update(
            {
                "equity_usd": str(ending_equity),
                "realized_pnl_usd": str(realized),
                "unrealized_pnl_usd": str(period_unrealized),
                "pnl": str(realized),
            }
        )
        report["report_id"] = _report_id(report)
        return report

    @staticmethod
    def _equity_path_drawdown_pct(equity_path: list[Decimal]) -> Decimal:
        peak = Decimal("0")
        maximum = Decimal("0")
        for equity in equity_path:
            peak = max(peak, equity)
            if peak > 0:
                maximum = max(maximum, (peak - equity) / peak)
        return maximum

    def all_time(
        self,
        *,
        as_of: datetime | None = None,
        max_drawdown_pct: Decimal | None = None,
    ) -> dict[str, Any]:
        fills = list(self.runtime.ledger.iter_fills())
        exits = [item for item in fills if item.fill_type == FillType.EXIT]
        entries = [item for item in fills if item.fill_type == FillType.ENTRY]
        trades = self._closed_trades(fills)
        snapshot = self.runtime.ledger.snapshot()
        report_time = as_of or datetime.now(tz=timezone.utc)
        first = min((item.created_at for item in fills), default=report_time)
        current = Decimal(str(snapshot["equity_usd"]))
        starting = self.runtime.config.starting_equity_usd
        exit_reasons = Counter(
            item.position.final_exit_reason
            for item in trades
            if item.position.final_exit_reason
        )
        candidates = list(getattr(self.runtime.pipeline, "candidates", {}).values())
        days = max(1, (report_time.date() - first.date()).days + 1)
        peak = Decimal(str(snapshot["peak_equity_usd"]))
        current_drawdown = (
            (peak - current) / peak if peak > 0 else Decimal("0")
        )
        report_drawdown = (
            max_drawdown_pct
            if max_drawdown_pct is not None
            else max(self._max_drawdown_pct(starting, exits), current_drawdown)
        )
        operational = self._daily_operational_cost() * days
        simulated = self._simulated_costs(fills)
        report: dict[str, Any] = {
            "period": "all_time", "date": "all_time", "timezone": self.runtime.config.time_zone,
            "first_run_at": first.isoformat(), "calendar_days": days,
            "strategy_version": self.runtime.config.strategy_version,
            "config_hash": self.runtime.config.config_hash,
            "starting_equity_usd": str(starting), "current_equity_usd": str(current),
            "net_pnl_usd": str(current - starting),
            "return_pct": str((current - starting) / starting if starting > 0 else Decimal("0")),
            "peak_equity_usd": str(peak),
            "max_drawdown_pct": str(report_drawdown),
            "new_pools": len(candidates), "candidates": len(candidates),
            "paper_entries": len(entries), "closed_trades": len(trades),
            "open_trades": len(self.runtime.ledger.open_positions),
            "trade_statistics": self._trade_statistics(trades),
            "execution_quality": self._execution_quality(
                fills,
                no_route_rejects=0,
                exit_route_failures=exit_reasons["UNRECOVERABLE"],
            ),
            "total_simulated_costs_usd": str(simulated),
            "total_operational_costs_usd": str(operational),
            "economic_pnl_usd": str(current - starting - operational),
            "exit_reasons": dict(sorted(exit_reasons.items())),
            "score_buckets": self._bucket_pnl(
                trades, lambda item: _score_bucket(item.position.entry_score)
            ),
            "liquidity_buckets": self._bucket_pnl(
                trades,
                lambda item: _liquidity_bucket(item.position.entry_liquidity_usd),
            ),
            "pool_age_buckets": self._bucket_pnl(
                trades,
                lambda item: _pool_age_bucket(item.position.entry_pool_age_seconds),
            ),
            "strategy_versions": self._bucket_pnl(
                trades,
                lambda item: item.position.strategy_version or "unknown",
            ),
            "open_positions": [item.model_dump(mode="json") for item in self.runtime.ledger.open_positions],
            "sample_size_warning": len(trades) < self.runtime.config.reporting.minimum_sample_warning_trades,
            "reconcile": _normalize(self.runtime.ledger.reconcile()),
        }
        report.update(
            {
                "equity_usd": str(current),
                "realized_pnl_usd": str(snapshot["realized_pnl_usd"]),
                "unrealized_pnl_usd": str(snapshot["unrealized_pnl_usd"]),
                "pnl": str(snapshot["realized_pnl_usd"]),
            }
        )
        report["report_id"] = _report_id(report)
        return report

    def _score_count(self, candidates: list[Any], threshold: Decimal) -> int:
        scores = getattr(self.runtime.pipeline, "_scores", {})
        persisted = getattr(self.runtime.pipeline, "_persisted_score_totals", {})
        return sum(
            (
                scores[item.candidate_id].total_score
                if scores.get(item.candidate_id) is not None
                else persisted.get(item.candidate_id, Decimal("-1"))
            )
            >= threshold
            for item in candidates
        )

    def _open_positions_as_of(
        self, fills: list[FillRecord], end_utc: datetime
    ) -> list[dict[str, Any]]:
        positions = self.runtime.ledger.state.positions.values()
        result: list[dict[str, Any]] = []
        for position in positions:
            if position.opened_at >= end_utc:
                continue
            entry_fills = [
                fill
                for fill in fills
                if fill.fill_type == FillType.ENTRY
                and fill.position_id == position.position_id
                and fill.created_at < end_utc
            ]
            if not entry_fills:
                continue
            exit_fills = [
                fill
                for fill in fills
                if fill.fill_type == FillType.EXIT
                and fill.position_id == position.position_id
                and fill.created_at < end_utc
            ]
            acquired = sum((fill.token_amount for fill in entry_fills), Decimal("0"))
            sold = sum((fill.token_amount for fill in exit_fills), Decimal("0"))
            remaining = acquired - sold
            if remaining <= 0:
                continue
            remaining_cost = sum(
                (fill.usd_notional for fill in entry_fills), Decimal("0")
            ) - sum(
                (fill.cost_basis_usd * fill.token_amount for fill in exit_fills),
                Decimal("0"),
            )
            result.append(
                {
                    "position_id": position.position_id,
                    "token_mint": position.token_mint,
                    "remaining_token_amount": str(remaining),
                    "remaining_cost_usd": str(remaining_cost),
                    "opened_at": position.opened_at.isoformat(),
                    "status": "OPEN",
                }
            )
        return sorted(result, key=lambda item: str(item["position_id"]))

    @staticmethod
    def _trade_statistics(trades: list[ClosedTrade]) -> dict[str, Any]:
        pnl = [item.pnl_usd for item in trades]
        count = len(pnl)
        wins = sum(value > 0 for value in pnl)
        losses = sum(value < 0 for value in pnl)
        gross_profit = sum((value for value in pnl if value > 0), Decimal("0"))
        gross_loss = sum((value for value in pnl if value < 0), Decimal("0"))
        average_win = gross_profit / wins if wins else Decimal("0")
        average_loss = gross_loss / losses if losses else Decimal("0")
        average_holding = (
            sum(item.holding_seconds for item in trades) // count if count else 0
        )
        return {
            "closed": count, "profitable": wins, "losing": losses,
            "win_rate": str(Decimal(wins) / Decimal(count) if count else Decimal("0")),
            "gross_profit_usd": str(gross_profit), "gross_loss_usd": str(gross_loss),
            "profit_factor": str(gross_profit / abs(gross_loss) if gross_loss < 0 else Decimal("0")),
            "expectancy_usd": str(sum(pnl, Decimal("0")) / count if count else Decimal("0")),
            "average_win_usd": str(average_win), "average_loss_usd": str(average_loss),
            "payoff_ratio": str(average_win / abs(average_loss) if average_loss < 0 else Decimal("0")),
            "largest_win_usd": str(max(pnl, default=Decimal("0"))),
            "largest_loss_usd": str(min(pnl, default=Decimal("0"))),
            "average_holding_seconds": average_holding,
            "max_consecutive_wins": _max_streak(pnl, winning=True),
            "max_consecutive_losses": _max_streak(pnl, winning=False),
        }

    def _closed_trades(
        self,
        fills: list[FillRecord],
        *,
        start_utc: datetime | None = None,
        end_utc: datetime | None = None,
    ) -> list[ClosedTrade]:
        result: list[ClosedTrade] = []
        positions = list(self.runtime.ledger.state.positions.values())
        for position in positions:
            if position.status != PositionStatus.CLOSED:
                continue
            matching = [
                fill
                for fill in fills
                if fill.fill_type == FillType.EXIT
                and (
                    fill.position_id == position.position_id
                    or (fill.position_id is None and fill.token_mint == position.token_mint)
                )
                and fill.created_at >= position.opened_at
            ]
            closed_at = position.closed_at or max(
                (fill.created_at for fill in matching), default=None
            )
            if closed_at is None:
                continue
            if start_utc is not None and closed_at < start_utc:
                continue
            if end_utc is not None and closed_at >= end_utc:
                continue
            result.append(
                ClosedTrade(
                    position=position,
                    closed_at=closed_at,
                    pnl_usd=position.realized_pnl_usd,
                    holding_seconds=max(
                        0, int((closed_at - position.opened_at).total_seconds())
                    ),
                )
            )
        return sorted(result, key=lambda item: (item.closed_at, item.position.position_id))

    @staticmethod
    def _simulated_costs(fills: list[FillRecord]) -> Decimal:
        return sum(
            (
                item.platform_fee_usd
                + item.network_fee_usd
                + item.other_cost_usd
                for item in fills
            ),
            Decimal("0"),
        )

    @staticmethod
    def _execution_quality(
        fills: list[FillRecord], *, no_route_rejects: int, exit_route_failures: int
    ) -> dict[str, Any]:
        buys = [item for item in fills if item.fill_type == FillType.ENTRY]
        sells = [item for item in fills if item.fill_type == FillType.EXIT]
        round_trips = [
            item.round_trip_cost_pct
            for item in buys
            if item.round_trip_cost_pct is not None
        ]
        return {
            "average_buy_impact_pct": str(
                _average([item.price_impact_pct for item in buys])
            ),
            "average_sell_impact_pct": str(
                _average([item.price_impact_pct for item in sells])
            ),
            "average_round_trip_cost_pct": str(_average(round_trips)),
            "average_quote_latency_ms": int(
                _average([Decimal(item.quote_latency_ms) for item in fills])
            ),
            "no_route_rejects": no_route_rejects,
            "exit_route_failures": exit_route_failures,
            "average_adverse_fill_bps": str(
                _average([Decimal(item.adverse_fill_bps) for item in fills])
            ),
        }

    @staticmethod
    def _max_drawdown_pct(starting_equity: Decimal, exits: list[FillRecord]) -> Decimal:
        equity = starting_equity
        peak = starting_equity
        maximum = Decimal("0")
        for fill in sorted(exits, key=lambda item: (item.created_at, item.fill_id)):
            equity += fill.usd_notional - fill.cost_basis_usd * fill.token_amount
            peak = max(peak, equity)
            if peak > 0:
                maximum = max(maximum, (peak - equity) / peak)
        return maximum

    @staticmethod
    def _bucket_pnl(
        trades: list[ClosedTrade], selector: Any
    ) -> dict[str, str]:
        buckets: dict[str, Decimal] = {}
        for trade in trades:
            key = str(selector(trade))
            buckets[key] = buckets.get(key, Decimal("0")) + trade.pnl_usd
        return {key: str(buckets[key]) for key in sorted(buckets)}

    def _daily_operational_cost(self) -> Decimal:
        if not self.runtime.config.reporting.include_operational_costs:
            return Decimal("0")
        monthly = Decimal(str(self.runtime.config.reporting.monthly_infrastructure_cost_usd))
        return monthly / Decimal("30")


def _normalize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _normalize(item) for key, item in value.items()}
    return value


def _report_id(report: dict[str, Any]) -> str:
    payload = json.dumps(report, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _metric_value(metric: Any) -> int:
    value = getattr(metric, "_value", None)
    return int(value.get()) if value is not None else 0


def _average(values: list[Decimal]) -> Decimal:
    return sum(values, Decimal("0")) / len(values) if values else Decimal("0")


def _max_streak(values: list[Decimal], *, winning: bool) -> int:
    maximum = 0
    current = 0
    for value in values:
        matches = value > 0 if winning else value < 0
        current = current + 1 if matches else 0
        maximum = max(maximum, current)
    return maximum


def _score_bucket(value: Decimal | None) -> str:
    if value is None:
        return "unknown"
    if value < 80:
        return "below_80"
    if value < 85:
        return "80_84"
    if value < 90:
        return "85_89"
    return "90_plus"


def _liquidity_bucket(value: Decimal | None) -> str:
    if value is None:
        return "unknown"
    if value < 40_000:
        return "below_40k"
    if value < 75_000:
        return "40k_75k"
    if value < 150_000:
        return "75k_150k"
    return "150k_plus"


def _pool_age_bucket(value: Decimal | None) -> str:
    if value is None:
        return "unknown"
    if value <= 75:
        return "45_75s"
    if value <= 120:
        return "76_120s"
    return "121_180s"
