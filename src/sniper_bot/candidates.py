"""Validated candidate lifecycle with single-transition event processing."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, Field

from .features import FeatureSnapshot
from .scoring import ScoreBreakdown
from .security import RejectReason, SecurityResult


class CandidateState(StrEnum):
    DISCOVERED = "DISCOVERED"
    COLLECTING = "COLLECTING"
    SECURITY_CHECK = "SECURITY_CHECK"
    ELIGIBLE = "ELIGIBLE"
    WAITING_PULLBACK = "WAITING_PULLBACK"
    ARMED = "ARMED"
    ENTRY_PENDING = "ENTRY_PENDING"
    POSITION_OPEN = "POSITION_OPEN"
    POSITION_PARTIAL = "POSITION_PARTIAL"
    EXIT_PENDING = "EXIT_PENDING"
    RETRYING_EXIT = "RETRYING_EXIT"
    CLOSED = "CLOSED"
    REJECTED = "REJECTED"


_ALLOWED = {
    CandidateState.DISCOVERED: {CandidateState.COLLECTING, CandidateState.REJECTED},
    CandidateState.COLLECTING: {CandidateState.SECURITY_CHECK, CandidateState.REJECTED},
    CandidateState.SECURITY_CHECK: {CandidateState.ELIGIBLE, CandidateState.REJECTED},
    CandidateState.ELIGIBLE: {CandidateState.WAITING_PULLBACK, CandidateState.REJECTED},
    CandidateState.WAITING_PULLBACK: {CandidateState.ARMED, CandidateState.REJECTED},
    CandidateState.ARMED: {CandidateState.ENTRY_PENDING, CandidateState.REJECTED},
    CandidateState.ENTRY_PENDING: {CandidateState.POSITION_OPEN, CandidateState.REJECTED},
    CandidateState.POSITION_OPEN: {CandidateState.POSITION_PARTIAL, CandidateState.EXIT_PENDING},
    CandidateState.POSITION_PARTIAL: {CandidateState.EXIT_PENDING},
    CandidateState.EXIT_PENDING: {
        CandidateState.POSITION_PARTIAL,
        CandidateState.RETRYING_EXIT,
        CandidateState.CLOSED,
    },
    CandidateState.RETRYING_EXIT: {CandidateState.EXIT_PENDING, CandidateState.CLOSED},
    CandidateState.CLOSED: set(),
    CandidateState.REJECTED: set(),
}


class Candidate(BaseModel):
    candidate_id: str
    mint: str
    pool_address: str
    state: CandidateState = CandidateState.DISCOVERED
    detected_at: datetime
    updated_at: datetime
    eligible_at: datetime | None = None
    armed_at: datetime | None = None
    expired_at: datetime | None = None
    rejected_at: datetime | None = None
    reject_reason: RejectReason | None = None
    strategy_version: str
    config_hash: str
    score_confirmations: list[datetime] = Field(default_factory=list)
    entry_signal_emitted: bool = False
    previous_price: Decimal | None = None
    previous_vwap: Decimal | None = None
    pullback_local_high: Decimal | None = None


class CandidateStateMachine:
    def __init__(
        self,
        *,
        collect_seconds: int = 45,
        expiry_seconds: int = 600,
        minimum_score: Decimal = Decimal("80"),
        required_confirmations: int = 2,
        score_window_seconds: int = 5,
        minimum_pullback: Decimal = Decimal("0.10"),
        maximum_pullback: Decimal = Decimal("0.25"),
        maximum_liquidity_drop: Decimal = Decimal("0.03"),
        minimum_buyer_acceleration: Decimal = Decimal("1.3"),
    ) -> None:
        self.collect_seconds = collect_seconds
        self.expiry_seconds = expiry_seconds
        self.minimum_score = minimum_score
        self.required_confirmations = required_confirmations
        self.score_window_seconds = score_window_seconds
        self.minimum_pullback = minimum_pullback
        self.maximum_pullback = maximum_pullback
        self.maximum_liquidity_drop = maximum_liquidity_drop
        self.minimum_buyer_acceleration = minimum_buyer_acceleration

    def transition(
        self,
        candidate: Candidate,
        target: CandidateState,
        at: datetime,
        *,
        reject_reason: RejectReason | None = None,
    ) -> Candidate:
        if target not in _ALLOWED[candidate.state]:
            raise ValueError(f"invalid candidate transition {candidate.state} -> {target}")
        if target == CandidateState.REJECTED and reject_reason is None:
            raise ValueError("rejected candidate requires a machine-readable reason")
        update: dict[str, object] = {"state": target, "updated_at": at}
        if target == CandidateState.ELIGIBLE:
            update["eligible_at"] = at
        if target == CandidateState.ARMED:
            update["armed_at"] = at
        if target == CandidateState.REJECTED:
            update["rejected_at"] = at
            update["reject_reason"] = reject_reason
        return candidate.model_copy(update=update)

    def evaluate(
        self,
        candidate: Candidate,
        snapshot: FeatureSnapshot,
        *,
        security: SecurityResult | None = None,
        score: ScoreBreakdown | None = None,
        sell_route_available: bool = False,
        dev_sold: bool = False,
    ) -> Candidate:
        at = snapshot.snapshot_time.astimezone(timezone.utc)
        age = at - candidate.detected_at.astimezone(timezone.utc)
        if candidate.state not in {CandidateState.POSITION_OPEN, CandidateState.POSITION_PARTIAL, CandidateState.EXIT_PENDING, CandidateState.RETRYING_EXIT, CandidateState.CLOSED} and age > timedelta(seconds=self.expiry_seconds):
            return self.transition(
                candidate,
                CandidateState.REJECTED,
                at,
                reject_reason=RejectReason.ENTRY_WINDOW_EXPIRED,
            )
        if (
            security is not None
            and security.hard_reject
            and candidate.state
            in {
                CandidateState.ELIGIBLE,
                CandidateState.WAITING_PULLBACK,
                CandidateState.ARMED,
                CandidateState.ENTRY_PENDING,
            }
        ):
            return self.transition(
                candidate,
                CandidateState.REJECTED,
                at,
                reject_reason=security.reject_reasons[0],
            )
        if candidate.state == CandidateState.DISCOVERED:
            return self.transition(candidate, CandidateState.COLLECTING, at)
        if candidate.state == CandidateState.COLLECTING:
            if age.total_seconds() >= self.collect_seconds:
                return self.transition(candidate, CandidateState.SECURITY_CHECK, at)
            return self._remember_prices(candidate, snapshot, at)
        if candidate.state == CandidateState.SECURITY_CHECK:
            if security is None:
                return candidate
            if security.hard_reject:
                return self.transition(
                    candidate,
                    CandidateState.REJECTED,
                    at,
                    reject_reason=security.reject_reasons[0],
                )
            return self.transition(candidate, CandidateState.ELIGIBLE, at)
        if candidate.state == CandidateState.ELIGIBLE:
            return self.transition(candidate, CandidateState.WAITING_PULLBACK, at)
        if candidate.state == CandidateState.WAITING_PULLBACK:
            candidate = self._record_score(candidate, score, at)
            if dev_sold:
                return self.transition(candidate, CandidateState.REJECTED, at, reject_reason=RejectReason.DEV_SOLD)
            pullback_ok = (
                self.minimum_pullback
                <= snapshot.drawdown_from_local_high
                <= self.maximum_pullback
            )
            confirmations_ok = (
                len(candidate.score_confirmations) >= self.required_confirmations
            )
            if (
                confirmations_ok
                and pullback_ok
                and snapshot.quote_liquidity_change_30s
                >= -self.maximum_liquidity_drop
                and snapshot.buyer_acceleration >= self.minimum_buyer_acceleration
                and sell_route_available
            ):
                candidate = candidate.model_copy(update={"pullback_local_high": snapshot.local_high})
                return self.transition(candidate, CandidateState.ARMED, at)
            return self._remember_prices(candidate, snapshot, at)
        if candidate.state == CandidateState.ARMED:
            previous_price = candidate.previous_price
            previous_vwap = candidate.previous_vwap
            crossed_vwap = (
                previous_price is not None
                and previous_vwap is not None
                and previous_price <= previous_vwap
                and snapshot.current_price_usd > snapshot.rolling_vwap_30s
            )
            broke_high = (
                candidate.pullback_local_high is not None
                and snapshot.current_price_usd > candidate.pullback_local_high
            )
            if (crossed_vwap or broke_high) and not candidate.entry_signal_emitted:
                candidate = candidate.model_copy(update={"entry_signal_emitted": True})
                return self.transition(candidate, CandidateState.ENTRY_PENDING, at)
            return self._remember_prices(candidate, snapshot, at)
        return candidate

    def _record_score(
        self,
        candidate: Candidate,
        score: ScoreBreakdown | None,
        at: datetime,
    ) -> Candidate:
        confirmations = list(candidate.score_confirmations)
        if score is None or score.total_score < self.minimum_score:
            confirmations = []
        elif not confirmations:
            confirmations = [at]
        else:
            delta = (at - confirmations[-1]).total_seconds()
            minimum_delta = max(0, self.score_window_seconds - 1)
            maximum_delta = self.score_window_seconds + 1
            if minimum_delta <= delta <= maximum_delta:
                confirmations = (confirmations + [at])[-self.required_confirmations :]
            elif delta > maximum_delta:
                confirmations = [at]
        return candidate.model_copy(update={"score_confirmations": confirmations, "updated_at": at})

    @staticmethod
    def _remember_prices(candidate: Candidate, snapshot: FeatureSnapshot, at: datetime) -> Candidate:
        return candidate.model_copy(
            update={
                "previous_price": snapshot.current_price_usd,
                "previous_vwap": snapshot.rolling_vwap_30s,
                "updated_at": at,
            }
        )
