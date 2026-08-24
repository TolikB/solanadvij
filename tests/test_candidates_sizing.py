from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from sniper_bot.candidates import Candidate, CandidateState, CandidateStateMachine
from sniper_bot.features import FeatureSnapshot
from sniper_bot.scoring import ScoreBreakdown
from sniper_bot.security import SecurityResult
from sniper_bot.sizing import PositionSizingInput, calculate_position_size


def _score(value: str = "90") -> ScoreBreakdown:
    total = Decimal(value)
    return ScoreBreakdown(
        total_score=total,
        organic_score=Decimal("25"),
        distribution_score=Decimal("18"),
        execution_score=Decimal("18"),
        liquidity_score=Decimal("14"),
        developer_score=Decimal("7"),
        price_structure_score=total - Decimal("82"),
        explanations={},
    )


def _snapshot(at: datetime, *, price: str = "0.9", vwap: str = "1") -> FeatureSnapshot:
    return FeatureSnapshot(
        pool_address="POOL",
        snapshot_time=at,
        pool_age_seconds=Decimal(str((at - datetime(2026, 8, 24, tzinfo=timezone.utc)).total_seconds())),
        current_price_usd=Decimal(price),
        rolling_vwap_30s=Decimal(vwap),
        local_high=Decimal("1.1"),
        drawdown_from_local_high=Decimal("0.18"),
        quote_liquidity_change_30s=Decimal("0"),
        buyer_acceleration=Decimal("1.5"),
    )


def test_state_machine_requires_two_consecutive_score_windows_and_emits_once() -> None:
    start = datetime(2026, 8, 24, tzinfo=timezone.utc)
    machine = CandidateStateMachine()
    candidate = Candidate(
        candidate_id="candidate",
        mint="TOKEN",
        pool_address="POOL",
        detected_at=start,
        updated_at=start,
        strategy_version="v1",
        config_hash="hash",
    )
    candidate = machine.evaluate(candidate, _snapshot(start))
    candidate = machine.evaluate(candidate, _snapshot(start + timedelta(seconds=45)))
    candidate = machine.evaluate(
        candidate,
        _snapshot(start + timedelta(seconds=46)),
        security=SecurityResult(checked_at=start, hard_reject=False, reject_reasons=[]),
    )
    candidate = machine.evaluate(candidate, _snapshot(start + timedelta(seconds=47)))
    assert candidate.state == CandidateState.WAITING_PULLBACK
    candidate = machine.evaluate(
        candidate,
        _snapshot(start + timedelta(seconds=50)),
        score=_score(),
        sell_route_available=True,
    )
    assert candidate.state == CandidateState.WAITING_PULLBACK
    candidate = machine.evaluate(
        candidate,
        _snapshot(start + timedelta(seconds=55)),
        score=_score(),
        sell_route_available=True,
    )
    assert candidate.state == CandidateState.ARMED
    candidate = machine.evaluate(
        candidate,
        _snapshot(start + timedelta(seconds=56), price="1.05", vwap="1"),
    )
    assert candidate.state == CandidateState.ENTRY_PENDING
    assert candidate.entry_signal_emitted is True
    assert machine.evaluate(candidate, _snapshot(start + timedelta(seconds=57))).state == CandidateState.ENTRY_PENDING


def test_invalid_state_transition_is_rejected() -> None:
    start = datetime(2026, 8, 24, tzinfo=timezone.utc)
    candidate = Candidate(
        candidate_id="candidate",
        mint="TOKEN",
        pool_address="POOL",
        detected_at=start,
        updated_at=start,
        strategy_version="v1",
        config_hash="hash",
    )
    with pytest.raises(ValueError, match="invalid candidate transition"):
        CandidateStateMachine().transition(candidate, CandidateState.POSITION_OPEN, start)


def test_position_sizing_matches_spec_example() -> None:
    result = calculate_position_size(
        PositionSizingInput(
            current_equity_usd=Decimal("500"),
            daily_pnl_usd=Decimal("0"),
            quote_liquidity_usd=Decimal("50000"),
            estimated_round_trip_cost_pct=Decimal("0.05"),
            score=Decimal("92"),
        )
    )
    assert result.allowed is True
    assert result.position_size_usd == Decimal("11.90")


@given(
    equity=st.integers(min_value=1, max_value=100_000),
    liquidity=st.integers(min_value=1, max_value=10_000_000),
    costs=st.integers(min_value=0, max_value=100),
    score=st.integers(min_value=0, max_value=100),
)
def test_position_size_is_never_negative(equity: int, liquidity: int, costs: int, score: int) -> None:
    result = calculate_position_size(
        PositionSizingInput(
            current_equity_usd=Decimal(equity),
            daily_pnl_usd=Decimal("0"),
            quote_liquidity_usd=Decimal(liquidity),
            estimated_round_trip_cost_pct=Decimal(costs) / Decimal("100"),
            score=Decimal(score),
        )
    )
    assert result.position_size_usd >= 0
    assert result.position_size_usd <= Decimal("20")
