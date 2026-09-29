from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from sniper_bot.candidates import Candidate, CandidateState, CandidateStateMachine
from sniper_bot.config import AppConfig
from sniper_bot.events import ChainEventType, EventEnvelope, EventSource, Protocol
from sniper_bot.features import FeatureSnapshot
from sniper_bot.metrics import BotMetrics
from sniper_bot.models import QuoteResponse, RoundTripQuote
from sniper_bot.pipeline import ConfirmationPipeline
from sniper_bot.rate_limit import PriorityRateLimiter, QuotePriority
from sniper_bot.registry import WSOL_MINT, QuoteAssetPrice
from sniper_bot.runtime import SniperRuntime
from sniper_bot.scoring import ScoreBreakdown
from sniper_bot.security import (
    ExecutionChecks,
    HolderBalance,
    HolderMetrics,
    MintInfo,
    RejectReason,
    SecurityContext,
    SecurityEngine,
)
from sniper_bot.stream import EntryGate

NOW = datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_limiter_serves_urgent_requests_before_queued_ones() -> None:
    limiter = PriorityRateLimiter(0.05)
    order: list[str] = []

    async def request(name: str, priority: QuotePriority) -> None:
        await limiter.acquire(priority)
        order.append(name)

    await limiter.acquire(QuotePriority.SECURITY)  # occupies the current slot
    waiting = [
        asyncio.create_task(request(f"security-{index}", QuotePriority.SECURITY))
        for index in range(3)
    ]
    await asyncio.sleep(0)
    urgent = asyncio.create_task(request("exit", QuotePriority.EXIT))
    await asyncio.gather(*waiting, urgent)

    assert order[0] == "exit"
    assert order[1:] == ["security-0", "security-1", "security-2"]


@pytest.mark.asyncio
async def test_limiter_keeps_the_rate_and_skips_cancelled_waiters() -> None:
    limiter = PriorityRateLimiter(0.05)
    await limiter.acquire()
    cancelled = asyncio.create_task(limiter.acquire(QuotePriority.EXIT))
    await asyncio.sleep(0)
    cancelled.cancel()
    started = time.monotonic()
    await limiter.acquire(QuotePriority.SECURITY)
    await limiter.acquire(QuotePriority.SECURITY)
    elapsed = time.monotonic() - started
    assert 0.08 <= elapsed < 0.5
    assert limiter.queued == 0


def _snapshot(at: datetime, **values: Any) -> FeatureSnapshot:
    return FeatureSnapshot(
        pool_address="POOL",
        snapshot_time=at,
        pool_age_seconds=Decimal("60"),
        drawdown_from_local_high=Decimal("0.01"),
        **values,
    )


def _score(total: str) -> ScoreBreakdown:
    zero = Decimal("0")
    return ScoreBreakdown(
        total_score=Decimal(total),
        organic_score=zero,
        distribution_score=zero,
        execution_score=zero,
        liquidity_score=zero,
        developer_score=zero,
        price_structure_score=zero,
        explanations={},
    )


def _waiting(at: datetime) -> Candidate:
    return Candidate(
        candidate_id="c",
        mint="MINT",
        pool_address="POOL",
        state=CandidateState.WAITING_PULLBACK,
        detected_at=at - timedelta(seconds=60),
        updated_at=at,
        strategy_version="s",
        config_hash="h",
    )


def test_score_confirmation_survives_a_slow_evaluation_loop() -> None:
    legacy = CandidateStateMachine(expiry_seconds=600)
    tolerant = CandidateStateMachine(expiry_seconds=600, score_confirmation_max_gap_seconds=15)
    results = {}
    for name, machine in (("legacy", legacy), ("tolerant", tolerant)):
        candidate = _waiting(NOW)
        for step in range(3):
            at = NOW + timedelta(seconds=7 * step)
            candidate = machine.evaluate(candidate, _snapshot(at), score=_score("85"))
        results[name] = len(candidate.score_confirmations)
    assert results == {"legacy": 1, "tolerant": 2}


def test_score_confirmation_keeps_the_one_second_behaviour() -> None:
    machine = CandidateStateMachine(expiry_seconds=600, score_confirmation_max_gap_seconds=15)
    candidate = _waiting(NOW)
    confirmations = []
    for second in range(6):
        candidate = machine.evaluate(
            candidate, _snapshot(NOW + timedelta(seconds=second)), score=_score("85")
        )
        confirmations.append(len(candidate.score_confirmations))
    assert confirmations == [1, 1, 1, 1, 2, 2]

    candidate = machine.evaluate(
        candidate, _snapshot(NOW + timedelta(seconds=6)), score=_score("70")
    )
    assert candidate.score_confirmations == []
    far = machine.evaluate(
        _waiting(NOW).model_copy(update={"score_confirmations": [NOW]}),
        _snapshot(NOW + timedelta(seconds=20)),
        score=_score("85"),
    )
    assert far.score_confirmations == [NOW + timedelta(seconds=20)]


def _context(quote_at: datetime, now: datetime) -> SecurityContext:
    return SecurityContext(
        mint=MintInfo(
            mint="MINT",
            token_program="TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            decimals=6,
            total_supply_raw=Decimal("1000000"),
            observed_at=now,
        ),
        holders=HolderMetrics(),
        holders_observed_at=now,
        execution=ExecutionChecks(
            buy_route_available=True,
            sell_route_available=True,
            round_trip_loss_pct=Decimal("0.01"),
            buy_price_impact_pct=Decimal("0.001"),
            sell_price_impact_pct=Decimal("0.001"),
            quote_observed_at=quote_at,
        ),
        quote_mint=WSOL_MINT,
        quote_liquidity_usd=Decimal("100000"),
        liquidity_change_30s=Decimal("0"),
        pool_age_seconds=Decimal("60"),
        external_successful_sellers=10,
        stream_observed_at=now,
    )


def test_watched_candidates_use_the_periodic_quote_but_entries_need_a_fresh_one() -> None:
    engine = SecurityEngine(
        maximum_quote_age_seconds=Decimal("1.5"),
        maximum_monitoring_quote_age_seconds=Decimal("9.5"),
    )
    context = _context(NOW - timedelta(seconds=6), NOW)

    watched = engine.evaluate(context, now=NOW, entry_decision=False)
    entry = engine.evaluate(context, now=NOW, entry_decision=True)

    assert RejectReason.STALE_QUOTE not in watched.reject_reasons
    assert RejectReason.STALE_QUOTE in entry.reject_reasons


def _config() -> AppConfig:
    return AppConfig(
        APP_MODE="paper",
        APP_REVISION="",
        HELIUS_API_KEY="helius-key",
        JUPITER_API_KEY="jupiter-key",
        POSTGRES_DSN="postgresql://user:pass@localhost:5432/db",
        TELEGRAM_BOT_TOKEN="telegram-token",
        TELEGRAM_ADMIN_CHAT_ID=123456,
        STARTING_EQUITY_USD=Decimal("500"),
    )


class _CountingRpc:
    def __init__(self) -> None:
        self.mint_calls = 0
        self.holder_calls = 0

    async def get_mint_info(self, mint: str) -> MintInfo:
        self.mint_calls += 1
        return MintInfo(
            mint=mint,
            token_program="TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            decimals=6,
            total_supply_raw=Decimal("1000000"),
            observed_at=datetime.now(tz=timezone.utc),
        )

    async def get_all_holders(
        self, mint: str, *, expected_supply_raw: Decimal
    ) -> list[HolderBalance]:
        self.holder_calls += 1
        return [HolderBalance(token_account="acc", owner="holder", amount_raw=Decimal("10"))]


class _CountingQuotes:
    def __init__(self) -> None:
        self.priorities: list[QuotePriority] = []

    async def get_round_trip_quote(
        self,
        *,
        quote_token: str,
        token: str,
        usdc_amount: Decimal,
        priority: QuotePriority = QuotePriority.SECURITY,
    ) -> RoundTripQuote:
        self.priorities.append(priority)
        received = datetime.now(tz=timezone.utc)
        quote = QuoteResponse(
            token_in=quote_token,
            token_out=token,
            in_amount=usdc_amount,
            out_amount=usdc_amount,
            received_at=received,
        )
        return RoundTripQuote(
            buy=quote,
            sell=quote,
            starting_usd=usdc_amount,
            ending_usd=usdc_amount,
            loss_pct=Decimal("0"),
        )


def _pool_event(pool: str, mint: str, at: datetime) -> EventEnvelope:
    return EventEnvelope(
        source=EventSource.HELIUS_WSS,
        protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.POOL_CREATED,
        slot=1,
        signature=f"create-{pool}",
        instruction_index=1,
        block_time=at,
        observed_at=at,
        mint=mint,
        pool_address=pool,
        payload={
            "base_mint": mint,
            "quote_mint": WSOL_MINT,
            "base_mint_decimals": 6,
            "quote_mint_decimals": 9,
            "pool_base_amount": 1_000_000_000_000,
            "pool_quote_amount": 300_000_000_000,
        },
    )


@pytest.mark.asyncio
async def test_security_inputs_refresh_on_their_own_cadence(tmp_path: Path) -> None:
    runtime = SniperRuntime(_config(), data_dir=tmp_path)
    runtime.database = None
    runtime.pipeline.database = None
    rpc, quotes = _CountingRpc(), _CountingQuotes()
    runtime.rpc = rpc  # type: ignore[assignment]
    runtime.quote_provider = quotes  # type: ignore[assignment]
    start = datetime.now(tz=timezone.utc)
    await runtime.pipeline.process_event(_pool_event("POOL", "MINT", start))
    candidate = next(iter(runtime.pipeline.candidates.values())).model_copy(
        update={"state": CandidateState.WAITING_PULLBACK}
    )

    for second in (0, 1, 2, 3, 4, 5, 6, 9, 10, 11):
        snapshot = runtime.pipeline.features.snapshot(
            "POOL", start + timedelta(seconds=second)
        )
        await runtime._build_security_context(candidate, snapshot)

    # Holders and mint every 10 s, round trips every 5 s.
    assert (rpc.mint_calls, rpc.holder_calls) == (2, 2)
    assert quotes.priorities == [QuotePriority.SECURITY] * 3

    entry = candidate.model_copy(update={"state": CandidateState.ENTRY_PENDING})
    await runtime._build_security_context(
        entry, runtime.pipeline.features.snapshot("POOL", start + timedelta(seconds=12))
    )
    assert quotes.priorities[-1] == QuotePriority.ENTRY
    assert rpc.holder_calls == 2


@pytest.mark.asyncio
async def test_candidate_security_reads_overlap(tmp_path: Path) -> None:
    metrics = BotMetrics()
    calls: list[str] = []

    async def slow_provider(candidate: Candidate, snapshot: FeatureSnapshot) -> SecurityContext:
        calls.append(candidate.pool_address)
        await asyncio.sleep(0.2)
        if candidate.pool_address == "POOL-1":
            raise RuntimeError("holder index lagging")
        now = datetime.now(tz=timezone.utc)
        return _context(now, now)

    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="strategy-v1",
        config_hash="config-hash",
        entry_gate=EntryGate(metrics),
        metrics=metrics,
        security_provider=slow_provider,
        record_raw=False,
        config=_config(),
    )
    start = datetime.now(tz=timezone.utc)
    pipeline.pools.set_quote_price(
        QuoteAssetPrice(mint=WSOL_MINT, price_usd=Decimal("150"), observed_at=start)
    )
    for index in range(4):
        await pipeline.process_event(_pool_event(f"POOL-{index}", f"MINT-{index}", start))
    for candidate_id, candidate in list(pipeline.candidates.items()):
        pipeline.candidates[candidate_id] = candidate.model_copy(
            update={"state": CandidateState.WAITING_PULLBACK}
        )

    started = time.monotonic()
    await pipeline.evaluate_candidates(start + timedelta(seconds=50))
    elapsed = time.monotonic() - started

    assert sorted(calls) == ["POOL-0", "POOL-1", "POOL-2", "POOL-3"]
    assert elapsed < 0.6
    assert (
        metrics.candidate_evaluation_failures.labels(stage="security")._value.get() == 1
    )
