from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from sniper_bot.candidates import CandidateState
from sniper_bot.events import ChainEventType, EventEnvelope, EventSource, Protocol
from sniper_bot.metrics import BotMetrics
from sniper_bot.pipeline import ConfirmationPipeline
from sniper_bot.stream import EntryGate


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source",
    [EventSource.BASELINE_WSS, EventSource.RPC_RECOVERY],
)
async def test_non_tradable_sources_materialize_state_without_candidate(
    tmp_path,
    source: EventSource,
) -> None:
    observed: list[str] = []

    async def observe(event: EventEnvelope) -> None:
        observed.append(event.event_id)

    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(BotMetrics()),
        metrics=BotMetrics(),
        event_observer=observe,
        record_raw=False,
    )
    now = datetime.now(tz=timezone.utc)
    event = EventEnvelope(
        source=source,
        protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.POOL_CREATED,
        slot=1,
        signature=f"{source.value}-signature",
        instruction_index=0,
        block_time=now,
        observed_at=now,
        mint="TOKEN",
        pool_address="POOL",
        payload={
            "base_mint": "TOKEN",
            "quote_mint": "So11111111111111111111111111111111111111112",
            "base_mint_decimals": 6,
            "quote_mint_decimals": 9,
            "pool_base_amount": 1_000_000,
            "pool_quote_amount": 1_000_000_000,
        },
    )

    accepted = await pipeline.process_event(event)

    assert accepted is True
    assert observed == [event.event_id]
    assert pipeline.candidates == {}
    assert pipeline.pools.pool("POOL") is not None


@pytest.mark.asyncio
async def test_untracked_swap_is_filtered_from_live_state(tmp_path) -> None:
    metrics = BotMetrics()
    observed: list[str] = []

    async def observe(event: EventEnvelope) -> None:
        observed.append(event.event_id)

    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(metrics),
        metrics=metrics,
        event_observer=observe,
        record_raw=False,
    )
    now = datetime.now(tz=timezone.utc)
    event = EventEnvelope(
        source=EventSource.HELIUS_WSS,
        protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.SWAP_BUY,
        slot=2,
        signature="untracked-swap",
        instruction_index=0,
        block_time=now,
        observed_at=now,
        mint="TOKEN",
        pool_address="UNTRACKED_POOL",
        payload={},
    )

    accepted = await pipeline.process_event(event)

    assert accepted is True
    assert observed == []
    assert metrics.chain_events_received._value.get() == 1
    assert (
        metrics.chain_event_state_filter_decisions.labels(
            reason="unknown_pool"
        )._value.get()
        == 1
    )


@pytest.mark.asyncio
async def test_recent_candidate_swap_requires_live_state(tmp_path) -> None:
    metrics = BotMetrics()
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(metrics),
        metrics=metrics,
        record_raw=False,
    )
    now = datetime.now(tz=timezone.utc)
    pool_event = EventEnvelope(
        source=EventSource.HELIUS_WSS,
        protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.POOL_CREATED,
        slot=3,
        signature="tracked-pool",
        instruction_index=0,
        block_time=now,
        observed_at=now,
        mint="TOKEN",
        pool_address="TRACKED_POOL",
        payload={
            "base_mint": "TOKEN",
            "quote_mint": "So11111111111111111111111111111111111111112",
            "base_mint_decimals": 6,
            "quote_mint_decimals": 9,
            "pool_base_amount": 1_000_000,
            "pool_quote_amount": 1_000_000_000,
        },
    )
    await pipeline.process_event(pool_event)
    swap_event = EventEnvelope(
        source=EventSource.HELIUS_WSS,
        protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.SWAP_BUY,
        slot=4,
        signature="tracked-swap",
        instruction_index=0,
        block_time=now + timedelta(seconds=1),
        observed_at=now + timedelta(seconds=1),
        mint="TOKEN",
        pool_address="TRACKED_POOL",
        payload={},
    )


    assert pipeline._event_state_filter_reason(swap_event) is None
    assert (
        pipeline._event_state_filter_reason(
            swap_event.model_copy(
                update={
                    "block_time": now - timedelta(seconds=1),
                    "observed_at": now - timedelta(seconds=1),
                }
            )
        )
        == "before_candidate"
    )
    expired_swap = swap_event.model_copy(
        update={
            "block_time": now + timedelta(seconds=181),
            "observed_at": now + timedelta(seconds=181),
        }
    )
    assert pipeline._event_state_filter_reason(expired_swap) == "expired"

    candidate_id, candidate = next(iter(pipeline.candidates.items()))
    for state in (
        CandidateState.POSITION_OPEN,
        CandidateState.POSITION_PARTIAL,
        CandidateState.EXIT_PENDING,
        CandidateState.RETRYING_EXIT,
    ):
        pipeline.candidates[candidate_id] = candidate.model_copy(
            update={"state": state}
        )
        assert pipeline._event_state_filter_reason(expired_swap) is None

    for state in (
        CandidateState.CLOSED,
        CandidateState.REJECTED,
    ):
        pipeline.candidates[candidate_id] = candidate.model_copy(
            update={"state": state}
        )
        assert pipeline._event_state_filter_reason(swap_event) == "terminal"

@pytest.mark.asyncio
async def test_rehydrate_event_reports_filtered_and_applied(
    tmp_path,
    monkeypatch,
) -> None:
    metrics = BotMetrics()
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="test",
        config_hash="hash",
        entry_gate=EntryGate(metrics),
        metrics=metrics,
        record_raw=False,
    )
    applied: list[str] = []

    async def capture_apply(
        event: EventEnvelope,
        *,
        persist: bool,
        observe: bool,
        allow_candidate: bool = True,
    ) -> None:
        applied.append(event.event_id)

    monkeypatch.setattr(pipeline, "_apply_event", capture_apply)
    now = datetime.now(tz=timezone.utc)
    filtered_swap = EventEnvelope(
        source=EventSource.REPLAY,
        protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.SWAP_BUY,
        slot=10,
        signature="rehydrate-filtered",
        instruction_index=0,
        block_time=now,
        observed_at=now,
        mint="TOKEN",
        pool_address="UNKNOWN-POOL",
        payload={},
    )
    applied_event = EventEnvelope(
        source=EventSource.REPLAY,
        protocol=Protocol.PUMP,
        event_type=ChainEventType.TOKEN_CREATED,
        slot=11,
        signature="rehydrate-applied",
        instruction_index=0,
        block_time=now,
        observed_at=now,
        mint="TOKEN",
        payload={},
    )

    assert await pipeline.rehydrate_event(filtered_swap) is False
    assert await pipeline.rehydrate_event(applied_event) is True
    assert applied == [applied_event.event_id]
