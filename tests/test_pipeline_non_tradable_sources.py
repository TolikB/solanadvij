from __future__ import annotations

from datetime import datetime, timezone

import pytest

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
