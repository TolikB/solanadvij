from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from sniper_bot.events import ChainEventType, EventEnvelope, EventSource, Protocol
from sniper_bot.features import TradeSide
from sniper_bot.metrics import BotMetrics
from sniper_bot.pipeline import ConfirmationPipeline
from sniper_bot.registry import WSOL_MINT, QuoteAssetPrice
from sniper_bot.stream import EntryGate

TOKEN_MINT = "2r8hyN4p3uTtTjGkkyzhNt4Ygxe3J1JnQPqCoi3pyyvu"


def _pipeline(tmp_path) -> ConfirmationPipeline:
    metrics = BotMetrics()
    return ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="strategy-v1",
        config_hash="config-hash",
        entry_gate=EntryGate(metrics),
        metrics=metrics,
        record_raw=False,
    )


def _pool_event(source: EventSource, now: datetime) -> EventEnvelope:
    return EventEnvelope(
        source=source,
        protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.POOL_CREATED,
        slot=1,
        signature=f"pool-{source.value}",
        instruction_index=1,
        block_time=now,
        observed_at=now,
        mint=TOKEN_MINT,
        pool_address="POOL",
        payload={
            "base_mint": WSOL_MINT,
            "quote_mint": TOKEN_MINT,
            "base_mint_decimals": 9,
            "quote_mint_decimals": 6,
            "pool_base_amount": 84_990_000_000,
            "pool_quote_amount": 1_000_000_000_000_000,
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source",
    [EventSource.BASELINE_WSS, EventSource.RPC_RECOVERY],
)
async def test_non_tradable_sources_materialize_pool_without_candidate(
    tmp_path,
    source: EventSource,
) -> None:
    now = datetime(2026, 8, 25, 8, 35, tzinfo=timezone.utc)
    pipeline = _pipeline(tmp_path)

    assert await pipeline.process_event(_pool_event(source, now)) is True

    pool = pipeline.pools.pool("POOL")
    assert pool is not None
    assert pool.base_mint == TOKEN_MINT
    assert pipeline.tokens.get(TOKEN_MINT) is not None
    assert pipeline.list_candidates() == []


@pytest.mark.asyncio
async def test_reversed_pool_flips_trade_side_and_uses_wsol_volume(tmp_path) -> None:
    now = datetime(2026, 8, 25, 8, 35, tzinfo=timezone.utc)
    pipeline = _pipeline(tmp_path)
    pipeline.pools.set_quote_price(
        QuoteAssetPrice(mint=WSOL_MINT, price_usd=Decimal("200"), observed_at=now)
    )
    await pipeline.process_event(_pool_event(EventSource.HELIUS_WSS, now))
    swap_time = now + timedelta(seconds=1)
    await pipeline.process_event(
        EventEnvelope(
            source=EventSource.HELIUS_WSS,
            protocol=Protocol.PUMPSWAP,
            event_type=ChainEventType.SWAP_BUY,
            slot=2,
            signature="source-buy",
            instruction_index=1,
            block_time=swap_time,
            observed_at=swap_time,
            pool_address="POOL",
            payload={
                "pool_base_token_reserves": 83_990_000_000,
                "pool_quote_token_reserves": 1_001_000_000_000_000,
                "base_amount_out": 1_000_000_000,
                "quote_amount_in": 1_000_000_000_000,
                "virtual_quote_reserves": 0,
                "user": "wallet",
            },
        )
    )

    trades = pipeline.features.trades("POOL")
    assert len(trades) == 1
    assert trades[0].side == TradeSide.SELL
    assert trades[0].volume_usd == Decimal("200")
    token = pipeline.tokens.get(TOKEN_MINT)
    assert token is not None
    assert token.updated_at == swap_time


@pytest.mark.asyncio
async def test_live_pool_without_unique_supported_quote_has_no_candidate(tmp_path) -> None:
    now = datetime(2026, 8, 25, 8, 35, tzinfo=timezone.utc)
    pipeline = _pipeline(tmp_path)
    event = EventEnvelope(
        source=EventSource.HELIUS_WSS,
        protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.POOL_CREATED,
        slot=1,
        signature="unsupported-pair",
        instruction_index=1,
        block_time=now,
        observed_at=now,
        mint="TOKEN-A",
        pool_address="UNSUPPORTED",
        payload={
            "base_mint": "TOKEN-A",
            "quote_mint": "TOKEN-B",
            "base_mint_decimals": 6,
            "quote_mint_decimals": 6,
            "pool_base_amount": 1_000_000,
            "pool_quote_amount": 1_000_000,
        },
    )

    assert await pipeline.process_event(event) is True

    assert pipeline.pools.pool("UNSUPPORTED") is not None
    assert pipeline.list_candidates() == []
