from __future__ import annotations

import asyncio
import json
import tempfile
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from sniper_bot.database import Database
from sniper_bot.events import ChainEventType, EventEnvelope, EventSource, Protocol
from sniper_bot.features import (
    EventTimeFeatureEngine,
    HolderObservation,
    LiquidityObservation,
    TradeObservation,
    TradeSide,
)

FEATURE_P95_LIMIT_MS = 100.0
INTERNAL_EVENT_P95_LIMIT_MS = 250.0
WARMUP_EVENTS = 100
MEASURED_EVENTS = 1_000


def _p95(values: list[float]) -> float:
    if not values:
        raise ValueError("at least one latency sample is required")
    ordered = sorted(values)
    return ordered[max(0, (len(ordered) * 95 + 99) // 100 - 1)]


async def _run_benchmark() -> dict[str, int | float]:
    engine = EventTimeFeatureEngine()
    start = datetime(2026, 8, 24, tzinfo=timezone.utc)
    engine.register_pool("BENCHMARK_POOL", start)
    engine.ingest_liquidity(
        LiquidityObservation(
            event_id="liquidity",
            pool_address="BENCHMARK_POOL",
            event_time=start,
            quote_liquidity_usd=Decimal("50000"),
            market_cap_usd=Decimal("250000"),
        )
    )
    engine.ingest_holders(
        HolderObservation(
            event_id="holders",
            pool_address="BENCHMARK_POOL",
            event_time=start,
            holder_count=20,
            top_10_holders_pct=Decimal("0.20"),
            dev_cluster_holding_pct=Decimal("0.02"),
            largest_related_cluster_pct=Decimal("0.05"),
        )
    )

    with tempfile.TemporaryDirectory(prefix="sniper-benchmark-") as temp_dir:
        database_path = Path(temp_dir, "benchmark.db").as_posix()
        database = Database(f"sqlite+aiosqlite:///{database_path}")
        try:
            await database.create_schema_for_tests()
            feature_latencies: list[float] = []
            event_latencies: list[float] = []
            total = WARMUP_EVENTS + MEASURED_EVENTS
            for index in range(total):
                event_time = start + timedelta(milliseconds=index)
                event = EventEnvelope(
                    source=EventSource.REPLAY,
                    protocol=Protocol.PUMPSWAP,
                    event_type=ChainEventType.SWAP_BUY if index % 3 else ChainEventType.SWAP_SELL,
                    slot=index,
                    signature=f"benchmark-{index}",
                    instruction_index=0,
                    block_time=event_time,
                    observed_at=event_time,
                    pool_address="BENCHMARK_POOL",
                    payload={"benchmark": True},
                )
                event_started = time.perf_counter_ns()
                claimed = await database.record_event(event)
                if not claimed:
                    raise RuntimeError("benchmark event was unexpectedly deduplicated")

                feature_started = time.perf_counter_ns()
                accepted = engine.ingest_trade(
                    TradeObservation(
                        event_id=event.event_id,
                        pool_address="BENCHMARK_POOL",
                        event_time=event_time,
                        side=TradeSide.BUY if index % 3 else TradeSide.SELL,
                        wallet=f"wallet-{index % 100}",
                        volume_usd=Decimal("10"),
                        price_usd=Decimal("1.1"),
                    )
                )
                feature_ms = (time.perf_counter_ns() - feature_started) / 1_000_000
                if not accepted:
                    raise RuntimeError("benchmark feature event was unexpectedly deduplicated")
                engine.snapshot("BENCHMARK_POOL", event_time)
                await database.mark_event_processed(event.event_id, processed_at=event_time)
                event_ms = (time.perf_counter_ns() - event_started) / 1_000_000
                if index >= WARMUP_EVENTS:
                    feature_latencies.append(feature_ms)
                    event_latencies.append(event_ms)
        finally:
            await database.close()

    feature_p95 = _p95(feature_latencies)
    event_p95 = _p95(event_latencies)
    return {
        "samples": MEASURED_EVENTS,
        "feature_update_p95_ms": round(feature_p95, 3),
        "feature_update_limit_ms": FEATURE_P95_LIMIT_MS,
        "durable_event_processing_p95_ms": round(event_p95, 3),
        "durable_event_processing_limit_ms": INTERNAL_EVENT_P95_LIMIT_MS,
    }


def main() -> None:
    result = asyncio.run(_run_benchmark())
    print(json.dumps(result, sort_keys=True))
    if (
        result["feature_update_p95_ms"] >= FEATURE_P95_LIMIT_MS
        or result["durable_event_processing_p95_ms"] >= INTERNAL_EVENT_P95_LIMIT_MS
    ):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
