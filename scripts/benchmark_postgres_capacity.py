"""Fail CI if PostgreSQL ingestion cannot sustain a realistic Helius stream.

The gate replays a Pump/PumpSwap notification mix through the ordered
durable/state/archive workers against a real PostgreSQL database and asserts
the release capacity targets: sustained notification throughput, bounded
internal and feature latency, no lost or dropped events, a backlog that does
not trend upwards, and a bounded shutdown drain.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import struct
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import delete, func, select

from sniper_bot.database import Database
from sniper_bot.db_models import (
    CandidateRow,
    EventDedupRow,
    PoolRow,
    RawChainEventRow,
    TokenRow,
)
from sniper_bot.events import EventEnvelope, EventSource, Protocol
from sniper_bot.features import LiquidityObservation, TradeObservation
from sniper_bot.metrics import BotMetrics
from sniper_bot.pipeline import ConfirmationPipeline
from sniper_bot.protocols.anchor import _base58_encode
from sniper_bot.protocols.pump import PUMP_PROGRAM_ID
from sniper_bot.protocols.pumpswap import PUMPSWAP_PROGRAM_ID
from sniper_bot.registry import WSOL_MINT
from sniper_bot.stream import EntryGate

TARGET_NOTIFICATIONS_PER_SECOND = 800
WARMUP_SECONDS = float(os.environ.get("CAPACITY_WARMUP_SECONDS", "5"))
MEASURED_SECONDS = float(os.environ.get("CAPACITY_MEASURED_SECONDS", "20"))
SUBMIT_INTERVAL_SECONDS = 0.025
INTERNAL_EVENT_P95_LIMIT_MS = 250.0
FEATURE_P95_LIMIT_MS = 100.0
MAX_DRAIN_SECONDS = 60.0
BACKLOG_SAMPLE_INTERVAL_SECONDS = 0.25
# The ordered stage queues are bounded, so a pipeline that genuinely cannot
# keep up blocks its producer instead of growing an unbounded backlog: that
# shows up as lost throughput and rising latency, which the throughput, p95
# and drain criteria already fail on. The backlog trend therefore only has to
# separate a diverging queue from the ordinary in-flight batch, so it is
# measured against a small fraction of the ingest rate rather than zero.
BACKLOG_SLOPE_TOLERANCE_FRACTION = 0.01
MINIMUM_BACKLOG_SLOPE_LIMIT = 1.0
DISTINCT_POOLS = 64
BENCHMARK_SIGNATURE_PREFIX = "capacity-"
BENCHMARK_STRATEGY = "capacity"

# Roughly the observed mainnet mix: mostly PumpSwap swaps, a steady trickle of
# new pools, and Pump bonding-curve trades alongside them.
NOTIFICATION_MIX = (
    ("pumpswap", "BuyEvent", 55),
    ("pumpswap", "SellEvent", 30),
    ("pumpswap", "CreatePoolEvent", 3),
    ("pump", "TradeEvent", 12),
)

_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


class BorshEventEncoder:
    """Encode Anchor events straight from the vendored IDL used for decoding."""

    def __init__(self, idl_path: Path) -> None:
        with idl_path.open("r", encoding="utf-8") as stream:
            idl = json.load(stream)
        self.program_id = str(idl["address"])
        self._types = {item["name"]: item["type"] for item in idl.get("types", [])}
        self._discriminators = {
            item["name"]: bytes(item["discriminator"])
            for item in idl.get("events", [])
        }

    def encode(self, event_name: str, overrides: dict[str, Any]) -> str:
        definition = self._types[event_name]
        payload = bytearray(self._discriminators[event_name])
        for index, item in enumerate(definition["fields"]):
            payload += self._encode_type(
                item["type"],
                overrides.get(item["name"]),
                index,
            )
        return base64.b64encode(bytes(payload)).decode("ascii")

    def _encode_type(self, type_spec: Any, value: Any, index: int) -> bytes:
        if isinstance(type_spec, str):
            return self._encode_primitive(type_spec, value, index)
        if not isinstance(type_spec, dict):
            raise ValueError(f"unsupported benchmark IDL type: {type_spec!r}")
        if "option" in type_spec:
            if value is None:
                return b"\x00"
            return b"\x01" + self._encode_type(type_spec["option"], value, index)
        if "vec" in type_spec:
            items = list(value or [])
            payload = bytearray(struct.pack("<I", len(items)))
            for item in items:
                payload += self._encode_type(type_spec["vec"], item, index)
            return bytes(payload)
        if "array" in type_spec:
            item_type, length = type_spec["array"]
            items = list(value or [])
            payload = bytearray()
            for position in range(int(length)):
                item = items[position] if position < len(items) else None
                payload += self._encode_type(item_type, item, index)
            return bytes(payload)
        if "defined" in type_spec:
            defined = type_spec["defined"]
            name = defined["name"] if isinstance(defined, dict) else str(defined)
            return self._encode_defined(name, value, index)
        raise ValueError(f"unsupported benchmark IDL type: {type_spec!r}")

    def _encode_defined(self, name: str, value: Any, index: int) -> bytes:
        definition = self._types[name]
        if definition.get("kind") == "enum":
            return b"\x00"
        if definition.get("kind") != "struct":
            raise ValueError(f"unsupported benchmark defined type {name}")
        fields = value if isinstance(value, dict) else {}
        payload = bytearray()
        for item in definition.get("fields", []):
            payload += self._encode_type(
                item["type"], fields.get(item["name"]), index
            )
        return bytes(payload)

    def _encode_primitive(self, name: str, value: Any, index: int) -> bytes:
        formats = {
            "u8": "<B",
            "i8": "<b",
            "u16": "<H",
            "i16": "<h",
            "u32": "<I",
            "i32": "<i",
            "u64": "<Q",
            "i64": "<q",
        }
        if name in formats:
            return struct.pack(
                formats[name], int(value if value is not None else index + 1)
            )
        if name == "u128":
            return int(value or index + 1).to_bytes(16, "little", signed=False)
        if name == "i128":
            return int(value or index + 1).to_bytes(16, "little", signed=True)
        if name == "bool":
            return b"\x01" if bool(value) else b"\x00"
        if name == "pubkey":
            if value:
                return _base58_decode(str(value))
            return bytes([index % 251 + 1]) * 32
        if name == "string":
            encoded = str(
                value if value is not None else "benchmark"
            ).encode("utf-8")
            return struct.pack("<I", len(encoded)) + encoded
        raise ValueError(f"unsupported benchmark primitive IDL type {name}")


def _base58_decode(value: str) -> bytes:
    number = 0
    for character in value:
        number = number * 58 + _BASE58_ALPHABET.index(character)
    raw = number.to_bytes(32, "big")
    return raw[-32:]


def _address(prefix: int, index: int) -> str:
    return _base58_encode(bytes([prefix]) + index.to_bytes(4, "big") + bytes(27))


@dataclass
class CapacityMeasurements:
    submitted_at: dict[str, float] = field(default_factory=dict)
    internal_latencies_ms: list[float] = field(default_factory=list)
    feature_latencies_ms: list[float] = field(default_factory=list)
    backlog_samples: list[tuple[float, int]] = field(default_factory=list)
    notifications_submitted: int = 0
    measured_notifications: int = 0


def _p95(values: list[float]) -> float:
    if not values:
        raise ValueError("at least one latency sample is required")
    ordered = sorted(values)
    return ordered[max(0, (len(ordered) * 95 + 99) // 100 - 1)]


def _backlog_slope_limit(notifications_per_second: float) -> float:
    return max(
        MINIMUM_BACKLOG_SLOPE_LIMIT,
        BACKLOG_SLOPE_TOLERANCE_FRACTION * float(notifications_per_second),
    )


def _slope(samples: list[tuple[float, int]]) -> float:
    if len(samples) < 2:
        return 0.0
    count = len(samples)
    mean_x = sum(point[0] for point in samples) / count
    mean_y = sum(point[1] for point in samples) / count
    numerator = sum((point[0] - mean_x) * (point[1] - mean_y) for point in samples)
    denominator = sum((point[0] - mean_x) ** 2 for point in samples)
    return numerator / denominator if denominator else 0.0


class NotificationGenerator:
    def __init__(self) -> None:
        protocols = (
            Path(__file__).resolve().parents[1]
            / "src"
            / "sniper_bot"
            / "protocols"
        )
        self._encoders = {
            "pump": BorshEventEncoder(protocols / "pump" / "idl.json"),
            "pumpswap": BorshEventEncoder(protocols / "pumpswap" / "idl.json"),
        }
        self._programs = {
            "pump": PUMP_PROGRAM_ID,
            "pumpswap": PUMPSWAP_PROGRAM_ID,
        }
        self._plan = [
            (protocol, event_name)
            for protocol, event_name, weight in NOTIFICATION_MIX
            for _ in range(weight)
        ]
        self._sequence = 0
        self._block_time = int(datetime(2026, 9, 1, tzinfo=timezone.utc).timestamp())

    def next_notification(self) -> tuple[Protocol, dict[str, Any], EventSource]:
        index = self._sequence
        self._sequence += 1
        protocol_name, event_name = self._plan[index % len(self._plan)]
        pool_index = index % DISTINCT_POOLS
        overrides: dict[str, Any] = {
            "timestamp": self._block_time,
            "pool": _address(7, pool_index),
            "user": _address(9, index % 512),
            "creator": _address(9, index % 512),
            "base_mint": _address(11, pool_index),
            "quote_mint": WSOL_MINT,
            "mint": _address(11, pool_index),
            "bonding_curve": _address(13, pool_index),
            "base_mint_decimals": 6,
            "quote_mint_decimals": 9,
            "pool_base_amount": 1_000_000_000,
            "pool_quote_amount": 300_000_000_000,
            "pool_base_token_reserves": 1_000_000_000,
            "pool_quote_token_reserves": 300_000_000_000,
            "base_amount_out": 1_000_000,
            "base_amount_in": 1_000_000,
            "quote_amount_in": 300_000,
            "quote_amount_out": 300_000,
            "sol_amount": 300_000,
            "token_amount": 1_000_000,
            "is_buy": index % 2 == 0,
            "virtual_sol_reserves": 300_000_000_000,
            "virtual_token_reserves": 1_000_000_000,
            "real_sol_reserves": 300_000_000_000,
            "real_token_reserves": 1_000_000_000,
            "index": pool_index,
        }
        program_id = self._programs[protocol_name]
        payload = self._encoders[protocol_name].encode(event_name, overrides)
        transaction = {
            "slot": 400_000_000 + index,
            "blockTime": (
                self._block_time + index // TARGET_NOTIFICATIONS_PER_SECOND
            ),
            "signature": f"{BENCHMARK_SIGNATURE_PREFIX}{index:012d}",
            "meta": {
                "logMessages": [
                    f"Program {program_id} invoke [1]",
                    f"Program data: {payload}",
                    f"Program {program_id} success",
                ]
            },
        }
        protocol = Protocol.PUMP if protocol_name == "pump" else Protocol.PUMPSWAP
        return protocol, transaction, EventSource.HELIUS_WSS


def _instrument(
    pipeline: ConfirmationPipeline,
    measurements: CapacityMeasurements,
    measurement_started: list[float],
) -> None:
    original_state_batch = pipeline._apply_state_batch
    original_trade = pipeline.features.ingest_trade
    original_liquidity = pipeline.features.ingest_liquidity

    async def timed_state_batch(events: list[EventEnvelope]) -> None:
        await original_state_batch(events)
        completed = time.perf_counter()
        for event in events:
            submitted = measurements.submitted_at.pop(event.signature, None)
            if submitted is None:
                continue
            if measurement_started[0] and submitted >= measurement_started[0]:
                measurements.internal_latencies_ms.append(
                    (completed - submitted) * 1000
                )

    def timed_trade(observation: TradeObservation) -> bool:
        started = time.perf_counter()
        accepted = original_trade(observation)
        measurements.feature_latencies_ms.append(
            (time.perf_counter() - started) * 1000
        )
        return accepted

    def timed_liquidity(observation: LiquidityObservation) -> bool:
        started = time.perf_counter()
        accepted = original_liquidity(observation)
        measurements.feature_latencies_ms.append(
            (time.perf_counter() - started) * 1000
        )
        return accepted

    pipeline._apply_state_batch = timed_state_batch  # type: ignore[method-assign]
    pipeline.features.ingest_trade = timed_trade  # type: ignore[method-assign]
    pipeline.features.ingest_liquidity = timed_liquidity  # type: ignore[method-assign]


async def _sample_backlog(
    pipeline: ConfirmationPipeline,
    measurements: CapacityMeasurements,
    stop: asyncio.Event,
    measurement_started: list[float],
) -> None:
    while not stop.is_set():
        if measurement_started[0]:
            pending = sum(
                len(item.events)
                for stage in pipeline._stage_pending.values()
                for item in stage
            )
            measurements.backlog_samples.append(
                (time.perf_counter() - measurement_started[0], pending)
            )
        try:
            await asyncio.wait_for(
                stop.wait(), timeout=BACKLOG_SAMPLE_INTERVAL_SECONDS
            )
        except TimeoutError:
            continue


def _benchmark_event_ids() -> Any:
    return select(RawChainEventRow.event_id).where(
        RawChainEventRow.signature.like(f"{BENCHMARK_SIGNATURE_PREFIX}%")
    )


def _synthetic_mints() -> list[str]:
    return [_address(11, index) for index in range(DISTINCT_POOLS)]


def _synthetic_pools() -> list[str]:
    return [_address(7, index) for index in range(DISTINCT_POOLS)]


async def _register_benchmark_strategy(database: Database) -> None:
    # The runtime registers its strategy version before ingestion starts, and
    # candidates reference it, so the gate has to do the same.
    await database.register_strategy(
        strategy_id=BENCHMARK_STRATEGY,
        version=BENCHMARK_STRATEGY,
        config_hash=BENCHMARK_STRATEGY,
        config_json={},
        now=datetime.now(tz=timezone.utc),
    )


async def _clear_benchmark_rows(database: Database) -> None:
    async with database.sessions.begin() as session:
        # Children first: candidates reference tokens, pools and the strategy.
        await session.execute(
            delete(CandidateRow).where(
                CandidateRow.strategy_version_id == BENCHMARK_STRATEGY
            )
        )
        await session.execute(
            delete(EventDedupRow).where(
                EventDedupRow.event_id.in_(_benchmark_event_ids())
            )
        )
        await session.execute(
            delete(RawChainEventRow).where(
                RawChainEventRow.signature.like(
                    f"{BENCHMARK_SIGNATURE_PREFIX}%"
                )
            )
        )
        await session.execute(
            delete(PoolRow).where(
                PoolRow.pool_address.in_(_synthetic_pools())
            )
        )
        await session.execute(
            delete(TokenRow).where(TokenRow.mint.in_(_synthetic_mints()))
        )
        # strategy_versions is append-only by database trigger, and registering
        # the same row again is a no-op, so the gate leaves its row in place.


async def _count_benchmark_events(database: Database) -> tuple[int, int]:
    async with database.sessions() as session:
        persisted = int(
            await session.scalar(
                select(func.count())
                .select_from(EventDedupRow)
                .where(EventDedupRow.event_id.in_(_benchmark_event_ids()))
            )
            or 0
        )
        processed = int(
            await session.scalar(
                select(func.count())
                .select_from(EventDedupRow)
                .where(
                    EventDedupRow.event_id.in_(_benchmark_event_ids()),
                    EventDedupRow.processing_status == "PROCESSED",
                )
            )
            or 0
        )
    return persisted, processed


def _dropped_events(metrics: BotMetrics) -> int:
    total = 0.0
    for metric in metrics.registry.collect():
        if metric.name != "ingestion_events_dropped":
            continue
        for sample in metric.samples:
            if sample.name.endswith("_total"):
                total += sample.value
    return int(total)


async def _run_capacity_gate(dsn: str) -> dict[str, Any]:
    generator = NotificationGenerator()
    measurements = CapacityMeasurements()
    measurement_started = [0.0]
    metrics = BotMetrics()
    database = Database(dsn, metrics=metrics)
    stop_sampling = asyncio.Event()
    submit_wall_seconds = 0.0
    drain_seconds = 0.0
    persisted = 0
    processed = 0

    with tempfile.TemporaryDirectory(prefix="sniper-capacity-") as temp_dir:
        pipeline = ConfirmationPipeline(
            data_dir=temp_dir,
            strategy_version=BENCHMARK_STRATEGY,
            config_hash=BENCHMARK_STRATEGY,
            entry_gate=EntryGate(metrics),
            metrics=metrics,
            database=database,
            record_raw=True,
        )
        _instrument(pipeline, measurements, measurement_started)
        sampler = asyncio.create_task(
            _sample_backlog(
                pipeline, measurements, stop_sampling, measurement_started
            )
        )
        try:
            await _clear_benchmark_rows(database)
            await _register_benchmark_strategy(database)
            await pipeline.start_background_workers()

            per_tick = max(
                1,
                round(TARGET_NOTIFICATIONS_PER_SECOND * SUBMIT_INTERVAL_SECONDS),
            )
            loop = asyncio.get_running_loop()
            run_started = loop.time()
            warmup_deadline = run_started + WARMUP_SECONDS
            deadline = warmup_deadline + MEASURED_SECONDS
            next_tick = run_started
            submit_wall_started = 0.0

            while loop.time() < deadline:
                now = loop.time()
                if now < next_tick:
                    await asyncio.sleep(next_tick - now)
                next_tick += SUBMIT_INTERVAL_SECONDS
                measuring = loop.time() >= warmup_deadline
                if measuring and not measurement_started[0]:
                    measurement_started[0] = time.perf_counter()
                    submit_wall_started = measurement_started[0]
                batch = [generator.next_notification() for _ in range(per_tick)]
                submitted = time.perf_counter()
                for _protocol, transaction, _source in batch:
                    measurements.submitted_at[
                        str(transaction["signature"])
                    ] = submitted
                await pipeline.process_transactions(batch)
                measurements.notifications_submitted += len(batch)
                if measuring:
                    measurements.measured_notifications += len(batch)

            submit_wall_seconds = time.perf_counter() - submit_wall_started
            # Stop sampling before the drain so the backlog trend describes the
            # measured window only and cannot be flattened by shutdown.
            stop_sampling.set()
            await sampler
            drain_started = time.perf_counter()
            await pipeline.stop_background_workers(
                timeout_seconds=MAX_DRAIN_SECONDS
            )
            drain_seconds = time.perf_counter() - drain_started
            persisted, processed = await _count_benchmark_events(database)
            await _clear_benchmark_rows(database)
        finally:
            stop_sampling.set()
            await asyncio.gather(sampler, return_exceptions=True)
            await database.close()

    throughput = (
        measurements.measured_notifications / submit_wall_seconds
        if submit_wall_seconds > 0
        else 0.0
    )
    return {
        "notifications_submitted": measurements.notifications_submitted,
        "measured_notifications": measurements.measured_notifications,
        "notifications_per_second": round(throughput, 1),
        "notifications_per_second_target": TARGET_NOTIFICATIONS_PER_SECOND,
        "internal_event_p95_ms": round(_p95(measurements.internal_latencies_ms), 3),
        "internal_event_limit_ms": INTERNAL_EVENT_P95_LIMIT_MS,
        "feature_update_p95_ms": round(_p95(measurements.feature_latencies_ms), 3),
        "feature_update_limit_ms": FEATURE_P95_LIMIT_MS,
        "backlog_slope_events_per_second": round(
            _slope(measurements.backlog_samples), 3
        ),
        "final_backlog_events": (
            measurements.backlog_samples[-1][1]
            if measurements.backlog_samples
            else 0
        ),
        "dropped_events": _dropped_events(metrics),
        "events_persisted": persisted,
        "events_processed": processed,
        "drain_seconds": round(drain_seconds, 3),
        "drain_limit_seconds": MAX_DRAIN_SECONDS,
    }


def _failures(result: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    if result["notifications_per_second"] < TARGET_NOTIFICATIONS_PER_SECOND:
        failures.append(
            "sustained throughput "
            f"{result['notifications_per_second']}/s is below "
            f"{TARGET_NOTIFICATIONS_PER_SECOND}/s"
        )
    if result["internal_event_p95_ms"] >= INTERNAL_EVENT_P95_LIMIT_MS:
        failures.append(
            f"internal event p95 {result['internal_event_p95_ms']} ms reaches "
            f"{INTERNAL_EVENT_P95_LIMIT_MS} ms"
        )
    if result["feature_update_p95_ms"] >= FEATURE_P95_LIMIT_MS:
        failures.append(
            f"feature update p95 {result['feature_update_p95_ms']} ms reaches "
            f"{FEATURE_P95_LIMIT_MS} ms"
        )
    slope_limit = _backlog_slope_limit(result["notifications_per_second"])
    if result["backlog_slope_events_per_second"] > slope_limit:
        failures.append(
            "ingestion backlog trends upwards at "
            f"{result['backlog_slope_events_per_second']} events/s, "
            f"above {round(slope_limit, 3)} events/s"
        )
    if result["dropped_events"]:
        failures.append(f"{result['dropped_events']} events were dropped")
    if result["events_persisted"] != result["events_processed"]:
        failures.append(
            f"{result['events_persisted'] - result['events_processed']} persisted "
            "events never reached applied state"
        )
    if result["drain_seconds"] > MAX_DRAIN_SECONDS:
        failures.append(
            f"shutdown drain took {result['drain_seconds']} s, above "
            f"{MAX_DRAIN_SECONDS} s"
        )
    return failures


def main() -> None:
    dsn = (
        os.environ.get("CAPACITY_POSTGRES_DSN")
        or os.environ.get("MIGRATION_POSTGRES_DSN")
        or os.environ.get("POSTGRES_DSN")
        or ""
    )
    if not dsn:
        raise SystemExit(
            "the PostgreSQL capacity gate requires POSTGRES_DSN, "
            "MIGRATION_POSTGRES_DSN, or CAPACITY_POSTGRES_DSN"
        )
    if not dsn.startswith("postgresql"):
        raise SystemExit("the capacity gate must run against PostgreSQL")
    result = asyncio.run(_run_capacity_gate(dsn))
    failures = _failures(result)
    print(json.dumps({**result, "failures": failures}, sort_keys=True))
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
