from __future__ import annotations

from typing import Any

from scripts.benchmark_postgres_capacity import (
    FEATURE_P95_LIMIT_MS,
    INTERNAL_EVENT_P95_LIMIT_MS,
    MAX_DRAIN_SECONDS,
    TARGET_NOTIFICATIONS_PER_SECOND,
    NotificationGenerator,
    _backlog_slope_limit,
    _failures,
    _phase_p95_ms,
    _slope,
)
from sniper_bot.events import ChainEventType, Protocol
from sniper_bot.metrics import BotMetrics
from sniper_bot.protocols.pump import PumpDecoder
from sniper_bot.protocols.pumpswap import PumpSwapDecoder


def _passing_result() -> dict[str, Any]:
    return {
        "notifications_per_second": float(TARGET_NOTIFICATIONS_PER_SECOND),
        "internal_event_p95_ms": 40.0,
        "feature_update_p95_ms": 0.5,
        "backlog_slope_events_per_second": -2.0,
        "dropped_events": 0,
        "events_persisted": 16_000,
        "events_processed": 16_000,
        "drain_seconds": 4.0,
    }


def test_generated_notifications_decode_as_a_pump_pumpswap_mix() -> None:
    generator = NotificationGenerator()
    pump = PumpDecoder()
    pumpswap = PumpSwapDecoder()
    seen: set[tuple[Protocol, ChainEventType]] = set()
    decoded_count = 0

    for _ in range(200):
        protocol, transaction, source = generator.next_notification()
        decoder = pump if protocol is Protocol.PUMP else pumpswap
        events = decoder.decode_transaction(transaction, source=source)
        assert events
        for event in events:
            assert event.signature.startswith("capacity-")
            seen.add((event.protocol, event.event_type))
            decoded_count += 1

    assert decoded_count >= 200
    assert (Protocol.PUMPSWAP, ChainEventType.SWAP_BUY) in seen
    assert (Protocol.PUMPSWAP, ChainEventType.SWAP_SELL) in seen
    assert (Protocol.PUMPSWAP, ChainEventType.POOL_CREATED) in seen
    assert (Protocol.PUMP, ChainEventType.SWAP_BUY) in seen


def test_generated_notification_signatures_are_unique() -> None:
    generator = NotificationGenerator()
    signatures = {
        str(generator.next_notification()[1]["signature"]) for _ in range(500)
    }

    assert len(signatures) == 500


def test_capacity_gate_accepts_a_result_that_meets_every_target() -> None:
    assert _failures(_passing_result()) == []


def test_capacity_gate_rejects_each_missed_target() -> None:
    below_throughput = {
        **_passing_result(),
        "notifications_per_second": TARGET_NOTIFICATIONS_PER_SECOND * 0.9,
    }
    slow_events = {
        **_passing_result(),
        "internal_event_p95_ms": INTERNAL_EVENT_P95_LIMIT_MS,
    }
    slow_features = {
        **_passing_result(),
        "feature_update_p95_ms": FEATURE_P95_LIMIT_MS,
    }
    growing_backlog = {
        **_passing_result(),
        "backlog_slope_events_per_second": 825.0,
    }
    dropped = {**_passing_result(), "dropped_events": 3}
    unapplied = {**_passing_result(), "events_processed": 15_999}
    slow_drain = {
        **_passing_result(),
        "drain_seconds": MAX_DRAIN_SECONDS + 0.5,
    }

    assert "below" in _failures(below_throughput)[0]
    assert "internal event p95" in _failures(slow_events)[0]
    assert "feature update p95" in _failures(slow_features)[0]
    assert "backlog trends upwards" in _failures(growing_backlog)[0]
    assert "were dropped" in _failures(dropped)[0]
    assert "never reached applied state" in _failures(unapplied)[0]
    assert "shutdown drain" in _failures(slow_drain)[0]


def test_backlog_slope_separates_a_rising_queue_from_a_stable_one() -> None:
    rising = [(float(index), index * 100) for index in range(10)]
    stable = [(float(index), 40 if index % 2 else 38) for index in range(10)]

    assert _slope(rising) > 0
    assert _slope(stable) <= 0.5
    assert _slope([]) == 0.0


def test_one_in_flight_batch_is_not_a_diverging_backlog() -> None:
    # The stage queues are bounded, so an ordinary in-flight batch always
    # leaves a small positive trend. Only divergence may fail the gate.
    in_flight = {
        **_passing_result(),
        "backlog_slope_events_per_second": 2.6,
    }
    diverging = {
        **_passing_result(),
        "backlog_slope_events_per_second": 826.0,
    }

    assert _backlog_slope_limit(800.0) == 8.0
    assert _backlog_slope_limit(10.0) == 1.0
    assert _failures(in_flight) == []
    assert "backlog trends upwards" in _failures(diverging)[0]


def test_phase_p95_reports_each_instrumented_stage() -> None:
    metrics = BotMetrics()
    for seconds in (0.002, 0.004, 0.3):
        metrics.chain_batch_phase_seconds.labels(phase="state_commit").observe(seconds)
    metrics.postgres_event_ingest_phase_seconds.labels(phase="commit").observe(0.02)

    phases = _phase_p95_ms(metrics)

    assert phases["chain_batch_phase_seconds:state_commit"] >= 250.0
    assert phases["postgres_event_ingest_phase_seconds:commit"] <= 25.0
    assert _phase_p95_ms(BotMetrics()) == {}


def test_submit_jitter_passes_but_real_backpressure_fails() -> None:
    # Sub-percent jitter is the driver's own scheduling, not a system property.
    jitter = {
        **_passing_result(),
        "notifications_per_second": TARGET_NOTIFICATIONS_PER_SECOND * 0.9985,
    }
    # The contended VM measured this far below its target.
    backpressure = {
        **_passing_result(),
        "notifications_per_second": TARGET_NOTIFICATIONS_PER_SECOND * 0.956,
    }

    assert _failures(jitter) == []
    assert "sustained throughput" in _failures(backpressure)[0]
