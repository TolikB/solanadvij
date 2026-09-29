from __future__ import annotations

from typing import Any

from scripts.soak_check import Observation, Thresholds, evaluate, parse_prometheus
from sniper_bot.metrics import BotMetrics


def _metrics_text(
    *,
    received: int = 0,
    dropped: int = 0,
    notification_depth: int = 0,
    reconnects: int = 0,
    jupiter_ok: int = 0,
    jupiter_errors: int = 0,
    rejections: int = 0,
    quarantines: int = 0,
    backlog: int = 0,
) -> str:
    metrics = BotMetrics()
    metrics.chain_events_received.inc(received)
    if dropped:
        metrics.ingestion_events_dropped.labels(stage="notification").inc(dropped)
    metrics.event_notification_queue_depth.set(notification_depth)
    metrics.websocket_reconnects.inc(reconnects)
    metrics.jupiter_requests.labels(status="ok").inc(jupiter_ok)
    if jupiter_errors:
        metrics.jupiter_requests.labels(status="http_429").inc(jupiter_errors)
    if rejections:
        metrics.candidate_rejections.labels(reason="LOW_QUOTE_LIQUIDITY").inc(rejections)
    if quarantines:
        metrics.protocol_layout_quarantines.labels(
            protocol="pump", kind="unknown_discriminator"
        ).inc(quarantines)
    metrics.ingestion_backlog_events.labels(stage="durable").set(backlog)
    return metrics.render().decode("utf-8")


def _observation(at: float, ready: bool = True, lag: float = 0.8, **values: Any) -> Observation:
    return Observation(
        at=at,
        metrics=parse_prometheus(_metrics_text(**values)),
        ready=ready,
        status={"stream_processing_lag_seconds": lag, "entry_block_reasons": []},
    )


def _healthy_window() -> list[Observation]:
    return [
        _observation(0, received=100, jupiter_ok=10, reconnects=1),
        _observation(900, received=40_000, jupiter_ok=400, reconnects=1, rejections=50),
        _observation(1800, received=80_000, jupiter_ok=800, reconnects=2, rejections=100),
    ]


def test_prometheus_parser_keeps_labels_and_values() -> None:
    sample = parse_prometheus(_metrics_text(jupiter_ok=3, jupiter_errors=2))

    assert sample[("jupiter_requests_total", (("status", "ok"),))] == 3.0
    assert sample[("jupiter_requests_total", (("status", "http_429"),))] == 2.0


def test_healthy_real_stream_window_passes() -> None:
    result = evaluate(_healthy_window(), Thresholds())

    assert result["passed"] is True, result["criteria"]
    assert result["window_seconds"] == 1800


def test_each_ingestion_failure_mode_fails_the_gate() -> None:
    cases = {
        "no_dropped_events": {"dropped": 3},
        "notification_queue_bounded": {"notification_depth": 16_384},
        "ordered_stage_backlog_bounded": {"backlog": 50_000},
        "no_reconnect_churn": {"reconnects": 9},
        "no_protocol_layout_quarantine": {"quarantines": 1},
        "jupiter_quotes_working": {"jupiter_errors": 5_000},
    }
    for criterion, fault in cases.items():
        window = _healthy_window()
        values = {
            "received": 80_000,
            "jupiter_ok": 800,
            "reconnects": 2,
            "rejections": 100,
            **fault,
        }
        window[-1] = _observation(1800, **values)
        result = evaluate(window, Thresholds())
        failed = {item["name"] for item in result["criteria"] if not item["passed"]}
        assert failed == {criterion}, (criterion, result["criteria"])


def test_stalled_stream_unready_runtime_and_missing_candidates_fail() -> None:
    stalled = [
        _observation(0, received=100, jupiter_ok=10),
        _observation(1800, ready=False, lag=40.0, received=100, jupiter_ok=20),
    ]

    result = evaluate(stalled, Thresholds())

    failed = {item["name"] for item in result["criteria"] if not item["passed"]}
    assert failed == {
        "events_accumulating",
        "stream_processing_lag",
        "runtime_ready",
        "candidates_reach_outcomes",
    }
    assert "candidates_reach_outcomes" not in {
        item["name"]
        for item in evaluate(stalled, Thresholds(require_candidates=False))["criteria"]
    }
