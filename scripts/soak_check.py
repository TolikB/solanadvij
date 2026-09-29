"""Soak and monitoring gate for a running bot, read from its local API.

It samples ``/metrics``, ``/health/ready`` and ``/api/v1/status`` for a fixed
duration and fails unless ingestion kept up with the real stream: no dropped
events, bounded queues and backlog, no reconnect churn, no protocol layout
quarantine, events and candidates accumulating, working Jupiter quotes, and a
ready runtime. It prints one JSON document and exits non-zero on failure.

    python scripts/soak_check.py --base-url http://127.0.0.1:8080 --duration 1800

Only the standard library is used so it runs on the host or in the image.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

Labels = tuple[tuple[str, str], ...]
MetricSample = dict[tuple[str, Labels], float]

# Well under the 16384-notification ingress queue and the 2000-item
# processing queue: a healthy consumer drains both within one batch window.
DEFAULT_MAX_NOTIFICATION_QUEUE = 1024
DEFAULT_MAX_PROCESSING_QUEUE = 1000
DEFAULT_MAX_STAGE_BACKLOG = 5000
DEFAULT_MAX_OLDEST_EVENT_AGE_SECONDS = 5.0
DEFAULT_MAX_RECONNECTS = 1
DEFAULT_MAX_JUPITER_ERROR_RATIO = 0.2
DEFAULT_MIN_READY_RATIO = 0.9
DEFAULT_MAX_PROCESSING_LAG_SECONDS = 3.0


def parse_prometheus(text: str) -> MetricSample:
    samples: MetricSample = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        name_part, _, value_part = line.rpartition(" ")
        if not name_part:
            continue
        try:
            value = float(value_part)
        except ValueError:
            continue
        name, labels = name_part, ""
        if "{" in name_part and name_part.endswith("}"):
            name, _, labels = name_part[:-1].partition("{")
        parsed: list[tuple[str, str]] = []
        for item in _split_labels(labels):
            key, _, raw = item.partition("=")
            parsed.append((key.strip(), raw.strip().strip('"')))
        samples[(name, tuple(sorted(parsed)))] = value
    return samples


def _split_labels(labels: str) -> Iterable[str]:
    current: list[str] = []
    quoted = False
    for character in labels:
        if character == '"':
            quoted = not quoted
        if character == "," and not quoted:
            yield "".join(current)
            current = []
            continue
        current.append(character)
    if current:
        yield "".join(current)


def metric_sum(
    sample: MetricSample,
    name: str,
    predicate: Callable[[dict[str, str]], bool] | None = None,
) -> float:
    total = 0.0
    for (metric, labels), value in sample.items():
        if metric != name:
            continue
        if predicate is not None and not predicate(dict(labels)):
            continue
        total += value
    return total


def metric_max(sample: MetricSample, name: str) -> float:
    values = [value for (metric, _), value in sample.items() if metric == name]
    return max(values) if values else 0.0


@dataclass
class Observation:
    at: float
    metrics: MetricSample
    ready: bool
    status: dict[str, Any] = field(default_factory=dict)


@dataclass
class Thresholds:
    max_notification_queue: float = DEFAULT_MAX_NOTIFICATION_QUEUE
    max_processing_queue: float = DEFAULT_MAX_PROCESSING_QUEUE
    max_stage_backlog: float = DEFAULT_MAX_STAGE_BACKLOG
    max_oldest_event_age_seconds: float = DEFAULT_MAX_OLDEST_EVENT_AGE_SECONDS
    max_reconnects: float = DEFAULT_MAX_RECONNECTS
    max_jupiter_error_ratio: float = DEFAULT_MAX_JUPITER_ERROR_RATIO
    min_ready_ratio: float = DEFAULT_MIN_READY_RATIO
    max_processing_lag_seconds: float = DEFAULT_MAX_PROCESSING_LAG_SECONDS
    require_candidates: bool = True


def _criterion(name: str, passed: bool, actual: Any, expected: str) -> dict[str, Any]:
    return {"name": name, "passed": bool(passed), "actual": actual, "expected": expected}


def evaluate(observations: list[Observation], thresholds: Thresholds) -> dict[str, Any]:
    if len(observations) < 2:
        raise ValueError("at least two observations are required")
    first, last = observations[0].metrics, observations[-1].metrics

    def delta(name: str, predicate: Callable[[dict[str, str]], bool] | None = None) -> float:
        return metric_sum(last, name, predicate) - metric_sum(first, name, predicate)

    def peak(name: str) -> float:
        return max(metric_max(item.metrics, name) for item in observations)

    jupiter_ok = delta("jupiter_requests_total", lambda labels: labels.get("status") == "ok")
    jupiter_all = delta("jupiter_requests_total")
    jupiter_errors = jupiter_all - jupiter_ok
    error_ratio = jupiter_errors / jupiter_all if jupiter_all else 0.0
    ready_ratio = sum(1 for item in observations if item.ready) / len(observations)
    lags = [
        float(item.status["stream_processing_lag_seconds"])
        for item in observations
        if isinstance(item.status.get("stream_processing_lag_seconds"), (int, float))
    ]
    lag_p95 = sorted(lags)[max(0, (len(lags) * 95 + 99) // 100 - 1)] if lags else None
    reasons = sorted(
        {
            str(reason)
            for item in observations
            for reason in item.status.get("entry_block_reasons") or []
        }
    )
    protocol_blocks = [reason for reason in reasons if reason.startswith("protocol:")]
    candidates_created = delta("candidate_rejections_total") + metric_sum(
        last, "candidate_count"
    )

    criteria = [
        _criterion(
            "no_dropped_events",
            metric_sum(last, "ingestion_events_dropped_total") == 0,
            metric_sum(last, "ingestion_events_dropped_total"),
            "0 events dropped since start",
        ),
        _criterion(
            "notification_queue_bounded",
            peak("event_notification_queue_depth") <= thresholds.max_notification_queue,
            peak("event_notification_queue_depth"),
            f"<= {thresholds.max_notification_queue:g}",
        ),
        _criterion(
            "processing_queue_bounded",
            peak("event_processing_queue_depth") <= thresholds.max_processing_queue,
            peak("event_processing_queue_depth"),
            f"<= {thresholds.max_processing_queue:g}",
        ),
        _criterion(
            "ordered_stage_backlog_bounded",
            peak("ingestion_backlog_events") <= thresholds.max_stage_backlog,
            peak("ingestion_backlog_events"),
            f"<= {thresholds.max_stage_backlog:g}",
        ),
        _criterion(
            "ordered_stage_age_bounded",
            peak("ingestion_oldest_event_age_seconds")
            <= thresholds.max_oldest_event_age_seconds,
            round(peak("ingestion_oldest_event_age_seconds"), 3),
            f"<= {thresholds.max_oldest_event_age_seconds:g}s",
        ),
        _criterion(
            "no_reconnect_churn",
            delta("websocket_reconnects_total") <= thresholds.max_reconnects,
            delta("websocket_reconnects_total"),
            f"<= {thresholds.max_reconnects:g} during the window",
        ),
        _criterion(
            "no_open_recovery_gap",
            metric_max(last, "stream_recovery_gap_active") == 0,
            metric_max(last, "stream_recovery_gap_active"),
            "0 at the end of the window",
        ),
        _criterion(
            "no_protocol_layout_quarantine",
            metric_sum(last, "protocol_layout_quarantines_total") == 0 and not protocol_blocks,
            {
                "quarantines": metric_sum(last, "protocol_layout_quarantines_total"),
                "blocked": protocol_blocks,
            },
            "vendored IDLs match every consumed event",
        ),
        _criterion(
            "events_accumulating",
            delta("chain_events_received_total") > 0,
            delta("chain_events_received_total"),
            "> 0 new events",
        ),
        _criterion(
            "jupiter_quotes_working",
            jupiter_ok > 0 and error_ratio <= thresholds.max_jupiter_error_ratio,
            {"ok": jupiter_ok, "errors": jupiter_errors, "error_ratio": round(error_ratio, 4)},
            f"> 0 ok and error ratio <= {thresholds.max_jupiter_error_ratio:g}",
        ),
        _criterion(
            "stream_processing_lag",
            lag_p95 is not None and lag_p95 <= thresholds.max_processing_lag_seconds,
            lag_p95,
            f"p95 <= {thresholds.max_processing_lag_seconds:g}s",
        ),
        _criterion(
            "runtime_ready",
            ready_ratio >= thresholds.min_ready_ratio and observations[-1].ready,
            {"ready_ratio": round(ready_ratio, 4), "ready_now": observations[-1].ready},
            f">= {thresholds.min_ready_ratio:g} of samples and ready at the end",
        ),
    ]
    if thresholds.require_candidates:
        criteria.append(
            _criterion(
                "candidates_reach_outcomes",
                candidates_created > 0 and delta("candidate_rejections_total") > 0,
                {
                    "rejections": delta("candidate_rejections_total"),
                    "in_memory": metric_sum(last, "candidate_count"),
                    "signals": delta("signals_total"),
                    "security_unavailable": delta(
                        "candidate_evaluation_failures_total",
                        lambda labels: labels.get("stage") == "security",
                    ),
                },
                "> 0 new candidates reaching a terminal outcome",
            )
        )
    return {
        "window_seconds": round(observations[-1].at - observations[0].at, 1),
        "samples": len(observations),
        "entry_block_reasons_seen": reasons,
        "filtered_before_ingest": delta("chain_events_filtered_before_ingest_total"),
        "paper_orders": delta("paper_orders_total"),
        "criteria": criteria,
        "passed": all(item["passed"] for item in criteria),
    }


def _fetch(url: str, timeout: float) -> tuple[int, bytes]:
    request = urllib.request.Request(url, headers={"Accept": "*/*"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
            return int(response.status), response.read()
    except urllib.error.HTTPError as error:
        return int(error.code), error.read()


def observe(base_url: str, timeout: float = 5.0) -> Observation:
    base = base_url.rstrip("/")
    _, metrics_body = _fetch(f"{base}/metrics", timeout)
    ready_status, _ = _fetch(f"{base}/health/ready", timeout)
    status_code, status_body = _fetch(f"{base}/api/v1/status", timeout)
    status: dict[str, Any] = {}
    if status_code == 200:
        loaded = json.loads(status_body)
        status = loaded if isinstance(loaded, dict) else {}
    return Observation(
        at=time.monotonic(),
        metrics=parse_prometheus(metrics_body.decode("utf-8")),
        ready=ready_status == 200,
        status=status,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Soak / monitoring gate for a running bot")
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--duration", type=float, default=1800.0, help="seconds to observe")
    parser.add_argument("--interval", type=float, default=15.0, help="seconds between samples")
    parser.add_argument("--label", default="soak")
    parser.add_argument("--output", help="also write the JSON result here")
    parser.add_argument(
        "--no-candidates",
        action="store_true",
        help="do not require candidate outcomes (short monitoring windows)",
    )
    args = parser.parse_args(argv)
    if args.duration <= 0 or args.interval <= 0:
        parser.error("--duration and --interval must be positive")

    thresholds = Thresholds(require_candidates=not args.no_candidates)
    started = datetime.now(tz=timezone.utc)
    observations: list[Observation] = []
    deadline = time.monotonic() + args.duration
    errors: list[str] = []
    while True:
        try:
            observations.append(observe(args.base_url))
        except (OSError, ValueError) as error:
            errors.append(f"{type(error).__name__}: {error}")
        if time.monotonic() >= deadline:
            break
        time.sleep(min(args.interval, max(0.0, deadline - time.monotonic())))

    if len(observations) < 2:
        result: dict[str, Any] = {"passed": False, "criteria": [], "errors": errors}
    else:
        result = evaluate(observations, thresholds)
        if errors:
            result["errors"] = errors
            result["passed"] = False
    result = {
        "label": args.label,
        "started_at": started.isoformat(),
        "finished_at": datetime.now(tz=timezone.utc).isoformat(),
        **result,
    }
    rendered = json.dumps(result, sort_keys=True)
    print(rendered)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as stream:
            stream.write(rendered + "\n")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
