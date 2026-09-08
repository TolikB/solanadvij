# Testing

Required gates are unit and integration tests, property tests, Ruff, strict mypy, migration smoke,
data reconciliation, raw golden replay, and the no-live source audit.

```bash
python -m pytest --cov=sniper_bot --cov-report=term-missing --cov-report=html
python -m ruff check .
python -m mypy src
python -m alembic upgrade head
python scripts/audit_no_live.py
python scripts/verify_data.py
python scripts/benchmark_internal.py
python scripts/benchmark_postgres_capacity.py
```

Migration smoke testing must cover both a fresh database and upgrade from the previous revision.
Replay tests assert ordered consumption of repeated identical external requests, fixed virtual
time, stable hashes, no network clients, historical reports, and final ledger reconciliation.
Raw replay orders events by the durable `ingest_sequence`. An archive whose events predate that
field replays under `legacy_order`, is reported as such on the run and in `replay_runs.result_json`,
and is not accepted as exact canonical acceptance evidence.
Recovery tests cover failed event reclaim, durable checkpoints, outbox uncertainty, atomic paper
risk limits, and partial-exit accounting. Fault-injection tests fail one ordered-ingestion boundary
at a time - before and after the raw commit, the archive write, rename and segment checkpoint, the
state commit, the notification drain, cancellation, and restart - and assert after each case that
the durable sequence, the archive sequence, and applied state reconcile with no gap and no duplicate
effects.

`benchmark_internal.py` performs an offline warm-up followed by 1,000 measured normalized-event
claims, feature updates/snapshots, and token-fenced durable completions against local SQLite. It
exits nonzero when feature-update p95 reaches 100 ms or normalized event-to-durable-state p95
reaches 250 ms. External API, RPC, and raw transaction decoding latency are intentionally excluded
from this internal-processing NFR.

`benchmark_postgres_capacity.py` is the release capacity gate and needs a real PostgreSQL DSN
(`POSTGRES_DSN`, `MIGRATION_POSTGRES_DSN`, or `CAPACITY_POSTGRES_DSN`). It replays a Pump/PumpSwap
notification mix, encoded from the same vendored Anchor IDLs the decoders use, through the ordered
durable, state, and archive workers. It exits nonzero unless the run sustains at least 800
notifications/s, keeps internal event p95 under 250 ms and feature-update p95 under 100 ms, drops no
events, reconciles every persisted event to applied state, keeps the ingestion backlog from trending
upwards, and drains within 60 seconds. `CAPACITY_WARMUP_SECONDS` and `CAPACITY_MEASURED_SECONDS`
tune only the window, never the targets. It runs in its own `durability` CI job together with the
fresh and previous-revision migrations, the downgrade guard, concurrent canonical locking, and the
archive-rebuild tests; that job publishes its gate receipts to the release evidence bundle.

The generated HTML coverage report is intentionally not committed. CI should retain it as an
artifact together with the exact test output and replay hashes.
