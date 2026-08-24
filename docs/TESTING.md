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
```

Migration smoke testing must cover both a fresh database and upgrade from the previous revision.
Replay tests assert ordered consumption of repeated identical external requests, fixed virtual
time, stable hashes, no network clients, historical reports, and final ledger reconciliation.
Recovery tests cover failed event reclaim, durable checkpoints, outbox uncertainty, atomic paper
risk limits, and partial-exit accounting.

`benchmark_internal.py` performs an offline warm-up followed by 1,000 measured normalized-event
claims, feature updates/snapshots, and token-fenced durable completions against local SQLite. It
exits nonzero when feature-update p95 reaches 100 ms or normalized event-to-durable-state p95
reaches 250 ms. External API, RPC, and raw transaction decoding latency are intentionally excluded
from this internal-processing NFR.

The generated HTML coverage report is intentionally not committed. CI should retain it as an
artifact together with the exact test output and replay hashes.
