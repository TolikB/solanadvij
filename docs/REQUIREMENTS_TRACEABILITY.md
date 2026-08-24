# Requirements Traceability

This matrix separates implemented behavior from environment-dependent acceptance evidence.

| Specification area | Implementation evidence | Automated evidence |
| --- | --- | --- |
| Configuration and modes | `config.py`, YAML settings, secret masking, stable config hash, fail-fast `live` mode | Config, API, replay, and no-live tests |
| Helius stream and recovery | `stream.py`, raw recorder, gap recovery, checkpoints, reconnect gates | Stream, event, raw replay, and database recovery tests |
| Pump and PumpSwap decoding | Official vendored IDLs and strict Anchor decoders | Decoder fixtures and unknown-layout tests |
| Token and pool state | `registry.py`, SOL/USD freshness, effective reserves, quality flags | Registry/security tests |
| Security and holders | `solana_rpc.py`, `security.py`, paginated holder universe, index freshness, owner aggregation, PDA/burn exclusions | Security and RPC tests |
| Wallet analysis and clustering | `wallet_analysis.py`, `clustering.py` | Clustering/feature/scoring tests |
| Event-time features | `features.py` with 5/15/30/60 second windows and deduplication | Formula, late-event, duplicate, and property tests |
| Scoring and candidate state | `scoring.py`, `candidates.py`, durable candidate runtime state | Scoring/state transition and replay tests |
| Jupiter executable quotes | Jupiter V2 order endpoint, timeout, retry, rate limit, no-route and ordered journal | Jupiter and offline replay tests |
| Paper broker and ledger | Delayed adverse fills, partial exits, deterministic IDs, atomic DB commits, restart hydration | Broker, ledger, persistent-paper, reconciliation tests |
| Risk management | Sizing and DB-enforced position, exposure, count, daily and drawdown limits | Sizing, risk, property, persistent-paper tests |
| Exit engine | Executable marks, stop/TP/trailing/time/momentum/emergency exits, failure window | Exit engine and runtime exit-flow tests |
| Telegram | Allowlisted polling, commands, transactional outbox, bounded retry, operator-reconciled uncertain/dead states | Telegram, outbox, and database tests |
| Reports | Stored historical daily snapshots and all-time reports, scheduled idempotency, operational costs | Runtime report, database, API, and replay tests; examples under `docs/examples` |
| Record and replay | Raw reader, virtual clock, ordered external journals, seed, golden hashes | Offline, deterministic, golden, and no-network tests |
| API, metrics, and logging | Localhost read-only API, Prometheus metrics, JSON redaction | API/config/logging tests and no-live audit |
| Performance NFR | Internal feature and event processing benchmark with warm-up | CI and local p95 benchmark gate |
| PostgreSQL and migrations | SQLAlchemy schema, Alembic, period-bounded operational-cost ledger, app-role grants, backup/restore/integrity scripts | Fresh and previous-revision migration smoke; integrity and acceptance-loader queries |
| Deployment package | Non-root image, Compose health checks, restart policy, volumes, rotation, backup | Compose config plus image build and startup/liveness/non-root smoke in CI |

## Current acceptance evidence

Local automated gates include the full pytest suite with coverage XML, Ruff, strict mypy,
fresh-schema migration, previous-revision migration, integrity queries, golden replay, and the
no-live source audit. GitHub Actions repeats these gates against PostgreSQL 16 and validates the
Compose configuration.

## External operating gates

The following evidence cannot be manufactured by unit tests and must be collected in the target
runtime environment:

1. Docker image startup, health, restart, and scheduled backup/restore on an Ubuntu VPS.
2. Live read-only Helius, Solana RPC, Jupiter, Dexscreener, and Telegram contract probes using
   operator-provided credentials.
3. NTP state and post-reboot service recovery on the target host.
4. The statistical pre-live dataset of at least 3,000 pools and 300 closed paper trades, including
   the specified out-of-sample profitability and concentration checks.

These gates never enable live trading. Release 1 contains no signer, private-key loader,
transaction builder, or submission surface.

The external evidence schema, statistical evaluator, exact commands, and fail-closed verifier are
documented in [MVP acceptance evidence](ACCEPTANCE.md). The specification does not define a
72-hour duration, so no such arbitrary threshold is used as a substitute for its concrete reboot,
restart, backup, restore, notification, reconciliation, and latency requirements.
