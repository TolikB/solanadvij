# Solana Confirmation Sniper Bot

Record, replay, and paper-trading implementation of a confirmation sniper for Solana Mainnet.
Release 1 observes Pump, follows migration into PumpSwap, evaluates executable Jupiter V2
routes, and writes virtual fills. It has no signer, private-key loader, transaction builder,
or transaction-submission method.

## Runtime modes

| Mode | Behavior |
| --- | --- |
| `record` | Records and normalizes events, snapshots, security checks, features, and scores. No fills. |
| `paper` | Adds risk-sized virtual entries and executable-quote exits with a 500 USDC ledger. |
| `live` | Fails startup with `LIVE_TRADING_NOT_IMPLEMENTED`. |

## Local setup

Python 3.12 or newer and PostgreSQL 16 are required.

```powershell
python -m pip install -e ".[dev]"
Copy-Item .env.example .env
python -m alembic upgrade head
python -m sniper_bot.main --config configs/default.yaml
```

The API binds to `127.0.0.1` by default. Required secrets and DSNs are read from environment
variables. YAML contains strategy parameters, not secrets.

## Docker Compose

```bash
cp .env.example .env
# Replace every replace_me value, both database passwords, and APP_REVISION with the deployed 40-character commit SHA.
docker compose up --build -d
docker compose ps
curl -fsS http://127.0.0.1:8080/health/ready
```

Compose runs migrations before the bot, uses a non-root read-only application container,
publishes the API only on localhost, persists PostgreSQL and raw archives, rotates container
logs, and creates daily checksummed PostgreSQL backups.

## Raw replay

Record external responses during collection:

```text
JUPITER_QUOTE_JOURNAL_RECORD=true
JUPITER_QUOTE_JOURNAL_PATH=./data/jupiter_quotes.ndjson
```

Replay the raw archive with no outbound Solana, Jupiter, Dexscreener, or Telegram calls:

```powershell
$env:JUPITER_REPLAY_MODE = "true"
python -m sniper_bot.main --config configs/default.yaml --replay-data data/raw --replay-speed max
```

Supported speeds are `1x`, `5x`, `10x`, and `maximum`; legacy aliases `realtime` and `max` remain
accepted. Each run records input/output hashes and
the final reconciliation in `data/replay_runs.json` and PostgreSQL `replay_runs`.

## Read-only API

| Endpoint | Purpose |
| --- | --- |
| `GET /health/live` | Process liveness. |
| `GET /health/ready` | Fail-closed readiness. |
| `GET /metrics` | Prometheus exposition. |
| `GET /api/v1/status` | Runtime, stream, DB, and entry-gate state. |
| `GET /api/v1/account` | Paper account and reconciliation fields. |
| `GET /api/v1/positions/open` | Open paper positions. |
| `GET /api/v1/positions/closed` | Closed paper positions. |
| `GET /api/v1/candidates` | Candidate state machines. |
| `GET /api/v1/rejections` | Rejected candidates and reasons. |
| `GET /api/v1/reports/today` | Current local-calendar report. |
| `GET /api/v1/reports/all-time` | All-time report. |
| `GET /api/v1/reports/day/{date}` | IANA-timezone daily report. |

Legacy `/health`, `/state`, `/today`, `/all`, and `/day` aliases remain available.

## Quality and safety checks

```powershell
python -m pytest
python -m pytest --cov=sniper_bot --cov-report=term-missing --cov-report=html
python -m ruff check .
python -m mypy src
python scripts/audit_no_live.py
python scripts/verify_data.py
```

See [architecture](docs/ARCHITECTURE.md), [strategy](docs/STRATEGY.md),
[risk management](docs/RISK_MANAGEMENT.md), [database](docs/DATABASE.md),
[Telegram](docs/TELEGRAM.md), [runbook](docs/RUNBOOK.md), and
[no-live proof](docs/NO_LIVE_EXECUTION.md). External and statistical Definition-of-Done evidence
is specified in [MVP acceptance](docs/ACCEPTANCE.md).
