# Operations Runbook

## Before startup

All VM commands must run from `/opt/solanadvij` and must name the Compose
project. Never use global Docker stop, prune, down, or container enumeration
commands on the shared host.

1. Run `cd /opt/solanadvij` and confirm clock synchronization with `timedatectl status`.
2. Populate `/opt/solanadvij/.env`; require mode `600` and keep secrets out of YAML and image layers.
3. Set `APP_REVISION` to the exact 40-character checkout commit.
4. Run `docker compose -p solanadvij --env-file .env config --quiet`.
5. Run `docker compose -p solanadvij --env-file .env up --build -d postgres migrate`.
6. Confirm PostgreSQL is healthy and Alembic is at head before starting `sniper-bot`.
7. Run `docker compose -p solanadvij --env-file .env up --build -d sniper-bot`.
8. Confirm `/health/ready`, `APP_MODE=paper`, `/app/REVISION`, real Helius ingestion, and Jupiter quote-only capability.

## Degraded state

Inspect `/api/v1/status`, `/metrics`, and container logs. New entries remain blocked while the
stream, DB, protocol decoder, quote asset, security APIs, or exit monitor is unhealthy. Do not
override the entry gate. Restore the failed dependency and wait for freshness to recover.

Local operators can pause or resume entry without an HTTP admin endpoint:

```bash
python scripts/control.py pause
python scripts/control.py resume
```

These commands use Linux pidfds and the PID plus process start time stored in `data/sniper.pid`.
They fail closed if the file is stale, malformed, or no longer identifies the running bot.

## Database recovery

Daily dumps and checksums are written to the `backups` volume. Test restore on a separate empty
database:

```bash
/scripts/restore.sh /backups/sniper-TIMESTAMP.dump postgresql://admin:password@restore-db/sniper_restore
python scripts/verify_data.py
```

Never run restore against the active production database. Stop the bot before a planned restore,
verify reconciliation, then restart. Open positions hydrate from PostgreSQL and idempotent order
keys prevent duplicate fills.

## Release acceptance

Follow [MVP acceptance evidence](ACCEPTANCE.md). Freeze the revision, strategy/config, collection
interval, OOS entry boundary, costs, and sample policy before collection; generate `statistics.json`
with `scripts/analyze_statistical_stage.py`, and structurally verify the revision-specific artifact
bundle with `scripts/verify_acceptance_evidence.py` using an independently selected expected commit.
Missing credentials, Docker/VPS observations, Telegram receipts, artifact hashes, or statistical
criteria are a failed gate, not an operator waiver.

Set `APP_REVISION` to the exact deployed commit before starting collection. Publish the frozen
protocol to immutable storage, retain the typed precommit receipt and its independently recorded
SHA-256, and retain every redacted runtime receipt source artifact. With the bot stopped, record
each invoice-backed infrastructure cost with `scripts/record_operational_cost.py`, then restart so
the paper ledger rehydrates; never edit the account aggregate.

## Hard halt

Do not use Telegram `/resume` for an all-time drawdown hard halt. Preserve raw events, quote
journals, DB backup, config hash, and logs. Investigate reconciliation and replay the affected
period before any manual reset or deployment.

## No sell route

The broker retries for 30 seconds and then records a full remaining loss as `UNRECOVERABLE`.
Do not substitute chart or Dexscreener prices. Investigate Jupiter response journal and pool
liquidity after the position is safely closed in the paper ledger.
