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

## Stale stream checkpoint

When the durable stream checkpoint is older than the bounded recovery window, the gateway keeps
resuming from that checkpoint, records an `OPEN` row in `stream_recovery_gaps`, blocks entries with
`stream_recovery_gap`, and serves `/health/ready` as `503`. It never starts a tradable baseline over
an archive hole, so a bot that was down longer than the recovery window will not trade until the
paginated backfill completes.

Gap recovery is bounded by `MAX_GAP_RECOVERY_AGE` (60 seconds) and buffers the live socket in
memory for at most `GAP_RECOVERY_TIMEOUT_SECONDS` (15 seconds). At mainnet Pump/PumpSwap volume that
buffer fills long before a multi-minute gap can be paginated, so any outage longer than the recovery
window - including an ordinary restart - leaves a checkpoint that cannot be backfilled.

Accepting the resulting archive hole is an explicit operator decision, never an automatic one.
`configs/` is baked into the image, so the switch is an environment override and needs no rebuild:

1. Confirm from `/api/v1/status` and `stream_recovery_gaps` that the backfill is genuinely
   unreachable, and record why.
2. Set `CHAIN={"allow_stale_checkpoint_reset":true}` in `/opt/solanadvij/.env`, keeping mode `600`.
3. Restart only this project's bot:
   `docker compose -p solanadvij --env-file .env up -d sniper-bot`, then wait for the `ACCEPTED`
   `stream_recovery_gaps` rows that permanently record the hole and for `/health/ready` to serve
   `200`.
4. Clear `CHAIN=` again once the bot is ready, so the next stale checkpoint fails closed. Note that
   the next restart will hit the same decision, because it too exceeds the recovery window.

`CHAIN` replaces the whole `chain` block, so any value that must differ from the model defaults has
to be repeated in that JSON.

`ACCEPTED` gap rows are terminal. A later successful recovery never resolves them, and the affected
range must be excluded from canonical replay acceptance evidence.

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
