# Operations Runbook

## Before startup

All VM commands must run from `/opt/solanadvij` and must name the Compose
project. Never use global Docker stop, prune, down, or container enumeration
commands on the shared host. `scripts/vm_release.sh` wraps every step below with
`docker compose -p solanadvij --env-file .env`; each failed gate prints
`BLOCKED: ...`, leaves the bot stopped, and exits non-zero.

1. Check out the release commit: `git fetch origin && git checkout --detach SHA`.
2. Populate `/opt/solanadvij/.env` with mode `600`, `APP_MODE=paper` and `APP_REVISION=SHA`.
   Secrets stay out of YAML and image layers.
3. `scripts/vm_release.sh preflight SHA` checks the exact checkout, `.env`, NTP sync, green CI
   for SHA (`GITHUB_TOKEN`, or `CI_VERIFIED_SHA=SHA` after checking `quality-gates`), and the
   Compose configuration.
4. `scripts/vm_release.sh build SHA` builds the image, checks `/app/REVISION`, the absence of
   signer-capable modules, and the no-live source audit.
5. `scripts/vm_release.sh db` starts PostgreSQL, applies migrations, starts daily backups, and
   runs `scripts/preflight_db.py`. Existing rows are kept; the check fails only if terminally
   failed or out-of-order unresolved events would keep the stream disabled at startup.
6. `scripts/vm_release.sh soak 30` runs the bot in `record` mode with Telegram off: real Helius
   stream, Jupiter quotes only, no fills. `scripts/soak_check.py` then requires zero dropped
   events, bounded ingress, processing and ordered-stage queues, no reconnect churn, no open
   recovery gap, no protocol layout quarantine, accumulating events, working Jupiter quotes,
   stream lag p95 within 3 s, readiness, and candidates reaching outcomes. The JSON result is
   kept in `artifacts/`.
7. Freeze the statistical protocol before collection starts (see below), then
   `scripts/vm_release.sh start` recreates the bot in `paper` mode with Telegram on, waits for
   `/health/ready`, prints mode, strategy version and config hash, and starts the `monitor`
   service.

Telegram then carries exactly three proactive messages: bot started, bot stopped, and the human
daily report at `00:00 Europe/Kyiv`.

## Statistical collection window

The window is fixed before collection: start, OOS boundary on day 15, end on day 30. It cannot
be extended after publication.

1. Pick a start at least 10 minutes ahead and print its `.env` line:
   `docker compose -p solanadvij --env-file .env run --rm --no-deps -T --entrypoint python
   sniper-bot scripts/freeze_statistical_protocol.py collection-env --collection-start START`.
   Put the printed `COLLECTION=...` line into `.env`. It is part of the config hash.
2. `scripts/vm_release.sh freeze START` writes
   `artifacts/acceptance/statistical-protocol.json` from the release image and prints its
   SHA-256. It refuses a checkout, mode, revision, cost, or window that does not match.
3. Publish that exact file before START (for example as a GitHub issue in the repository),
   then write the receipt with `freeze_statistical_protocol.py receipt --protocol ...
   --published-at ... --reference URL`.
4. `scripts/vm_release.sh start` before START. Confirm the printed config hash equals the
   protocol's `config_hash`.

From `COLLECTION.ends_at` minus 30 minutes the bot takes no new entry and rejects open
candidates; open positions still close, so every pool and position finishes inside the window.
Keep every restart during the OOS half under five minutes: the equity path allows at most 300 s
between marks. Record the invoice-backed VPS cost with `scripts/record_operational_cost.py`
after the window ends, with `--incurred-at` inside the OOS half, so the bot stops only once the
path is complete. `scripts/vm_release.sh status artifacts/acceptance/statistical-protocol.json`
shows readiness, the latest monitor verdict, and progress toward 3000 pools and 300 closed
trades.

## Monitoring

The `monitor` service reruns `scripts/soak_check.py` against the bot every 10 minutes and logs
one JSON verdict per window: `docker compose -p solanadvij logs --tail 5 monitor`. A failed
verdict names the criterion (dropped events, queue growth, reconnect churn, layout quarantine,
stalled stream, Jupiter errors, readiness). Nothing is sent to Telegram.

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

## Unrecoverable stream checkpoint

Gap recovery is bounded by `MAX_GAP_RECOVERY_AGE` (60 seconds) and buffers the live socket in memory
for at most `GAP_RECOVERY_TIMEOUT_SECONDS` (15 seconds). At mainnet Pump/PumpSwap volume that buffer
fills long before even that window can be paginated, so any outage past it - an ordinary restart
included - leaves a checkpoint that cannot be backfilled.

By default the bot then records the hole permanently as an `ACCEPTED` row in `stream_recovery_gaps`
and resumes on a fresh non-tradable baseline. It never trades over the hole: entries stay blocked
through the baseline warmup, and the recorded range must be excluded from canonical replay
acceptance evidence. `ACCEPTED` rows are terminal and a later successful recovery never closes them.

Retrying such a backfill is futile and only consumes the provider quota, so the bot never does it.

To require a human decision instead, set `CHAIN={"halt_on_unrecoverable_gap":true}` in
`/opt/solanadvij/.env`, keeping mode `600`. The bot then records `checkpoint_unrecoverable`, blocks
entries, serves `/health/ready` as `503`, and opens no socket and issues no RPC until the setting is
cleared. `CHAIN` replaces the whole `chain` block, so any value that must differ from the model
defaults has to be repeated in that JSON.

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
