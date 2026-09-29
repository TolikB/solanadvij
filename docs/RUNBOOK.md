# Operations Runbook

## Before startup

All VM commands must run from `/opt/solanadvij` and must name the Compose
project. Never use global Docker stop, prune, down, or container enumeration
commands on the shared host. `scripts/vm_release.sh` wraps every step below with
`docker compose -p solanadvij --env-file .env`; each failed gate prints
`BLOCKED: ...`, leaves the bot stopped, and exits non-zero.

1. Check out the release commit: `git fetch origin && git checkout --detach SHA`.
2. Populate `/opt/solanadvij/.env` with mode `600`, `APP_MODE=paper`, `APP_REVISION=SHA` and
   `CONFIG_PATH=configs/default.yaml`. Secrets stay out of YAML and image layers.
3. `scripts/vm_release.sh preflight SHA` checks the exact checkout, `.env`, NTP sync, green CI
   for SHA (`GITHUB_TOKEN`, or `CI_VERIFIED_SHA=SHA` after checking `quality-gates`), and the
   Compose configuration. It also warns, without changing anything on the shared host, when
   Docker does not start at boot, a reboot is pending, or unattended upgrades may reboot.
   Schedule any reboot before the collection window.
4. `scripts/vm_release.sh build SHA` builds the image, checks `/app/REVISION`, the absence of
   signer-capable modules, the no-live source audit, and `scripts/check_idl_drift.py`: every
   Pump/PumpSwap event the bot decodes must be unchanged upstream or only extended by appended
   fields. A changed discriminator, field, or program address blocks the release.
5. `scripts/vm_release.sh db` starts PostgreSQL, applies migrations, starts daily backups, and
   runs `scripts/preflight_db.py`. Existing rows are kept; the check fails only if terminally
   failed or out-of-order unresolved events would keep the stream disabled at startup.
6. `nohup scripts/vm_release.sh soak 1440 calibration > artifacts/soak.out 2>&1 &` runs the bot
   for 24 hours in `record` mode with Telegram off and `configs/calibration.yaml`: real Helius
   stream, Jupiter quotes only, no fills. `scripts/soak_check.py` then requires zero dropped
   events, bounded ingress, processing and ordered-stage queues, at most one reconnect an hour,
   no open recovery gap, no protocol layout quarantine, accumulating events, working Jupiter
   quotes, stream lag p95 within 3 s, readiness, and candidates reaching outcomes. The JSON
   result is kept in `artifacts/`.
7. `scripts/vm_release.sh funnel 24` applies the decision rule below to the soak and prints the
   decision with its exact config changes.
8. For decision B, commit those changes to `configs/default.yaml`, let CI pass, and repeat steps
   1-5 plus a one-hour `soak 60` for the new SHA. For A, commit the trade-cap change the same way.
9. Freeze the statistical protocol (see below), `scripts/vm_release.sh start`, then
   `scripts/vm_release.sh restart-drill artifacts/acceptance/statistical-protocol.json` at least
   15 minutes before the window: it restarts the bot once and requires the equity path to
   resume within half of the frozen gap.

`start` recreates the bot in `paper` mode with Telegram on, waits for `/health/ready`, prints
mode, strategy version and config hash, and starts the `monitor` service. Telegram then carries
exactly three proactive messages: bot started, bot stopped, and the human daily report at
`00:00 Europe/Kyiv`. The restart drill produces one stop and one start message; an unexpected
"bot started" message later means the bot restarted on its own.

## Threshold decision rule

Fixed before any calibration data exists, so that the sample size cannot be tuned after
looking at results. The calibration soak runs the loosest rung; `calibration_funnel.py`
replays every rung offline for each candidate that passed all entry rules: quote liquidity
must hold the rung's floor at every evaluation from the security check to the entry, and
unique buyers must meet the rung's floor at the entry.

| Rung | `liquidity.min_quote_liquidity_usd` | `flow.min_unique_buyers_60s` |
| --- | --- | --- |
| R0 (specified strategy) | 40000 | 25 |
| R1 | 30000 | 25 |
| R2 | 20000 | 25 |
| R3 (calibration) | 20000 | 20 |

Walking from R0 to R3, the first rung with at least 12.5 entries a day (300 closed trades over
30 days with a 25% margin for loss halts, restarts and risk blocks) is chosen and
`risk.max_trades_per_day` rises to 24, so busy days do not cut the sample; the daily loss
limit, not the trade count, bounds daily risk. R0 is decision A, a looser rung decision B. If no
rung reaches the target, decision C keeps the specified strategy unchanged and a short sample is
itself the result. Safety, execution, holder, developer, exit and sizing rules are never part
of the ladder. A calibration window shorter than 20 hours does not decide.

## Statistical collection window

The window is fixed before collection: start, OOS boundary on day 15, end on day 30. It cannot
be extended after publication.

1. Pick a start at least 30 minutes ahead (room for publication and the restart drill) and
   print its `.env` line:
   `docker compose -p solanadvij --env-file .env run --rm --no-deps -T --entrypoint python
   sniper-bot scripts/freeze_statistical_protocol.py collection-env --collection-start START`.
   Put the printed `COLLECTION=...` line into `.env`. It is part of the config hash.
2. `scripts/vm_release.sh freeze START` writes
   `artifacts/acceptance/statistical-protocol.json` from the release image and prints its
   SHA-256. It refuses a checkout, mode, revision, cost, window, or config file that does not
   match. The equity-mark gap defaults to `exits.maximum_holding_seconds` (600 s): the path may
   go unobserved at most as long as a position may live.
3. Publish that exact file before START (for example as a GitHub issue in the repository),
   then write the receipt with `freeze_statistical_protocol.py receipt --protocol ...
   --published-at ... --reference URL`.
4. `scripts/vm_release.sh start` and the restart drill before START. Confirm the printed config
   hash equals the protocol's `config_hash`.

From `COLLECTION.ends_at` minus 30 minutes the bot takes no new entry and rejects open
candidates; open positions still close, so every pool and position finishes inside the window.
Do not deploy during the window: any code change alters the revision the cohort is pinned to.
Keep every unplanned outage during the OOS half shorter than the frozen gap.
`scripts/vm_release.sh status artifacts/acceptance/statistical-protocol.json` shows readiness,
the latest monitor verdict, and progress toward 3000 pools and 300 closed trades.

## Monitoring

The `monitor` service reruns `scripts/soak_check.py` against the bot every 10 minutes and logs
one JSON verdict per window: `docker compose -p solanadvij logs --tail 5 monitor`. A failed
verdict names the criterion (dropped events, queue growth, reconnect churn, layout quarantine,
stalled stream, Jupiter errors, readiness). `unknown_event_types` and `appended_layout_events`
are informational: the programs gained event types or fields since the vendored IDL; trading
continues, and the IDL is updated between collection windows. Nothing is sent to Telegram.

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
