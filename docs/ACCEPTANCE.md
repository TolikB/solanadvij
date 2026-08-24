# MVP Acceptance Evidence

The Definition of Done is not satisfied by unit tests or a hand-written manifest. The offline
verifier checks structure, revision, freshness, typed contents, independently supplied artifact
hashes, full statistical criteria, typed per-claim provenance receipts, and obvious secret/private-identifier
leakage. Secret scanning is defense-in-depth, not a complete privacy guarantee. The trusted hashes
and protocol publication timestamp must come from independent CI/object-storage/operator records;
copying them from the submitted manifest invalidates the release procedure.

## Frozen statistical protocol

Set `APP_REVISION` to the exact 40-character deployed commit so the persisted strategy cohort can
be matched to the independently selected release revision.

Create and retain a `StatisticalProtocol` JSON before collection. Start from
`docs/examples/statistical-protocol.json`. The protocol fixes the Git revision, strategy ID, config
hash, collection interval, entry-time OOS boundary, operational cost, and sample requirements. It
must be externally published before collection starts and its publication receipt retained. Project
policy strengthens the unspecified "sufficient"
sample wording to at least 300 distinct rejected PumpSwap pools and at least 100 OOS trades.

Run the evaluator against authoritative PostgreSQL data:

```bash
python scripts/analyze_statistical_stage.py \
  --protocol artifacts/acceptance/statistical-protocol.json \
  --output artifacts/acceptance/statistics.json
```

The cohort is restricted to one strategy/config/revision. OOS assignment uses position entry time,
not close time. Distinct PumpSwap `pool_created` raw events are compared with materialized pool rows,
and rejected-launch counts use distinct rejected candidate pools in the same interval and strategy.
All cohort positions are loaded: any position not closed by the cutoff fails the gate. Outer joins
prevent missing metadata from disappearing, and only an explicit known funding cluster counts as
completed clustering; inferred singleton clusters do not pass.
Every discovered raw PumpSwap pool must also be materialized and reach a terminal candidate outcome
(`REJECTED` by the cutoff or a position closed by the cutoff). Rejections recorded after the cutoff
do not enter the negative cohort.

Economic trade PnL includes simulated costs already committed in `realized_pnl` and allocates the
greater of the frozen cost floor and the period-bounded `operational_costs` ledger across OOS trades;
the ledger must cover the floor. Every ledger source hash must match a redacted operational-cost
receipt artifact referenced by runtime evidence, and the verifier compares its account, category,
amount, and incurred timestamp with the statistical report. Max drawdown comes from a gap-bounded durable executable-equity
curve that reaches both OOS boundaries, including unrealized movement and overlapping positions. The gate also requires net
PnL to remain positive without the best trade day and without the best developer cluster.

## Evidence bundle

The manifest in `docs/examples/acceptance-evidence.json` references five distinct JSON artifacts:

1. CI evidence generated only after all workflow gates pass.
2. Typed runtime evidence covering every section 43/46 criterion, Telegram, deployment, and NFRs.
3. The independently retained immutable publication receipt for the frozen protocol.
4. The frozen statistical protocol.
5. The generated statistical report.

Use lowercase or uppercase SHA-256 values; the verifier normalizes them. It opens each artifact with
identity and size fencing, limits it to 10 MiB, hashes and parses the same bytes, rejects path traversal/symlink escape, and
rejects common credentials, DSNs, bot tokens, and raw chat/user identifiers. Artifacts should contain
aggregates and redacted message receipt IDs only, never raw logs with credentials or private chats.

```bash
python scripts/verify_acceptance_evidence.py \
  artifacts/acceptance/evidence.json \
  --artifact-root artifacts/acceptance \
  --expected-revision COMMIT_SHA \
  --max-evidence-age-hours 168 \
  --expected-ci-sha256 TRUSTED_CI_SHA256 \
  --expected-runtime-sha256 TRUSTED_RUNTIME_SHA256 \
  --expected-precommit-receipt-sha256 TRUSTED_PRECOMMIT_RECEIPT_SHA256 \
  --expected-protocol-sha256 TRUSTED_PROTOCOL_SHA256 \
  --expected-report-sha256 TRUSTED_REPORT_SHA256 \
  --trusted-protocol-published-at 2026-08-31T00:00:00Z
```

`--expected-revision` must come from the independently selected release/deployment, not from the
manifest. Runtime freshness is based on observation end, not merely JSON generation. Collection and
report generation before the declared cutoff fail. A successful command means the bundle matches
the independent trust anchors and is internally consistent; the trust anchors themselves remain an
operator/auditor responsibility.

## Runtime evidence collection

Retain redacted machine-readable aggregates proving both modes, Pump/PumpSwap ingestion, all hard
filters, holder aggregation, on-chain liquidity, executable Jupiter buy/sell and every exit quote,
round-trip costs and risk sizing for every closed-position denominator, risk limits, reconciliation,
reports, deterministic replay, restart idempotency, and complete audit
trails. Telegram evidence must cover every mandatory event type and outage behavior. Ubuntu evidence
must cover non-root/localhost/minimal-role security, NTP, automatic migrations, reboot, restart,
daily backup, and a separate-database restore followed by `scripts/verify_data.py`.

Each runtime claim receipt names the required source type and references a redacted source artifact
inside the evidence root by path and SHA-256. The verifier loads every referenced source and applies
the same size, identity, hash, and secret-scanning controls. Each receipt must itself be fresh under
the selected evidence age.

Create a redacted receipt JSON using `docs/examples/operational-cost-receipt.json`. Stop the bot,
record the exact receipt file in the period ledger, retain that same file in the runtime evidence
bundle, then restart so the paper ledger rehydrates. Never manually update the paper account aggregate:

```bash
python scripts/record_operational_cost.py \
  --account-id paper-main \
  --category vps \
  --amount-usd 12.50 \
  --incurred-at 2026-10-01T00:00:00Z \
  --source-receipt artifacts/acceptance/receipts/vps-2026-10.json
```

No arbitrary 72-hour threshold is used: the specification does not define one.
