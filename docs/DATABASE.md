# Database

PostgreSQL 16 stores strategy versions, system runs, raw events, tokens, pools, snapshots,
security checks, wallet profiles and relations, candidates, score evaluations, external API
responses, paper accounts, orders, fills, positions, risk events, reports, outbox events, and
replay runs.

`raw_chain_events` and `market_snapshots` are range-partitioned with safe default partitions.
Canonical `event_dedup` processing claims and order/outbox idempotency constraints prevent
duplicate state changes. Failed or interrupted event claims are recovered from raw rows at
startup; candidate runtime state and momentum windows are persisted for deterministic restart.
A partial unique index permits only one open or partial position per mint.

The application role receives only schema usage and DML privileges. Migrations run with the
admin DSN before the application starts. Trade fills atomically update the order, fill, position,
paper account, risk audit, external quote reference, and outbox. Account initialization, fills,
exits, and executable marks also append immutable `paper_equity_marks`; calendar-day reports use
these snapshots for boundary equity, unrealized PnL, and intraday drawdown.

`operational_costs` is an immutable, source-hash-idempotent period ledger. Recording a positive
cost atomically updates the paper-account aggregate, cash, equity, drawdown, and equity marks.
Statistical acceptance sums only cost rows inside the frozen OOS interval; the mutable account
aggregate is never accepted as period evidence.
Database triggers reject row updates/deletes, and the application role has no UPDATE/DELETE grant
on this table. Recording is blocked while a `system_runs` row is active so the in-memory paper
ledger cannot diverge from PostgreSQL; stop the bot, record the cost, then restart and rehydrate.

Outbox rows are leased as `SENDING`. A process crash after an external send has an unknowable
delivery outcome, so an expired lease becomes `UNCERTAIN` and is not automatically resent.
Known transient failures retry with bounded backoff; permanent failures and exhausted retries
become `DEAD` for operator inspection.
`scripts/reconcile_outbox.py` is the only supported manual transition from `UNCERTAIN`; it requires
the operator to choose `delivered`, `retry`, or `dead` after checking Telegram.

Raw events are retained at least 90 days. Snapshots retain one-second density for the first ten
minutes, five-second density to one hour, and one-minute density to 24 hours. Maintenance runs
outside the event hot path.
