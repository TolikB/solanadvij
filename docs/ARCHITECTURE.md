# Architecture

The application is a modular monolith. A single process owns event ordering, candidate state,
paper execution, reporting, Telegram command intake, and the read-only API.

## Data path

1. `HeliusStreamGateway` opens separate Pump and PumpSwap subscriptions. It asks for enhanced
   `transactionSubscribe` with compact base64 payloads (only `meta.logMessages` is decoded) and
   takes the signature and slot from the notification envelope. If that is unavailable it falls
   back to one standard `logsSubscribe` per program. Complete logs need no per-notification RPC:
   every Pump and PumpSwap event carries the program's `Clock::unix_timestamp`, which is the
   slot's block time, so the decoders date each transaction from it. An exact runtime
   `Log truncated` marker triggers an ordered confirmed transaction fetch, with signature,
   slot, success and prefix checks. RPC and enhanced subscriptions accept legacy/v0/v1;
   replay also reads journals recorded with the earlier version-0 request hash without network.
   Truncated PumpSwap and reviewed Pump operations require one matching event CPI per
   own operation, verified parent/stack/cardinality, complete IDL bodies and consistent Clock
   timestamps. Available consumed logs must exactly match the ordered CPI prefix, including
   repeated events; unknown operations, completion branches and incomplete metadata quarantine.
   Before the fixed event-identity cutover, missing consumed logs still fail closed because a
   legacy ID requires the original log index. Above the cutover, a missing suffix is decoded
   from genuine CPI bodies and carries the outer/inner CPI coordinates as explicit provenance.
   Post-marker log boundaries are never guessed. `create_v2`/`CreateEvent` and
   `extend_account`/`ExtendAccountEvent` are reviewed Pump contracts; the ignored Extend event
   is validated but never applied to state. Truncated routing includes programs found only
   in full instructions.
   Failed recovery retains original evidence.
2. Vendored Anchor IDLs decode the events live state consumes: Pump `CreateEvent`,
   `CompleteEvent` and `CompletePumpAmmMigrationEvent`, and PumpSwap pool creation, swaps and
   liquidity changes. Bonding-curve trades never enter live state; v2 validates them before
   the state subset is selected so occurrence ordinals cannot depend on caller filters.
   Programs extend events by appending fields, so a consumed event decodes from the layout
   deployed when the IDL was first vendored through every field it carries; bytes beyond the
   IDL are counted, not rejected. A consumed event that still does not fit, or whose Clock
   timestamp is implausible or disagrees with the block, blocks entries on that protocol and is
   archived as `UNKNOWN_PROTOCOL_LAYOUT`; ingestion keeps running. Event types the IDL does not
   know are counted and archived once each: a discriminator is the hash of the event name in
   its own log line, so a new type cannot change how consumed events decode, and blocking on it
   would halt a collection window at every routine program upgrade.
3. Before durable ingest, pool activity is admitted only for pools a live candidate tracks:
   from the creation event to the end of the entry window plus a margin, and for as long as a
   position is open. Everything else is what live state would discard, so it never costs
   durable capacity. Admitted events are appended as zstd NDJSON frames and claimed by
   canonical event ID in PostgreSQL. Claims move through `PROCESSING`, `PROCESSED`, and
   `FAILED`; startup rehydrates processed history and retries unfinished events before opening
   the stream.
4. Token and pool registries update sequentially. Pump bonding-curve events are observed;
   only PumpSwap AMM pools create trade candidates. Every discovered PumpSwap pool gets a
   candidate so it reaches a terminal outcome: pools without exactly one supported quote mint,
   pools first seen while the stream is not tradable, and pools created after the frozen
   collection window closes are rejected at their creation block.
5. Event-time 5/15/30/60 second windows generate anti-lookahead features.
6. Security, holder aggregation, wallet relations, developer history, scoring, and the
   candidate state machine decide whether an entry can become pending. Market-only hard
   filters (quote liquidity, external sellers, pool age, liquidity trend, price extension) run
   first and reject without spending RPC or Jupiter quota. Each candidate is evaluated in
   isolation: one token's unavailable holder index or quote leaves that candidate waiting, and
   expiry never waits on provider data. Provider reads of all candidates in a pass run
   concurrently (decisions stay sequential); mint and holder data refresh every
   `execution.holder_refresh_seconds`, round trips every `execution.quote_refresh_seconds` and
   always again for the entry decision, which alone must pass `max_quote_age_ms`. Two scores
   above the entry bar confirm when they are at least one window apart with no lower score in
   between and no silence longer than `candidate.score_confirmation_max_gap_seconds`.
7. Jupiter V2 `/order` is queried without a taker, through one priority rate limiter: exits
   and marks first, then entries, the SOL/USD reference, and candidate security round trips.
   After `paper.execution_delay_ms` the broker re-quotes; an entry that moved more than
   `paper.max_entry_slippage_bps` against the decision quote fails like a swap with a minimum
   output (rejected order, network fee paid). Otherwise order, fill (with the pool reserves
   before and after the delay and the quote as evidence), position, account, risk audit, and
   Telegram outbox are written in one DB transaction.
8. The exit monitor marks every open position (a Jupiter sell quote, or the tracked pool
   reserves with `exits.mark_source: reserves`; the two are always compared and logged) and
   applies stop, partial take-profit, trailing, momentum, time, developer-dump, liquidity, and
   risk exits. Exits wait the same execution delay before they are priced.
9. An entry refused only by the account's risk state (loss-streak pause or halt, daily loss or
   trade cap, full slots, exposure, cash, drawdown, exhausted daily budget) is taken in the
   shadow book instead: same size rule on a fresh day, same fill model and exits, own tables
   and own exit loop, never touching the account. Account plus shadow trades are the signal
   sample the statistical protocol measures; the account alone is the portfolio result.
10. Source-linked infrastructure charges enter the immutable operational-cost ledger and update
    paper cash/equity in the same transaction; OOS analysis reads only interval-bounded rows.

## Bounded state

A collection window runs for a month on one VPS. Tokens, pools and feature windows leave
memory once no candidate, position or shadow trade needs them (a token that migrates later is
reloaded from PostgreSQL with its creator). Wallet history is indexed by creator, evicted after
a day of inactivity and reloaded on the creator's next launch or candidate; wallet rows merge
instead of overwriting, so a process that holds only part of a history never clobbers it.
Relations are kept for a week, seen-event ids and provider caches are capped. Tokens and pools
are written when they change, candidate rows on lifecycle changes, open-position equity marks
every 5 s, provider audit rows are kept 3 days, and restart restores only what it needs.

## Failure boundaries

Entry is blocked when stream lag exceeds three seconds, DB or critical quote/security data is
unavailable, an IDL layout is unknown, SOL/USD is stale, or warm-up is incomplete. Open
positions continue to be monitored when new entries are blocked. From `collection.ends_at`
minus `collection.entry_cutoff_seconds` no new entry is taken and open candidates are rejected,
so every pool and position of a frozen statistical window finishes inside it.

PostgreSQL is authoritative for paper fills. The exit monitor marks executable equity every
5 seconds while positions are open (fills add exact marks) and at least once a minute while the
account is flat, so the OOS equity path has bounded gaps. The JSON ledger is a deterministic local mirror
and is hydrated from committed DB rows after restart. Raw archives and external-response
journals preserve repeated responses in call order and make replay independent of the live
network. Stream and event-time momentum checkpoints are restored before entry can be enabled.


## Event identity cutover and rollback

The immutable boundary is confirmed Solana slot **453053813**, measured at
2026-10-03T21:25:36.409466Z with the runtime stopped. A decoded transaction uses v2 only
above that boundary and with a base58 signature encoding exactly 64 bytes. Historical
synthetic acceptance signatures remain v1 even when their test slots exceed the boundary.
Before the first v2 deployment, verify zero existing genuine chain signatures above this
boundary, zero identity descriptors, zero paper orders and zero open positions. Preserve all
old rows, raw archives, checkpoints and rollback backups. The existing stale-checkpoint path
records the accepted archive gap and requires a fresh 60-second baseline warmup before ready.

V2 is SHA256 of `v2:{protocol}:{signature}:{event_type}:{ordinal}`, where ordinal is zero-based
within that event type in execution order, before caller/admission filters. All live and RPC
recovery paths use the decoder policy. The reserved payload `_event_identity` has exactly
integer `version: 2`, nonnegative integer `ordinal`, and `origin: log|cpi`; DB `payload_json`
and raw envelopes preserve it without a schema migration. Log provenance keeps the genuine
log index and inner index -1; CPI provenance has genuine outer and inner instruction indices.
The ID excludes those coordinates and source, so full logs, CPI recovery and a restart share
one durable claim. No descriptor always means v1, regardless of slot; old envelope bytes and
archive/input hashes remain unchanged. Never recalculate or advance the cutover after v2.

Once v2 rows exist, the rollback reader floor is this first v2 release image, which reads both
versions. Older images such as de354 cannot hydrate v2 and must not be started against the new
DB. Preserve the pre-cutover snapshot and old images as evidence; preserve a tagged compatible
v2 image as the operational rollback floor. No ledger rewrite or destructive restore is part
of the cutover. Calibration and collection config, thresholds and fill model are unchanged.
