# Architecture

The application is a modular monolith. A single process owns event ordering, candidate state,
paper execution, reporting, Telegram command intake, and the read-only API.

## Data path

1. `HeliusStreamGateway` opens separate Pump and PumpSwap subscriptions. It asks for enhanced
   `transactionSubscribe` with compact base64 payloads (only `meta.logMessages` is decoded) and
   takes the signature and slot from the notification envelope. If that is unavailable it falls
   back to one standard `logsSubscribe` per program. Neither path calls RPC per notification:
   every Pump and PumpSwap event carries the program's `Clock::unix_timestamp`, which is the
   slot's block time, so the decoders date each transaction from it.
2. Vendored Anchor IDLs decode the events live state consumes: Pump `CreateEvent`,
   `CompleteEvent` and `CompletePumpAmmMigrationEvent`, and PumpSwap pool creation, swaps and
   liquidity changes. Bonding-curve trades and other events are only dated, never decoded.
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
   expiry never waits on provider data.
7. Jupiter V2 `/order` is queried without a taker. Risk sizing and the paper broker then write
   order, fill, position, account, risk audit, and Telegram outbox in one DB transaction.
8. The exit monitor marks every open position with a fresh executable sell quote and applies
   stop, partial take-profit, trailing, momentum, time, developer-dump, liquidity, and risk exits.
9. Source-linked infrastructure charges enter the immutable operational-cost ledger and update
   paper cash/equity in the same transaction; OOS analysis reads only interval-bounded rows.

## Failure boundaries

Entry is blocked when stream lag exceeds three seconds, DB or critical quote/security data is
unavailable, an IDL layout is unknown, SOL/USD is stale, or warm-up is incomplete. Open
positions continue to be monitored when new entries are blocked. From `collection.ends_at`
minus `collection.entry_cutoff_seconds` no new entry is taken and open candidates are rejected,
so every pool and position of a frozen statistical window finishes inside it.

PostgreSQL is authoritative for paper fills. The exit monitor marks executable equity every
second while positions are open and at least once a minute while the account is flat, so the
OOS equity path has bounded gaps. The JSON ledger is a deterministic local mirror
and is hydrated from committed DB rows after restart. Raw archives and external-response
journals preserve repeated responses in call order and make replay independent of the live
network. Stream and event-time momentum checkpoints are restored before entry can be enabled.
