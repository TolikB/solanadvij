# Architecture

The application is a modular monolith. A single process owns event ordering, candidate state,
paper execution, reporting, Telegram command intake, and the read-only API.

## Data path

1. `HeliusStreamGateway` opens separate Pump and PumpSwap subscriptions. If enhanced
   `transactionSubscribe` is unavailable, it falls back to one standard `logsSubscribe` per
   program and recovers each transaction through read-only JSON-RPC.
2. Every decoded `EventEnvelope` is appended as a zstd NDJSON frame and claimed by canonical
   event ID in PostgreSQL. Claims move through `PROCESSING`, `PROCESSED`, and `FAILED`; startup
   rehydrates processed history and retries unfinished events before opening the stream.
3. Strict vendored Anchor IDLs decode Pump and PumpSwap. Unknown discriminators block that
   protocol and are archived as `UNKNOWN_PROTOCOL_LAYOUT`.
4. Token and pool registries update sequentially. Pump bonding-curve events are observed;
   only PumpSwap AMM pools create trade candidates.
5. Event-time 5/15/30/60 second windows generate anti-lookahead features.
6. Security, holder aggregation, wallet relations, developer history, scoring, and the
   candidate state machine decide whether an entry can become pending.
7. Jupiter V2 `/order` is queried without a taker. Risk sizing and the paper broker then write
   order, fill, position, account, risk audit, and Telegram outbox in one DB transaction.
8. The exit monitor marks every open position with a fresh executable sell quote and applies
   stop, partial take-profit, trailing, momentum, time, developer-dump, liquidity, and risk exits.
9. Source-linked infrastructure charges enter the immutable operational-cost ledger and update
   paper cash/equity in the same transaction; OOS analysis reads only interval-bounded rows.

## Failure boundaries

Entry is blocked when stream lag exceeds three seconds, DB or critical quote/security data is
unavailable, an IDL layout is unknown, SOL/USD is stale, or warm-up is incomplete. Open
positions continue to be monitored when new entries are blocked.

PostgreSQL is authoritative for paper fills. The JSON ledger is a deterministic local mirror
and is hydrated from committed DB rows after restart. Raw archives and external-response
journals preserve repeated responses in call order and make replay independent of the live
network. Stream and event-time momentum checkpoints are restored before entry can be enabled.
