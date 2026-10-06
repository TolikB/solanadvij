# Pump IDL provenance

- Source: https://github.com/pump-fun/pump-public-docs/blob/cb188ce08b5069196eef1f3e4a0c43b70099793b/idl/pump.json
- Retrieved: 2026-09-29
- Commit: `cb188ce08b5069196eef1f3e4a0c43b70099793b`
- SHA-256: `ffe966c42f1af41652ee753fe2f1e3f7cd4077d7e6f49faf3138959c8b56064b`
- Adapter version: `pump-idl-cb188ce`
- Minimum accepted layouts are the ones deployed at commit
  `9c82f61cb711b044a17f770ab8ce9f9bdf78f333` (previously vendored); fields
  appended since then are decoded when present.

## Supplemental instruction contract

The pinned event IDL and adapter version remain unchanged. The truncated CPI
scanner also recognizes the published v3 trading family, each requiring
exactly one direct own `TradeEvent` CPI:

| Instruction | Selector | Argument bytes with selector | Accounts |
|---|---|---:|---:|
| `buy_exact_quote_in_v3` | `e1f7501ed5b38488` | 25 | 17 |
| `buy_v3` | `07051dc4f5176550` | 25 | 17 |
| `sell_v3` | `1c92de7726c469d5` | 24 | 17 |

- Primary reference: `pump-rust-client` 0.2.0, recommended by the official
  [Pump README](https://github.com/pump-fun/pump-public-docs/blob/cb188ce08b5069196eef1f3e4a0c43b70099793b/README.md).
- Published crate: https://static.crates.io/crates/pump-rust-client/pump-rust-client-0.2.0.crate
- Crate SHA-256, checked against the registry checksum:
  `4c940fb1363f719d3310201ec28eb3871ccd5929c5536dc8af10a5f30b0f7c3a`.
- Packaged `idls/pump.json` SHA-256:
  `ed96f86dc3bcd9abe7f19e4bba3eb78f274fc2308d33916650a243924ade2f70`.
- Retrieved: 2026-10-04; crate VCS commit
  `46eefb3878cbcc8283cfc5c444299996382d3ba6`.
- All three have identical full account definitions. Buy-quote arguments:
  `spendable_quote_in: u64`, `min_tokens_out: u64`, `partial_fill: OptionBool`;
  buy arguments: `amount: u64`, `max_sol_cost: u64`,
  `partial_fill: OptionBool`; sell arguments: `amount: u64`,
  `min_sol_output: u64`.
- All four consumed event definitions/discriminators and the referenced
  `Shareholder`/`OptionBool` types exactly match the pinned event IDL.

Authentic quarantined transactions matched the buy-quote and sell selectors,
argument lengths, account counts and direct Trade CPIs. Two RPCs independently
matched the archived buy-quote instruction/CPI data, accounts and stack heights.
`buy_v3` is supported from the same verified published ABI and synthetic
contract tests; a native `buy_v3` observation is not claimed. Event `ix_name` is
not an instruction discriminator. The IDL declares the ABI, not unconditional
event completeness: existing body, Clock, parent, exact-one, log-prefix and
buy-completion checks remain the runtime gate for every operation.

## Reviewed ignored creator-fee collection

The pinned IDL already declares `collect_creator_fee` (`1416567bc61cdb84`,
no arguments, 5 accounts), `collect_creator_fee_v2` (`cf118af204221338`, no
arguments, 10 accounts) and `CollectCreatorFeeEvent` (`7a027f010ebf0caf`). Its
complete body is 80 bytes: `timestamp:i64`, `creator` pubkey, `creator_fee:u64`
and `quote_mint` pubkey. An authentic truncated transaction (slot 453214987)
contained an outer `buy_exact_quote_in_v2` and an outer `collect_creator_fee_v2`
whose only own child was one complete direct `CollectCreatorFeeEvent` CPI after a
token `transferChecked`.

Recognize both collection operations in the truncated completeness contract
without adding the event to the consumed set. Require its full body, Clock, own
parent and exactly one CPI; reject short or trailing bytes. Trade identity,
ordinal and the selected log-prefix proof remain unchanged. `collect_creator_fee`
is supported from the same pinned ABI and synthetic contract tests; a native v1
observation is not claimed. Ignoring this event in decoder selection does not
imply that the instruction has no on-chain effects.

## Reviewed ignored user volume accumulator controls

The pinned IDL declares `init_user_volume_accumulator` (`5e06ca73ff60e8b7`) with
`InitUserVolumeAccumulatorEvent` (`86240d48e86582d8`, 72-byte body) and
`sync_user_volume_accumulator` (`561fc057a3574fee`) with `SyncUserVolumeAccumulatorEvent`
(`c57aa77c74515bff`, 56-byte body), identical to the PumpSwap definitions whose native
`init_user_volume_accumulator` CPI was observed at slot 453875673 (see the PumpSwap
SOURCE.md). Both are recognized beside the reviewed `close_user_volume_accumulator`
under the same full-body, Clock, own-parent and exactly-one CPI contract, outside the
consumed event set. Native Pump observations are not claimed.

## Completed reviewed control contract

Calibration windows kept stopping on one newly observed pinned-IDL control at a time;
the latest was a native `distribute_fee_to_holders` (slot 453881955) with one complete
direct `DistributeFeeToHoldersEvent` CPI. Every remaining pinned instruction that takes
the `event_authority` account (so emits through `emit_cpi!`) and has an event of its
own name is now mapped to that event, and the `_v2` variants without an event of their
own map to their v1 event (`claim_cashback_v2`, `distribute_creator_fees_v2`), as
`collect_creator_fee_v2` was observed to do. None of these events is consumed.

A mapping only names the single event each operation must prove; it never relaxes the
proof. Full body, Clock, own parent and exactly one CPI still apply, so an operation that
emits a different, a second or no event still fails closed.
`tests/test_reviewed_control_contracts.py` runs that contract for every ignored entry.
Still closed on purpose: `create` (a consumed event under a deprecated selector),
`migrate`, and controls without an unambiguous event (`add_quote_mint`,
`remove_quote_mint`, `toggle_*`, `set_reserved_fee_recipients`,
`set_virtual_quote_reserves`, `set_mayhem_virtual_params`, `update_buyback_config`,
`update_holder_reward_config`). Native observations are claimed only where stated.

## Optional ignored-control events and supplemental sweeps

A pre-calibration decode of 14,298 sampled successful mainnet Pump/PumpSwap
transactions (3,744 truncated, from 4,000 finalized blocks over 24 hours) showed
that fee controls skip their event when there is nothing to move: natively,
`claim_cashback_v2` emitted no event in 7 of 7 cases, `claim_cashback`,
`collect_creator_fee(_v2)` and `extend_account` in some. Twelve truncated
transactions with an eventless `claim_cashback_v2` would each have quarantined
the protocol under the exactly-one rule.

Every ignored control (an operation whose event is not consumed) may therefore
prove zero or one CPI of its own expected event; a second, foreign-typed, unknown
or malformed CPI under it still fails closed, and consumed operations still need
exactly one. Consumed events are never optional, which the scanner enforces.

The same sample showed `sweep_creator_fee` (`20f6bf3408c949ba`, 13 accounts) with
an event absent from the pinned IDL. `pump-rust-client` 0.2.0 (crate SHA-256
above) publishes it with `sweep_protocol_fee` (`0830be07b644b7e5`) and
`SweepBondingCurveFeeEvent` (`742b4dbd117a482b`; 145-byte body: `timestamp:i64`,
`mint`, `bonding_curve`, `quote_mint`, `recipient` pubkeys, `amount:u64`,
`bucket:u8`). Eleven native CPIs matched that layout byte for byte. The event is
registered as a supplemental event that cannot shadow a pinned event, type or the
CPI tag, and both sweeps are optional ignored controls.

## Completion, legacy create/migrate and the delivery filter

The pre-calibration probe now decodes every complete delivered transaction a second
time with its logs cut in half, so each operation in the sample also meets the
truncated-log contract. That showed a buy that empties the curve emitting
`TradeEvent` and then `CompleteEvent` as direct CPIs of the same operation. Such a
buy previously always failed closed when truncated ("requires a separate CPI
contract"). A completing buy (`is_buy` with `real_token_reserves == 0`) now must
prove `CompleteEvent` as its next direct CPI; without it the transaction still fails
closed, and a completion after a plain buy is rejected.

A native `migrate` appeared in a truncated transaction. It shares the documentation
and accounts of `migrate_v2`, so it carries the same exactly-one
`CompletePumpAmmMigrationEvent` contract; `create` (classic SPL) carries the
exactly-one `CreateEvent` contract of `create_v2`. Both events are consumed.

The stream subscribes with `logsSubscribe` `mentions`, which matches any account key.
A truncated transaction that merely mentions a subscribed program was routed to both
protocols and quarantined them even when complete jsonParsed metadata proved neither
program ran. With every outer and inner instruction listed, such a transaction now
routes nowhere; with incomplete metadata it still quarantines both.
