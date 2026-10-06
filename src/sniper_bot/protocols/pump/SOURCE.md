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
