# PumpSwap IDL provenance

- Source: https://github.com/pump-fun/pump-public-docs/blob/cb188ce08b5069196eef1f3e4a0c43b70099793b/idl/pump_amm.json
- Retrieved: 2026-09-29
- Commit: `cb188ce08b5069196eef1f3e4a0c43b70099793b`
- SHA-256: `2091433899b07d003d98118ae6cd3c628960fd393b40710b6e15bce6d0e7f2d1`
- Adapter version: `pumpswap-idl-cb188ce`
- Minimum accepted layouts are the ones deployed at commit
  `9c82f61cb711b044a17f770ab8ce9f9bdf78f333` (previously vendored); fields
  appended since then are decoded when present.

## Supplemental instruction contract

The pinned event IDL and adapter version remain unchanged. The truncated CPI
scanner also recognizes the published v2 trading family, with exactly one
direct own Buy or Sell CPI required by the runtime proof:

| Instruction | Selector | Event | Argument bytes with selector | Accounts |
|---|---|---|---:|---:|
| `buy_exact_quote_in_v2` | `c2ab1c46684d5b2f` | `BuyEvent` | 24 | 17 |
| `buy_v2` | `b817ee6167c5d33d` | `BuyEvent` | 24 | 17 |
| `sell_v2` | `5df6823ce7e940b2` | `SellEvent` | 24 | 17 |

- Primary reference: `pump-rust-client` 0.2.0, recommended by the official
  [Pump README](https://github.com/pump-fun/pump-public-docs/blob/cb188ce08b5069196eef1f3e4a0c43b70099793b/README.md).
- Published crate: https://static.crates.io/crates/pump-rust-client/pump-rust-client-0.2.0.crate
- Crate SHA-256, checked against the registry checksum:
  `4c940fb1363f719d3310201ec28eb3871ccd5929c5536dc8af10a5f30b0f7c3a`.
- Packaged `idls/pump_amm.json` SHA-256:
  `1cfbf1066283e4f261086ac2395bdfeb585ddcb76f5cb3bfcb3ecdf1de50bf77`.
- Retrieved: 2026-10-04; crate VCS commit
  `46eefb3878cbcc8283cfc5c444299996382d3ba6`.
- All three use identical full 17-account declarations and two `u64` arguments:
  buy-quote `spendable_quote_in`/`min_base_amount_out`, buy
  `base_amount_out`/`max_quote_amount_in`, sell
  `base_amount_in`/`min_quote_amount_out`.
- All five consumed and both reviewed ignored event definitions/discriminators
  exactly match the pinned IDL; these event types contain no referenced nested
  types. The Rust SDK builders use the typed ABI and one shared account builder.

An authentic quarantined transaction matched `buy_exact_quote_in_v2`, its
argument length/account count and a direct complete `BuyEvent` CPI with a
verified Clock and log prefix. `buy_v2` and `sell_v2` are supported from the
same published ABI and synthetic contract tests; native observations of them
are not claimed. The IDL declares the ABI, not unconditional event completeness.
Existing body, Clock, direct-parent, exact-one and log-prefix checks remain the
runtime gate; unknown and unreviewed control operations remain quarantined.

## Reviewed ignored boost control

The pinned IDL already declares `init_boost` (`8ce9215e845ac28f`, no arguments,
14 accounts) and `InitBoostEvent` (`ae7c4af90451f611`). Its complete body is
128 bytes: `timestamp:i64`, `mint`, `bonding_curve`, `pool` pubkeys,
`virtual_quote_reserves:i128` and `real_quote_reserves_after:u64`. These exact
definitions also match the published Rust SDK above. An authentic truncated
CreatePool/InitBoost transaction contained one complete direct CPI per operation.

Recognize the existing boost operation in the truncated completeness contract
without adding it to the consumed event set. Require its full body, Clock,
own parent and exactly one CPI; reject short or trailing bytes. Create identity,
ordinal and selected log-prefix proof remain unchanged. Ignoring this event in
decoder selection does not imply that the instruction has no on-chain effects.
