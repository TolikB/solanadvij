# Risk Management

The initial paper account is 500 USDC. Decimal arithmetic is used throughout.

Position size is bounded by `risk.risk_per_trade_pct` (0.5 percent) of equity divided by
hard-stop plus expected round-trip cost and `risk.adverse_execution_buffer_pct` (1 percent). It
is additionally capped at 20 USDC, `risk.max_position_equity_pct` (4 percent) of equity,
`liquidity.max_position_to_quote_liquidity_pct` (0.25 percent) of the pool's quote side,
remaining daily loss budget, available cash, 50 USDC total exposure, three open positions, and
12 entries per day. A calculated size below 8 USDC is rejected.

Three consecutive losing trades pause entry for 60 minutes; four halt entries for the calendar
day. A trade is one closed position with its net result: a TP1 profit followed by a remainder
closed below entry is one trade, counted by its sum. An entry these account limits refuse is
still measured as a shadow trade outside the account (see `ACCEPTANCE.md`). A 10 USDC daily
loss closes open positions and blocks new entries until the next day. A 10 percent all-time
drawdown is a hard halt and cannot be cleared by Telegram `/resume`.

Exits use executable `TOKEN -> USDC` quotes. TP1 closes 50 percent of the initial position at
30 percent net return. TP2 closes another 25 percent of the initial position at 60 percent.
The final amount uses a 15 percent executable trailing stop. Hard stop is minus 15 percent;
momentum, ten-minute lifetime, 120-second no-new-high, developer sell, liquidity loss, and risk
halts are also enforced.

Paper fills wait `paper.execution_delay_ms` (1.2 s) on entries and exits alike and pay
`paper.adverse_fill_bps` on the quote at that time. An entry whose quote moved more than
`paper.max_entry_slippage_bps` (3 percent) against the decision quote fails like a swap with a
minimum output: no position, only the network fee (at least `paper.min_network_fee_lamports`
per transaction) is charged.

If a sell route is absent, the broker retries once per second for up to 30 seconds. It then closes
the remaining paper position as `UNRECOVERABLE` with zero exit value.

A single mark-to-market quote failure never closes a position. The runtime keeps monitoring and
only enters the unrecoverable path after a continuous configured failure window. Partial exits
remove proportional cost, unrealized PnL, and executable high/low marks so equity cannot double
count the closed fraction. Daily and hard halts are durable and normal `/resume` cannot clear them.
