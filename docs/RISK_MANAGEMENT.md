# Risk Management

The initial paper account is 500 USDC. Decimal arithmetic is used throughout.

Position size is bounded by 0.5 percent equity risk divided by hard-stop plus expected
round-trip cost and adverse buffer. It is additionally capped at 20 USDC, pool participation,
remaining daily loss budget, available cash, 50 USDC total exposure, three open positions, and
12 entries per day. A calculated size below 8 USDC is rejected.

Three consecutive losing exits pause entry for 60 minutes. Four halt entries for the calendar
day. A 10 USDC daily loss closes open positions and blocks new entries until the next day. A
10 percent all-time drawdown is a hard halt and cannot be cleared by Telegram `/resume`.

Exits use executable `TOKEN -> USDC` quotes. TP1 closes 50 percent of the initial position at
30 percent net return. TP2 closes another 25 percent of the initial position at 60 percent.
The final amount uses a 15 percent executable trailing stop. Hard stop is minus 15 percent;
momentum, ten-minute lifetime, 120-second no-new-high, developer sell, liquidity loss, and risk
halts are also enforced.

If a sell route is absent, the broker retries once per second for up to 30 seconds. It then closes
the remaining paper position as `UNRECOVERABLE` with zero exit value.

A single mark-to-market quote failure never closes a position. The runtime keeps monitoring and
only enters the unrecoverable path after a continuous configured failure window. Partial exits
remove proportional cost, unrealized PnL, and executable high/low marks so equity cannot double
count the closed fraction. Daily and hard halts are durable and normal `/resume` cannot clear them.
