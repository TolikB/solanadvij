# Confirmation Strategy

The strategy never buys at token creation or during the Pump bonding curve. A PumpSwap pool is
observed for at least 45 seconds and may enter only before 180 seconds of age.

Entry requires two score snapshots at least five seconds apart with score at least 80, a 10 to
25 percent pullback from the local executable high, stable quote liquidity, continued buyer
growth, and a reclaim of rolling 30-second VWAP. The final tick repeats all security, route,
holder, liquidity, flow, and risk checks before a paper order is created.

Score caps are Organic Flow 25, Holder Distribution 20, Execution Quality 20, Liquidity Quality
15, Developer History 10, and Price Structure 10. Every score persists its feature snapshot and
per-component explanation.

Dexscreener metadata is enrichment only. It cannot create a candidate, alter on-chain reserves,
or provide an entry/exit price.
