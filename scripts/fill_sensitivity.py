"""Re-price closed paper trades under other execution assumptions.

The collection runs one frozen fill model (execution delay, adverse fill
bps, fee floor, position size). This report shows how much of the measured
result depends on it, from what every fill recorded, without re-running the
window:

* ``adverse_bps``: the flat adverse fill applied to every entry and exit.
  Tokens bought scale by k = (1 - b) / (1 - b0) and every exit sells k times
  the tokens at k times the price, so exit proceeds scale by k squared.
* ``no_delay``: fills at the pool price seen when the decision was made
  instead of after the execution delay (entry and exit marginal prices from
  the recorded pool reserves).
* ``size_multiple``: the same trades at m times the size; each side pays the
  extra constant-product price impact (m - 1) * notional / quote reserve.
* ``extra_fee_usd``: an added network fee per transaction (priority fees).

The approximations are first order and ignore how a different fill would
have changed later exit decisions; they bound sensitivity, they are not a
second backtest.

    python scripts/fill_sensitivity.py --since 2026-10-01T00:00:00Z
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from statistics import median
from typing import Any

from sqlalchemy import select

from sniper_bot.database import Database
from sniper_bot.db_models import PaperFillRow, PaperOrderRow, PaperPositionRow

ADVERSE_BPS = (0, 50, 100, 200)
SIZE_MULTIPLES = (Decimal("2"), Decimal("5"), Decimal("10"))
EXTRA_FEES_USD = (Decimal("0.05"), Decimal("0.10"), Decimal("0.30"))
ONE = Decimal("1")


@dataclass
class ExitFill:
    gross_usd: Decimal
    network_fee_usd: Decimal
    evidence: dict[str, Any] | None


@dataclass
class ClosedTrade:
    position_id: str
    notional_usd: Decimal
    entry_fee_usd: Decimal
    adverse_bps: int
    entry_evidence: dict[str, Any] | None
    exits: list[ExitFill] = field(default_factory=list)

    @property
    def cost_usd(self) -> Decimal:
        return self.notional_usd + self.entry_fee_usd

    def pnl(self, proceeds: Sequence[Decimal], *, extra_fee: Decimal = Decimal("0")) -> Decimal:
        fees = sum((item.network_fee_usd for item in self.exits), Decimal("0"))
        transactions = 1 + len(self.exits)
        return sum(proceeds, Decimal("0")) - fees - self.cost_usd - extra_fee * transactions


def _price(pool: Any) -> Decimal | None:
    if not isinstance(pool, dict):
        return None
    try:
        base = Decimal(str(pool["base_reserves"]))
        quote = Decimal(str(pool["quote_reserves"]))
    except (KeyError, ArithmeticError, ValueError):
        return None
    return quote / base if base > 0 and quote > 0 else None


def _quote_reserve_usd(pool: Any) -> Decimal | None:
    if not isinstance(pool, dict):
        return None
    try:
        quote = Decimal(str(pool["quote_reserves"]))
        decimals = int(pool["quote_decimals"])
        price = Decimal(str(pool["quote_price_usd"]))
    except (KeyError, ArithmeticError, ValueError):
        return None
    value = quote / (Decimal(10) ** decimals) * price
    return value if value > 0 else None


def scenario_adverse(trade: ClosedTrade, bps: int) -> Decimal:
    k = (ONE - Decimal(bps) / Decimal(10000)) / (
        ONE - Decimal(trade.adverse_bps) / Decimal(10000)
    )
    return trade.pnl([item.gross_usd * k * k for item in trade.exits])


def scenario_no_delay(trade: ClosedTrade) -> Decimal | None:
    evidence = trade.entry_evidence or {}
    decided, filled = _price(evidence.get("decision_pool")), _price(evidence.get("fill_pool"))
    if decided is None or filled is None:
        return None
    entry_scale = filled / decided  # more tokens when the price rose meanwhile
    proceeds: list[Decimal] = []
    for item in trade.exits:
        exit_evidence = item.evidence or {}
        exit_decided = _price(exit_evidence.get("decision_pool"))
        exit_filled = _price(exit_evidence.get("fill_pool"))
        if exit_decided is None or exit_filled is None:
            return None
        proceeds.append(item.gross_usd * entry_scale * exit_decided / exit_filled)
    return trade.pnl(proceeds)


def scenario_size(trade: ClosedTrade, multiple: Decimal) -> Decimal | None:
    entry_reserve = _quote_reserve_usd((trade.entry_evidence or {}).get("fill_pool"))
    if entry_reserve is None:
        return None
    entry_impact = (multiple - ONE) * trade.notional_usd / entry_reserve
    proceeds: list[Decimal] = []
    for item in trade.exits:
        exit_reserve = _quote_reserve_usd((item.evidence or {}).get("fill_pool"))
        if exit_reserve is None:
            return None
        exit_impact = (multiple - ONE) * item.gross_usd / exit_reserve
        proceeds.append(
            multiple
            * item.gross_usd
            * max(Decimal("0"), ONE - entry_impact)
            * max(Decimal("0"), ONE - exit_impact)
        )
    fees = sum((item.network_fee_usd for item in trade.exits), Decimal("0"))
    return sum(proceeds, Decimal("0")) - fees - (multiple * trade.notional_usd + trade.entry_fee_usd)


def _summary(values: Sequence[Decimal], capital: Sequence[Decimal]) -> dict[str, Any]:
    if not values:
        return {"trades": 0}
    returns = [value / base for value, base in zip(values, capital, strict=True) if base > 0]
    return {
        "trades": len(values),
        "total_pnl_usd": str(sum(values, Decimal("0")).quantize(Decimal("0.01"))),
        "mean_return_pct": str(
            (sum(returns, Decimal("0")) / len(returns) * 100).quantize(Decimal("0.01"))
        )
        if returns
        else None,
        "median_return_pct": str((Decimal(str(median(returns))) * 100).quantize(Decimal("0.01")))
        if returns
        else None,
        "win_rate": str(
            (Decimal(sum(1 for value in values if value > 0)) / len(values)).quantize(
                Decimal("0.0001")
            )
        ),
    }


def report(trades: Sequence[ClosedTrade]) -> dict[str, Any]:
    costs = [trade.cost_usd for trade in trades]
    result: dict[str, Any] = {
        "closed_trades": len(trades),
        "recorded": _summary([trade.pnl([item.gross_usd for item in trade.exits]) for trade in trades], costs),
        "adverse_bps": {
            str(bps): _summary([scenario_adverse(trade, bps) for trade in trades], costs)
            for bps in ADVERSE_BPS
        },
        "extra_fee_usd_per_transaction": {
            str(fee): _summary(
                [trade.pnl([item.gross_usd for item in trade.exits], extra_fee=fee) for trade in trades],
                costs,
            )
            for fee in EXTRA_FEES_USD
        },
    }
    no_delay = [(scenario_no_delay(trade), trade.cost_usd) for trade in trades]
    priced = [(value, cost) for value, cost in no_delay if value is not None]
    result["no_delay"] = {
        **_summary([value for value, _ in priced], [cost for _, cost in priced]),
        "trades_without_evidence": len(trades) - len(priced),
    }
    result["size_multiple"] = {}
    for multiple in SIZE_MULTIPLES:
        sized = [
            (scenario_size(trade, multiple), multiple * trade.notional_usd + trade.entry_fee_usd)
            for trade in trades
        ]
        kept = [(value, cost) for value, cost in sized if value is not None]
        result["size_multiple"][str(multiple)] = {
            **_summary([value for value, _ in kept], [cost for _, cost in kept]),
            "trades_without_evidence": len(trades) - len(kept),
        }
    return result


async def load_closed_trades(
    database: Database,
    *,
    since: datetime,
    until: datetime,
    strategy: str | None,
) -> list[ClosedTrade]:
    async with database.sessions() as session:
        query = select(PaperPositionRow).where(
            PaperPositionRow.status == "CLOSED",
            PaperPositionRow.entry_time >= since,
            PaperPositionRow.entry_time <= until,
        )
        if strategy:
            query = query.where(PaperPositionRow.strategy_version_id == strategy)
        positions = list((await session.scalars(query)).all())
        trades: list[ClosedTrade] = []
        for position in positions:
            rows = (
                await session.execute(
                    select(PaperFillRow, PaperOrderRow.requested_usd)
                    .join(PaperOrderRow, PaperOrderRow.id == PaperFillRow.order_id)
                    .where(PaperOrderRow.position_id == position.id)
                    .order_by(PaperFillRow.filled_at)
                )
            ).all()
            entry = next((fill for fill, _ in rows if fill.side == "BUY"), None)
            if entry is None:
                continue
            requested = next(
                (Decimal(requested) for fill, requested in rows if fill.side == "BUY" and requested),
                Decimal(entry.output_usd),
            )
            trade = ClosedTrade(
                position_id=position.id,
                notional_usd=requested,
                entry_fee_usd=Decimal(entry.network_fee_usd),
                adverse_bps=int(entry.adverse_fill_bps),
                entry_evidence=entry.evidence_json,
            )
            for fill, _ in rows:
                if fill.side != "SELL":
                    continue
                trade.exits.append(
                    ExitFill(
                        gross_usd=Decimal(fill.output_usd) + Decimal(fill.network_fee_usd),
                        network_fee_usd=Decimal(fill.network_fee_usd),
                        evidence=fill.evidence_json,
                    )
                )
            if trade.exits:
                trades.append(trade)
    return trades


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


async def run(args: argparse.Namespace) -> int:
    dsn = os.environ.get("POSTGRES_DSN", "").strip()
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is required")
    database = Database(dsn)
    try:
        trades = await load_closed_trades(
            database,
            since=_timestamp(args.since),
            until=_timestamp(args.until) if args.until else datetime.now(tz=timezone.utc),
            strategy=args.strategy_version,
        )
    finally:
        await database.close()
    print(json.dumps(report(trades), indent=2, sort_keys=True))
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--since", required=True)
    parser.add_argument("--until")
    parser.add_argument("--strategy-version")
    raise SystemExit(asyncio.run(run(parser.parse_args())))


if __name__ == "__main__":
    main()
