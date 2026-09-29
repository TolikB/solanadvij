"""Measure the strategy funnel on a calibration soak and apply the frozen rule.

The record-mode calibration soak runs ``configs/calibration.yaml``, the
loosest rung of the ladder below. Every candidate that got through all entry
rules there is replayed offline against each stricter rung: its quote
liquidity must hold the rung's floor at every evaluation from the security
check to the entry (security re-checks liquidity each tick), and its unique
buyers must meet the rung's floor at the entry. The other filters and the
state path do not depend on these two thresholds, so the count is exact for
the observed ticks.

Decision rule, fixed before any calibration data exists (docs/RUNBOOK.md):
walk the ladder from the specified strategy towards the loosest rung and take
the first rung whose entries per day reach 300 closed trades over 30 days with
a 25% margin (12.5 a day); the trade cap then rises to 24 so busy days do not
cut the sample. The specified rung is decision A, a looser one decision B. If
no rung gets there the specified strategy stays unchanged (decision C) and a
short sample is itself the result. Safety, execution, holder, developer, exit
and sizing rules are never part of the ladder.

    python scripts/calibration_funnel.py --hours 24
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select

from sniper_bot.database import Database
from sniper_bot.db_models import (
    CandidateRow,
    MarketSnapshotRow,
    PaperPositionRow,
    RawChainEventRow,
    SignalEvaluationRow,
    StrategyVersionRow,
    TokenSecurityCheckRow,
)


@dataclass(frozen=True)
class Rung:
    name: str
    min_quote_liquidity_usd: Decimal
    min_unique_buyers_60s: int


LADDER = (
    Rung("R0", Decimal("40000"), 25),  # the specified strategy
    Rung("R1", Decimal("30000"), 25),
    Rung("R2", Decimal("20000"), 25),
    Rung("R3", Decimal("20000"), 20),  # configs/calibration.yaml
)
TARGET_CLOSED_TRADES = 300
COLLECTION_DAYS = 30
SAFETY_MARGIN = Decimal("1.25")
TARGET_ENTRIES_PER_DAY = (
    Decimal(TARGET_CLOSED_TRADES) / Decimal(COLLECTION_DAYS) * SAFETY_MARGIN
)
RAISED_TRADE_CAP = 24
MINIMUM_WINDOW = timedelta(hours=20)
# Rejections that happen only after every entry rule passed.
PASSED_ENTRY_REASONS = frozenset(
    {
        "RISK_MANAGER_BLOCKED",
        "MAX_OPEN_POSITIONS",
        "DAILY_RISK_LIMIT",
        "POSITION_TOO_SMALL_AFTER_COSTS",
    }
)
ENTERED_STATES = frozenset(
    {"POSITION_OPEN", "POSITION_PARTIAL", "EXIT_PENDING", "RETRYING_EXIT", "CLOSED"}
)
UNTRADABLE_REASONS = frozenset(
    {"STREAM_NOT_TRADABLE", "UNSUPPORTED_QUOTE_MINT", "COLLECTION_WINDOW_CLOSED"}
)


@dataclass(frozen=True)
class PassedCandidate:
    candidate_id: str
    min_liquidity_usd: Decimal | None
    entry_unique_buyers_60s: int | None

    def passes(self, rung: Rung) -> bool:
        return (
            self.min_liquidity_usd is not None
            and self.entry_unique_buyers_60s is not None
            and self.min_liquidity_usd >= rung.min_quote_liquidity_usd
            and self.entry_unique_buyers_60s >= rung.min_unique_buyers_60s
        )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def decide(
    passed: Sequence[PassedCandidate],
    *,
    days: Decimal,
    calibration: Rung,
    window_long_enough: bool,
) -> dict[str, Any]:
    rungs: list[dict[str, Any]] = []
    chosen: Rung | None = None
    for rung in LADDER:
        measurable = (
            calibration.min_quote_liquidity_usd <= rung.min_quote_liquidity_usd
            and calibration.min_unique_buyers_60s <= rung.min_unique_buyers_60s
        )
        count = sum(1 for item in passed if item.passes(rung)) if measurable else None
        per_day = (Decimal(count) / days).quantize(Decimal("0.01")) if count is not None else None
        reaches = per_day is not None and per_day >= TARGET_ENTRIES_PER_DAY
        rungs.append(
            {
                "rung": rung.name,
                "min_quote_liquidity_usd": str(rung.min_quote_liquidity_usd),
                "min_unique_buyers_60s": rung.min_unique_buyers_60s,
                "measurable": measurable,
                "entries": count,
                "entries_per_day": str(per_day) if per_day is not None else None,
                "reaches_target": reaches,
            }
        )
        if chosen is None and reaches:
            chosen = rung
    if not window_long_enough:
        decision: dict[str, Any] = {
            "decision": None,
            "reason": f"calibration window shorter than {MINIMUM_WINDOW}",
            "config_changes": {},
        }
    elif chosen is None:
        decision = {
            "decision": "C",
            "reason": "no ladder rung reaches the target; keep the specified strategy",
            "config_changes": {},
        }
    else:
        changes: dict[str, Any] = {"risk.max_trades_per_day": RAISED_TRADE_CAP}
        if chosen != LADDER[0]:
            changes["liquidity.min_quote_liquidity_usd"] = int(chosen.min_quote_liquidity_usd)
            changes["flow.min_unique_buyers_60s"] = chosen.min_unique_buyers_60s
        decision = {
            "decision": "A" if chosen == LADDER[0] else "B",
            "rung": chosen.name,
            "reason": f"{chosen.name} is the first rung reaching {TARGET_ENTRIES_PER_DAY} entries a day",
            "config_changes": changes,
        }
    return {
        "target_entries_per_day": str(TARGET_ENTRIES_PER_DAY),
        "ladder": rungs,
        **decision,
    }


async def _strategy_for_window(
    database: Database, since: datetime, until: datetime, strategy: str | None
) -> StrategyVersionRow:
    async with database.sessions() as session:
        if strategy is None:
            strategy = await session.scalar(
                select(CandidateRow.strategy_version_id)
                .where(CandidateRow.detected_at >= since, CandidateRow.detected_at <= until)
                .group_by(CandidateRow.strategy_version_id)
                .order_by(func.count().desc())
                .limit(1)
            )
        if strategy is None:
            raise ValueError("no candidates were detected in the calibration window")
        row = await session.get(StrategyVersionRow, strategy)
    if row is None:
        raise ValueError(f"strategy version {strategy} is not registered")
    return row


async def analyze(
    database: Database,
    *,
    since: datetime,
    until: datetime,
    strategy: str | None = None,
) -> dict[str, Any]:
    strategy_row = await _strategy_for_window(database, since, until, strategy)
    config = strategy_row.config_json
    calibration = Rung(
        "calibration",
        Decimal(str(config["liquidity"]["min_quote_liquidity_usd"])),
        int(config["flow"]["min_unique_buyers_60s"]),
    )
    collect = timedelta(seconds=int(config["candidate"]["min_observation_seconds"]))
    async with database.sessions() as session:
        discovered = int(
            await session.scalar(
                select(func.count(func.distinct(RawChainEventRow.pool_address))).where(
                    RawChainEventRow.protocol == "pumpswap",
                    RawChainEventRow.event_type == "pool_created",
                    RawChainEventRow.block_time >= since,
                    RawChainEventRow.block_time <= until,
                )
            )
            or 0
        )
        candidates = (
            await session.execute(
                select(
                    CandidateRow.id,
                    CandidateRow.mint,
                    CandidateRow.pool_address,
                    CandidateRow.state,
                    CandidateRow.reject_reason,
                    CandidateRow.detected_at,
                    CandidateRow.rejected_at,
                ).where(
                    CandidateRow.strategy_version_id == strategy_row.id,
                    CandidateRow.detected_at >= since,
                    CandidateRow.detected_at <= until,
                )
            )
        ).all()
        live_mints = [
            row.mint for row in candidates if row.reject_reason not in UNTRADABLE_REASONS
        ]
        security_reasons: Counter[str] = Counter()
        security_checked = 0
        if live_mints:
            for reasons in (
                await session.scalars(
                    select(TokenSecurityCheckRow.reject_reasons_json).where(
                        TokenSecurityCheckRow.mint.in_(live_mints),
                        TokenSecurityCheckRow.checked_at >= since,
                        TokenSecurityCheckRow.checked_at <= until + timedelta(minutes=10),
                    )
                )
            ).all():
                security_checked += 1
                security_reasons.update(str(reason) for reason in reasons or [])
        best_scores = dict(
            (
                await session.execute(
                    select(SignalEvaluationRow.candidate_id, func.max(SignalEvaluationRow.score))
                    .where(SignalEvaluationRow.candidate_id.in_([row.id for row in candidates]))
                    .group_by(SignalEvaluationRow.candidate_id)
                )
            ).all()
        ) if candidates else {}
        passed: list[PassedCandidate] = []
        for row in candidates:
            if row.reject_reason in PASSED_ENTRY_REASONS:
                entry_time = row.rejected_at
            elif row.state in ENTERED_STATES:
                entry_time = await session.scalar(
                    select(func.min(PaperPositionRow.entry_time)).where(
                        PaperPositionRow.pool_address == row.pool_address,
                        PaperPositionRow.strategy_version_id == strategy_row.id,
                    )
                )
            else:
                continue
            if entry_time is None:
                passed.append(PassedCandidate(row.id, None, None))
                continue
            snapshots = (
                await session.execute(
                    select(MarketSnapshotRow.snapshot_time, MarketSnapshotRow.features_json)
                    .where(
                        MarketSnapshotRow.pool_address == row.pool_address,
                        MarketSnapshotRow.snapshot_time >= _utc(row.detected_at) + collect,
                        MarketSnapshotRow.snapshot_time <= _utc(entry_time),
                    )
                    .order_by(MarketSnapshotRow.snapshot_time)
                )
            ).all()
            if not snapshots:
                passed.append(PassedCandidate(row.id, None, None))
                continue
            passed.append(
                PassedCandidate(
                    row.id,
                    min(
                        Decimal(str(features.get("quote_liquidity_usd", "0")))
                        for _, features in snapshots
                    ),
                    int(snapshots[-1][1].get("unique_buyers_60s", 0)),
                )
            )
    days = Decimal(str((until - since).total_seconds())) / Decimal(86400)
    return {
        "since": since.isoformat(),
        "until": until.isoformat(),
        "strategy_version_id": strategy_row.id,
        "calibration_min_quote_liquidity_usd": str(calibration.min_quote_liquidity_usd),
        "calibration_min_unique_buyers_60s": calibration.min_unique_buyers_60s,
        "discovered_pumpswap_pools": discovered,
        "candidates": len(candidates),
        "live_candidates": len(live_mints),
        "reject_reasons": dict(
            Counter(row.reject_reason or row.state for row in candidates).most_common()
        ),
        "provider_checks": security_checked,
        "provider_reject_reasons": dict(security_reasons.most_common()),
        "candidates_scoring_80_or_more": sum(
            1 for score in best_scores.values() if Decimal(str(score)) >= Decimal("80")
        ),
        "passed_every_entry_rule": len(passed),
        "passed_without_market_history": sum(
            1 for item in passed if item.min_liquidity_usd is None
        ),
        **decide(
            passed,
            days=days,
            calibration=calibration,
            window_long_enough=until - since >= MINIMUM_WINDOW,
        ),
    }


async def run(args: argparse.Namespace) -> int:
    dsn = os.environ.get("POSTGRES_DSN", "").strip()
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is required")
    until = datetime.now(tz=timezone.utc) if args.until is None else _utc(
        datetime.fromisoformat(args.until.replace("Z", "+00:00"))
    )
    since = until - timedelta(hours=args.hours)
    database = Database(dsn)
    try:
        report = await analyze(database, since=since, until=until, strategy=args.strategy_version)
    except ValueError as error:
        report = {"decision": None, "reason": str(error), "config_changes": {}}
    finally:
        await database.close()
    print(json.dumps(report, sort_keys=True))
    return 0 if report["decision"] is not None else 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--hours", type=float, default=24.0)
    parser.add_argument("--until", help="end of the window, UTC ISO (default: now)")
    parser.add_argument("--strategy-version", help="default: most candidates in the window")
    args = parser.parse_args()
    if not math.isfinite(args.hours) or args.hours <= 0:
        parser.error("--hours must be positive")
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
