"""Show how far the frozen statistical window has accumulated its sample.

Reads the same authoritative rows as ``analyze_statistical_stage.py`` but only
reports progress, so it can run at any time during collection:

    python scripts/collection_progress.py --protocol statistical-protocol.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sniper_bot.acceptance import (
    StatisticalInputs,
    StatisticalProtocol,
    load_statistical_stage_data,
)
from sniper_bot.database import Database

TARGET_DISCOVERED_POOLS = 3000
TARGET_CLOSED_TRADES = 300


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def summarize(
    inputs: StatisticalInputs, protocol: StatisticalProtocol, now: datetime
) -> dict[str, Any]:
    if now < protocol.collection_started_at:
        phase = "not_started"
    elif now < protocol.oos_started_at:
        phase = "in_sample"
    elif now <= protocol.collection_ended_at:
        phase = "out_of_sample"
    else:
        phase = "ended"
    oos_trades = sum(
        1 for trade in inputs.closed_trades if _utc(trade.entry_time) >= protocol.oos_started_at
    )
    signal_trades = [*inputs.closed_trades, *inputs.shadow_trades]
    signal_oos = sum(
        1 for trade in signal_trades if _utc(trade.entry_time) >= protocol.oos_started_at
    )
    return {
        "phase": phase,
        "now": now.isoformat(),
        "collection_started_at": protocol.collection_started_at.isoformat(),
        "oos_started_at": protocol.oos_started_at.isoformat(),
        "collection_ended_at": protocol.collection_ended_at.isoformat(),
        "fixed_revision_strategy_cohort": inputs.fixed_revision_strategy_cohort,
        "discovered_pumpswap_pools": inputs.discovered_pool_count,
        "discovered_pumpswap_pools_target": TARGET_DISCOVERED_POOLS,
        "materialized_pumpswap_pools": inputs.materialized_pool_count,
        "pools_without_row": inputs.missing_materialized_pool_count,
        # Pools created in the last few minutes are normally still open here;
        # at the end of the window this must be zero.
        "pools_without_outcome_yet": inputs.missing_final_pool_outcome_count,
        "rejected_pools": inputs.negative_launch_count,
        "rejected_pools_target": protocol.minimum_negative_launches,
        # The sample-size targets count signal trades: account plus shadow.
        "closed_trades": len(inputs.closed_trades),
        "shadow_trades": len(inputs.shadow_trades),
        "signal_trades": len(signal_trades),
        "signal_trades_target": TARGET_CLOSED_TRADES,
        "in_sample_signal_trades": len(signal_trades) - signal_oos,
        "oos_signal_trades": signal_oos,
        "oos_signal_trades_target": protocol.minimum_oos_trades,
        "oos_account_trades": oos_trades,
        "open_positions": inputs.censored_position_count,
        "open_shadow_positions": inputs.censored_shadow_position_count,
        "oos_equity_marks": sum(
            1
            for point in inputs.equity_points
            if _utc(point.observed_at) > protocol.oos_started_at
        ),
    }


async def run(protocol_path: Path) -> int:
    dsn = os.environ.get("POSTGRES_DSN", "").strip()
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is required")
    protocol_bytes = await asyncio.to_thread(protocol_path.read_bytes)
    protocol = StatisticalProtocol.model_validate_json(protocol_bytes)
    database = Database(dsn)
    try:
        inputs = await load_statistical_stage_data(database, protocol)
    finally:
        await database.close()
    summary = summarize(inputs, protocol, datetime.now(tz=timezone.utc))
    print(json.dumps(summary, sort_keys=True))
    return 0 if summary["fixed_revision_strategy_cohort"] else 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--protocol", required=True)
    args = parser.parse_args()
    raise SystemExit(asyncio.run(run(Path(args.protocol))))


if __name__ == "__main__":
    main()
