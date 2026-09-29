"""Report whether the existing database lets the bot start ingesting.

Startup refuses to open the stream while any event is terminally failed or an
unresolved event sits in front of processed ones, so check that before a
deployment instead of discovering it from a silent stream. Prints one JSON
document and exits non-zero when the stream would stay disabled.
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select

from sniper_bot.database import Database
from sniper_bot.db_models import EventDedupRow, RawChainEventRow


async def inspect(database: Database) -> dict[str, Any]:
    quarantined = sorted(await database.load_quarantined_event_protocols())
    unprocessed = len(await database.load_unprocessed_events(include_owned_processing=True))
    slot, _signature, observed_at = await database.load_stream_checkpoint()
    async with database.sessions() as session:
        statuses = {
            str(status): int(count)
            for status, count in (
                await session.execute(
                    select(EventDedupRow.processing_status, func.count()).group_by(
                        EventDedupRow.processing_status
                    )
                )
            ).all()
        }
        raw_events = int(
            await session.scalar(select(func.count()).select_from(RawChainEventRow)) or 0
        )
    checkpoint_age = (
        (datetime.now(tz=timezone.utc) - observed_at).total_seconds()
        if observed_at is not None
        else None
    )
    return {
        "stream_enabled_at_startup": not quarantined,
        "quarantined_protocols": quarantined,
        "events_to_retry_at_startup": unprocessed,
        "event_claim_statuses": statuses,
        "raw_events": raw_events,
        "stream_checkpoint_slot": slot,
        "stream_checkpoint_age_seconds": checkpoint_age,
    }


async def run() -> int:
    dsn = os.environ.get("POSTGRES_DSN", "").strip()
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is required")
    database = Database(dsn)
    try:
        report = await inspect(database)
    finally:
        await database.close()
    print(json.dumps(report, sort_keys=True))
    return 0 if report["stream_enabled_at_startup"] else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
