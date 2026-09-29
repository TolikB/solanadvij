from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import update

from scripts.preflight_db import inspect
from sniper_bot.database import MAX_EVENT_PROCESSING_ATTEMPTS, Database
from sniper_bot.db_models import EventDedupRow
from sniper_bot.events import ChainEventType, EventEnvelope, EventSource, Protocol

NOW = datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)


def _event(signature: str) -> EventEnvelope:
    return EventEnvelope(
        source=EventSource.HELIUS_WSS,
        protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.POOL_CREATED,
        slot=1,
        signature=signature,
        instruction_index=1,
        block_time=NOW,
        observed_at=NOW,
        pool_address="POOL",
        payload={},
    )


@pytest.mark.asyncio
async def test_preflight_reports_a_terminally_failed_event_as_a_disabled_stream(
    tmp_path,
) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'preflight.db'}")
    await database.create_schema_for_tests()

    clean = await inspect(database)
    assert clean["stream_enabled_at_startup"] is True
    assert clean["raw_events"] == 0

    event = _event("stuck")
    await database.record_events([event])
    async with database.sessions.begin() as session:
        await session.execute(
            update(EventDedupRow)
            .where(EventDedupRow.event_id == event.event_id)
            .values(
                processing_status="FAILED",
                processing_attempts=MAX_EVENT_PROCESSING_ATTEMPTS,
            )
        )

    blocked = await inspect(database)
    assert blocked["stream_enabled_at_startup"] is False
    assert blocked["quarantined_protocols"] == ["pumpswap"]
    assert blocked["event_claim_statuses"] == {"FAILED": 1}
    await database.close()
