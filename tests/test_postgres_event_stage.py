from datetime import date, datetime, timezone

from sniper_bot.database import (
    POSTGRES_EVENT_STAGE_COLUMNS,
    POSTGRES_EVENT_STAGE_DDL,
    _postgresql_event_stage_records,
)


def test_postgres_claim_stage_excludes_raw_event_payload() -> None:
    first_seen_at = datetime(2026, 8, 31, tzinfo=timezone.utc)
    row = {
        "event_id": "event-id",
        "block_date": date(2026, 8, 31),
        "first_seen_at": first_seen_at,
        "processing_token": "claim-token",
    }

    assert POSTGRES_EVENT_STAGE_COLUMNS == (
        "event_id",
        "block_date",
        "first_seen_at",
        "processing_token",
    )
    assert _postgresql_event_stage_records([row]) == [
        ("event-id", date(2026, 8, 31), first_seen_at, "claim-token")
    ]
    assert "payload_json" not in POSTGRES_EVENT_STAGE_DDL
    assert "ON COMMIT DELETE ROWS" in POSTGRES_EVENT_STAGE_DDL