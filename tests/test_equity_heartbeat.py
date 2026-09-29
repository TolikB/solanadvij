from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from sniper_bot.config import AppConfig
from sniper_bot.database import Database
from sniper_bot.db_models import PaperEquityMarkRow
from sniper_bot.runtime import EQUITY_MARK_HEARTBEAT, SniperRuntime

NOW = datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)


def _runtime(tmp_path: Path) -> SniperRuntime:
    config = AppConfig(
        APP_MODE="paper",
        APP_REVISION="",
        HELIUS_API_KEY="helius-key",
        JUPITER_API_KEY="jupiter-key",
        POSTGRES_DSN="postgresql://user:pass@localhost:5432/db",
        TELEGRAM_BOT_TOKEN="telegram-token",
        TELEGRAM_ADMIN_CHAT_ID=123456,
        STARTING_EQUITY_USD=Decimal("500"),
    )
    return SniperRuntime(config, data_dir=tmp_path)


@pytest.mark.asyncio
async def test_flat_account_keeps_a_bounded_equity_mark_path(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    database = AsyncMock()
    runtime.database = database
    runtime.database_available = True

    for offset in (0, 30, 59, 60, 61, 125):
        await runtime.evaluate_and_close_exits(now=NOW + timedelta(seconds=offset))

    marked_at = [
        call.kwargs["observed_at"]
        for call in database.record_equity_heartbeat.await_args_list
    ]
    assert marked_at == [
        NOW,
        NOW + EQUITY_MARK_HEARTBEAT,
        NOW + timedelta(seconds=125),
    ]
    assert all(
        call.kwargs["account_id"] == "paper-main"
        for call in database.record_equity_heartbeat.await_args_list
    )


@pytest.mark.asyncio
async def test_equity_heartbeat_copies_the_authoritative_account(tmp_path: Path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'marks.db'}")
    await database.create_schema_for_tests()
    await database.initialize_paper_account(
        account_id="paper-main", starting_equity=Decimal("500"), now=NOW
    )

    await database.record_equity_heartbeat(
        account_id="paper-main", observed_at=NOW + timedelta(minutes=1)
    )

    async with database.sessions() as session:
        marks = list(
            (
                await session.scalars(
                    select(PaperEquityMarkRow).order_by(PaperEquityMarkRow.observed_at)
                )
            ).all()
        )
    assert [Decimal(mark.equity) for mark in marks] == [Decimal("500"), Decimal("500")]
    assert marks[-1].observed_at.replace(tzinfo=timezone.utc) == NOW + timedelta(minutes=1)
    with pytest.raises(RuntimeError, match="paper account is unavailable"):
        await database.record_equity_heartbeat(account_id="missing", observed_at=NOW)
    await database.close()


@pytest.mark.asyncio
async def test_latest_equity_mark_reports_the_newest_mark(tmp_path: Path) -> None:
    from scripts.latest_equity_mark import latest_mark

    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'latest.db'}")
    await database.create_schema_for_tests()
    assert await latest_mark(database, "paper-main") is None

    await database.initialize_paper_account(
        account_id="paper-main", starting_equity=Decimal("500"), now=NOW
    )
    await database.record_equity_heartbeat(
        account_id="paper-main", observed_at=NOW + timedelta(seconds=90)
    )

    assert await latest_mark(database, "paper-main") == NOW + timedelta(seconds=90)
    await database.close()
