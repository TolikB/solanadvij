"""Print the newest durable equity mark of the paper account as JSON.

The restart drill reads it before stopping the bot and until a newer one
appears after the restart; the difference is the gap the statistical
protocol's equity coverage has to tolerate.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select

from sniper_bot.database import Database
from sniper_bot.db_models import PaperEquityMarkRow


async def latest_mark(database: Database, account_id: str) -> datetime | None:
    async with database.sessions() as session:
        value = await session.scalar(
            select(func.max(PaperEquityMarkRow.observed_at)).where(
                PaperEquityMarkRow.account_id == account_id
            )
        )
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


async def run(account_id: str) -> int:
    dsn = os.environ.get("POSTGRES_DSN", "").strip()
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is required")
    database = Database(dsn)
    try:
        observed_at = await latest_mark(database, account_id)
    finally:
        await database.close()
    result: dict[str, Any] = {
        "account_id": account_id,
        "observed_at": observed_at.isoformat() if observed_at else None,
        "epoch": observed_at.timestamp() if observed_at else None,
    }
    print(json.dumps(result, sort_keys=True))
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--account-id", default="paper-main")
    raise SystemExit(asyncio.run(run(parser.parse_args().account_id)))


if __name__ == "__main__":
    main()
