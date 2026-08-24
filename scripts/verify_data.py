from __future__ import annotations

import asyncio
import os

from sqlalchemy import text

from sniper_bot.database import Database


async def main() -> None:
    dsn = os.environ["POSTGRES_DSN"]
    database = Database(dsn)
    checks = {
        "duplicate_event_ids": "SELECT count(*) FROM (SELECT event_id FROM event_dedup GROUP BY event_id HAVING count(*) > 1) q",
        "orphan_fills": "SELECT count(*) FROM paper_fills f LEFT JOIN paper_orders o ON o.id=f.order_id WHERE o.id IS NULL",
        "multiple_open_positions": "SELECT count(*) FROM (SELECT mint FROM paper_positions WHERE status IN ('OPEN','PARTIAL') GROUP BY mint HAVING count(*) > 1) q",
        "negative_accounts": "SELECT count(*) FROM paper_accounts WHERE cash_balance < 0 OR locked_capital < 0",
    }
    failed = False
    async with database.engine.connect() as connection:
        for name, query in checks.items():
            count = int((await connection.scalar(text(query))) or 0)
            print(f"{name}={count}")
            failed = failed or count != 0
    await database.close()
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
