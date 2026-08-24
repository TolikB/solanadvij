from __future__ import annotations

import asyncio
import os

from sqlalchemy import text

from sniper_bot.database import Database


async def main() -> None:
    role = os.environ["SNIPER_DB_USER"]
    database = Database(os.environ["MIGRATION_POSTGRES_DSN"])
    try:
        async with database.engine.connect() as connection:
            for table in ("operational_costs", "strategy_versions"):
                for privilege in ("UPDATE", "DELETE"):
                    allowed = await connection.scalar(
                        text("SELECT has_table_privilege(:role, :table, :privilege)"),
                        {"role": role, "table": table, "privilege": privilege},
                    )
                    if allowed:
                        raise RuntimeError(f"{role} unexpectedly has {privilege} on {table}")
    finally:
        await database.close()
    print("APP_ROLE_HARDENING_OK")


if __name__ == "__main__":
    asyncio.run(main())
