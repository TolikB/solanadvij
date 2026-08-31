from __future__ import annotations

import asyncio
import os
import re

from sqlalchemy import text

from sniper_bot.database import Database

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


async def main() -> None:
    role = os.environ["SNIPER_DB_USER"]
    if not _IDENTIFIER.fullmatch(role):
        raise SystemExit("SNIPER_DB_USER is not a valid PostgreSQL identifier")
    database = Database(os.environ["MIGRATION_POSTGRES_DSN"])
    quoted = database.engine.dialect.identifier_preparer.quote_identifier(role)
    async with database.engine.begin() as connection:
        await connection.execute(text(f"GRANT USAGE ON SCHEMA public TO {quoted}"))
        await connection.execute(
            text(f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {quoted}")
        )
        await connection.execute(
            text(f"REVOKE UPDATE, DELETE ON TABLE operational_costs FROM {quoted}")
        )
        await connection.execute(
            text(f"REVOKE UPDATE, DELETE ON TABLE strategy_versions FROM {quoted}")
        )
        await connection.execute(
            text(f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {quoted}")
        )
        await connection.execute(
            text(
                "GRANT EXECUTE ON FUNCTION "
                "public.ensure_raw_chain_events_partition(date) "
                f"TO {quoted}"
            )
        )
    await database.close()


if __name__ == "__main__":
    asyncio.run(main())
