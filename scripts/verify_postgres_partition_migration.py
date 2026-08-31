from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import text

from scripts.verify_postgres_hardening import _validate_probe_target
from sniper_bot.database import Database

PREVIOUS_REVISION = "20260824_0007"


def _alembic(*arguments: str) -> None:
    subprocess.run(
        [sys.executable, "-m", "alembic", *arguments],
        check=True,
        env=os.environ.copy(),
    )


def _probe_rows(now: datetime) -> list[dict[str, Any]]:
    return [
        {
            "id": str(uuid4()),
            "event_id": uuid4().hex,
            "block_date": (now - timedelta(days=offset)).date(),
            "slot": 900_000 + offset,
            "signature": f"partition-migration-{uuid4().hex}",
            "block_time": now - timedelta(days=offset),
            "observed_at": now,
            "payload_json": json.dumps({"probe": True, "offset": offset}),
        }
        for offset in (1, 0)
    ]


async def _seed_default_partition(
    dsn: str,
    rows: list[dict[str, Any]],
) -> None:
    database = Database(dsn)
    try:
        async with database.engine.begin() as connection:
            for row in rows:
                await connection.execute(
                    text(
                        """
                        INSERT INTO public.event_dedup
                            (event_id, block_date, first_seen_at,
                             processing_status, processing_attempts,
                             last_attempt_at, processed_at, last_error,
                             processing_token)
                        VALUES
                            (:event_id, :block_date, :observed_at,
                             'PROCESSED', 1, :observed_at, :observed_at,
                             NULL, NULL)
                        """
                    ),
                    row,
                )
                await connection.execute(
                    text(
                        """
                        INSERT INTO public.raw_chain_events_default
                            (id, block_date, event_id, ingest_sequence,
                             source, protocol, event_type, slot, signature,
                             instruction_index, inner_instruction_index,
                             block_time, observed_at, commitment, mint,
                             pool_address, payload_json, created_at)
                        VALUES
                            (:id, :block_date, :event_id,
                             nextval('raw_chain_events_ingest_sequence_seq'),
                             'replay', 'pumpswap', 'swap_buy', :slot,
                             :signature, 0, -1, :block_time, :observed_at,
                             'confirmed', 'CI_TOKEN', 'CI_POOL',
                             CAST(:payload_json AS json), :observed_at)
                        """
                    ),
                    row,
                )
    finally:
        await database.close()


async def _assert_partitioned(
    dsn: str,
    rows: list[dict[str, Any]],
) -> None:
    database = Database(dsn)
    event_ids = [str(row["event_id"]) for row in rows]
    expected_partitions = {
        f"raw_chain_events_{row['block_date']:%Y%m%d}" for row in rows
    }
    try:
        async with database.engine.connect() as connection:
            migrated = list(
                (
                    await connection.execute(
                        text(
                            """
                            SELECT event_id, block_date, ingest_sequence,
                                   tableoid::regclass::text AS partition_name
                            FROM public.raw_chain_events
                            WHERE event_id::text =
                                  ANY(CAST(:event_ids AS text[]))
                            ORDER BY event_id
                            """
                        ),
                        {"event_ids": event_ids},
                    )
                ).mappings()
            )
            default_rows = int(
                (
                    await connection.scalar(
                        text(
                            "SELECT count(*) FROM ONLY "
                            "public.raw_chain_events_default"
                        )
                    )
                )
                or 0
            )
            function_security = (
                await connection.execute(
                    text(
                        """
                        SELECT prosecdef, proconfig
                        FROM pg_catalog.pg_proc
                        WHERE oid =
                            'public.ensure_raw_chain_events_partition(date)'
                            ::regprocedure
                        """
                    )
                )
            ).one()
        if len(migrated) != len(rows):
            raise RuntimeError("partition migration lost probe rows")
        actual_partitions = {str(row.partition_name) for row in migrated}
        if actual_partitions != expected_partitions or default_rows != 0:
            raise RuntimeError(
                "partition migration left rows outside daily partitions"
            )
        if function_security.prosecdef is not True or not any(
            setting == "search_path=pg_catalog, public"
            for setting in (function_security.proconfig or [])
        ):
            raise RuntimeError(
                "partition function is not security-definer hardened"
            )
    finally:
        await database.close()


async def _assert_downgraded(
    dsn: str,
    rows: list[dict[str, Any]],
) -> None:
    database = Database(dsn)
    event_ids = [str(row["event_id"]) for row in rows]
    try:
        async with database.engine.connect() as connection:
            default_rows = int(
                (
                    await connection.scalar(
                        text(
                            """
                            SELECT count(*)
                            FROM ONLY public.raw_chain_events_default
                            WHERE event_id::text =
                                  ANY(CAST(:event_ids AS text[]))
                            """
                        ),
                        {"event_ids": event_ids},
                    )
                )
                or 0
            )
            daily_partitions = int(
                (
                    await connection.scalar(
                        text(
                            """
                            SELECT count(*)
                            FROM pg_catalog.pg_inherits AS inheritance
                            JOIN pg_catalog.pg_class AS child
                              ON child.oid = inheritance.inhrelid
                            WHERE inheritance.inhparent =
                                  'public.raw_chain_events'::regclass
                              AND child.relname <>
                                  'raw_chain_events_default'
                            """
                        )
                    )
                )
                or 0
            )
            function_exists = await connection.scalar(
                text(
                    "SELECT pg_catalog.to_regprocedure("
                    "'public.ensure_raw_chain_events_partition(date)')"
                )
            )
        if default_rows != len(rows) or daily_partitions != 0:
            raise RuntimeError(
                "partition downgrade did not preserve rows in default"
            )
        if function_exists is not None:
            raise RuntimeError(
                "partition downgrade left privileged function installed"
            )
    finally:
        await database.close()


async def _cleanup(dsn: str, rows: list[dict[str, Any]]) -> None:
    database = Database(dsn)
    event_ids = [str(row["event_id"]) for row in rows]
    try:
        async with database.engine.begin() as connection:
            await connection.execute(
                text(
                    "DELETE FROM public.raw_chain_events "
                    "WHERE event_id::text = "
                    "ANY(CAST(:event_ids AS text[]))"
                ),
                {"event_ids": event_ids},
            )
            await connection.execute(
                text(
                    "DELETE FROM public.event_dedup "
                    "WHERE event_id::text = "
                    "ANY(CAST(:event_ids AS text[]))"
                ),
                {"event_ids": event_ids},
            )
    finally:
        await database.close()


def main() -> None:
    dsn = os.environ.get("MIGRATION_POSTGRES_DSN", "").strip()
    _validate_probe_target(
        dsn,
        github_actions=os.environ.get("GITHUB_ACTIONS"),
        confirmation=os.environ.get("POSTGRES_HARDENING_PROBE_CONFIRM"),
    )
    rows = _probe_rows(datetime.now(tz=timezone.utc))
    asyncio.run(_seed_default_partition(dsn, rows))
    _alembic("upgrade", "head")
    asyncio.run(_assert_partitioned(dsn, rows))
    _alembic("downgrade", PREVIOUS_REVISION)
    asyncio.run(_assert_downgraded(dsn, rows))
    _alembic("upgrade", "head")
    asyncio.run(_assert_partitioned(dsn, rows))
    asyncio.run(_cleanup(dsn, rows))
    print(
        "POSTGRES_PARTITION_MIGRATION_OK "
        f"rows={len(rows)} round_trip=preserved"
    )


if __name__ == "__main__":
    main()
