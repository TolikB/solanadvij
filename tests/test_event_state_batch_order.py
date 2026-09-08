"""Staged upserts must respect foreign keys, not alphabetical model names."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import event as sqlalchemy_event
from sqlalchemy import select

from sniper_bot.candidates import Candidate
from sniper_bot.database import (
    TABLE_DEPENDENCY_ORDER,
    Database,
    _upsert_group_order,
)
from sniper_bot.db_models import CandidateRow, PoolRow, TokenRow
from sniper_bot.registry import PoolRecord, TokenRecord

NOW = datetime(2026, 9, 1, tzinfo=timezone.utc)
MINT = "BENCHMARK_MINT"
POOL = "BENCHMARK_POOL"
QUOTE = "So11111111111111111111111111111111111111112"


def test_dependent_tables_are_staged_after_their_parents() -> None:
    groups = [
        (CandidateRow, ("id",)),
        (TokenRow, ("mint",)),
        (PoolRow, ("pool_address",)),
    ]

    ordered = [model.__name__ for model, _ in sorted(groups, key=_upsert_group_order)]

    assert ordered == ["TokenRow", "PoolRow", "CandidateRow"]
    assert (
        TABLE_DEPENDENCY_ORDER["tokens"]
        < TABLE_DEPENDENCY_ORDER["candidates"]
    )
    assert (
        TABLE_DEPENDENCY_ORDER["pools"]
        < TABLE_DEPENDENCY_ORDER["candidates"]
    )


async def _database_with_enforced_foreign_keys(path: Path) -> Database:
    database = Database(f"sqlite+aiosqlite:///{path}")

    def enable_foreign_keys(connection: object, _record: object) -> None:
        cursor = connection.cursor()  # type: ignore[attr-defined]
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    sqlalchemy_event.listen(
        database.engine.sync_engine, "connect", enable_foreign_keys
    )
    await database.create_schema_for_tests()
    return database


@pytest.mark.asyncio
async def test_one_batch_persists_a_token_pool_and_candidate_together(
    tmp_path: Path,
) -> None:
    database = await _database_with_enforced_foreign_keys(
        tmp_path / "batch_order.db"
    )
    await database.register_strategy(
        strategy_id="order-test",
        version="order-test",
        config_hash="hash",
        config_json={},
        now=NOW,
    )
    candidate = Candidate(
        candidate_id="candidate-order-test",
        mint=MINT,
        pool_address=POOL,
        detected_at=NOW,
        updated_at=NOW,
        strategy_version="order-test",
        config_hash="hash",
    )

    try:
        async with database.event_state_batch_transaction():
            await database.upsert_candidate(candidate, "order-test")
            await database.upsert_pool(
                PoolRecord(
                    pool_address=POOL,
                    protocol="pumpswap",
                    base_mint=MINT,
                    quote_mint=QUOTE,
                    base_decimals=6,
                    quote_decimals=9,
                    base_reserve_raw=Decimal("1000000000"),
                    quote_reserve_raw=Decimal("300000000000"),
                    creation_signature="pool-order-test",
                    creation_slot=1,
                    creation_time=NOW,
                    updated_at=NOW,
                )
            )
            await database.upsert_token(
                TokenRecord(mint=MINT, updated_at=NOW)
            )

        async with database.sessions() as session:
            stored = await session.scalar(
                select(CandidateRow).where(
                    CandidateRow.id == candidate.candidate_id
                )
            )
        assert stored is not None
        assert stored.mint == MINT
        assert stored.pool_address == POOL
    finally:
        await database.close()
