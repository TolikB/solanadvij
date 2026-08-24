from __future__ import annotations

import asyncio
import os
from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal
from typing import Protocol
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection

from sniper_bot.database import ActiveRuntimeError, Database


class _AsyncClosable(Protocol):
    async def close(self) -> None: ...


def _probe_id() -> str:
    return str(uuid4())


def _probe_sha256() -> str:
    return uuid4().hex + uuid4().hex


def _validate_probe_target(
    dsn: str,
    *,
    github_actions: str | None,
    confirmation: str | None,
) -> None:
    if not dsn:
        raise RuntimeError("MIGRATION_POSTGRES_DSN is required")
    if github_actions != "true" or confirmation != "EPHEMERAL_CI_ONLY":
        raise RuntimeError("PostgreSQL hardening probe is restricted to ephemeral GitHub CI")
    url = make_url(dsn)
    if url.database is None or not url.database.endswith("_ci"):
        raise RuntimeError("PostgreSQL hardening probe requires a database ending in _ci")
    if url.host not in {"127.0.0.1", "localhost"}:
        raise RuntimeError("PostgreSQL hardening probe requires a local CI database")


def _validate_race_results(race_run: object, race_cost: object) -> str:
    if isinstance(race_run, BaseException):
        raise RuntimeError("runtime failed during startup/cost race") from race_run
    if not isinstance(race_run, str):
        raise RuntimeError("runtime returned an invalid run identifier")
    if race_cost is True:
        return race_run
    if isinstance(race_cost, RuntimeError) and "stop the bot" in str(race_cost):
        return race_run
    if isinstance(race_cost, BaseException):
        raise RuntimeError("unexpected startup/cost race result") from race_cost
    raise RuntimeError("startup/cost race returned an invalid result")


async def _close_all(*resources: _AsyncClosable) -> None:
    errors: list[BaseException] = []
    for resource in resources:
        try:
            await resource.close()
        except BaseException as exc:
            errors.append(exc)
    if errors:
        raise BaseExceptionGroup("failed to close PostgreSQL probe resources", errors)


async def _must_reject(
    connection: AsyncConnection,
    statement: str,
    parameters: Mapping[str, object],
    expected_error: str,
) -> None:
    savepoint = await connection.begin_nested()
    try:
        await connection.execute(text(statement), parameters)
    except DBAPIError as exc:
        await savepoint.rollback()
        if expected_error not in str(exc):
            raise
    else:
        await savepoint.rollback()
        raise RuntimeError("PostgreSQL append-only trigger accepted a mutation")


async def main() -> None:
    dsn = os.environ.get("MIGRATION_POSTGRES_DSN", "").strip()
    _validate_probe_target(
        dsn,
        github_actions=os.environ.get("GITHUB_ACTIONS"),
        confirmation=os.environ.get("POSTGRES_HARDENING_PROBE_CONFIRM"),
    )
    database = Database(dsn)
    competitor = Database(dsn)
    account_id = _probe_id()
    strategy_id = _probe_id()
    source_hash = _probe_sha256()
    active_source_hash = _probe_sha256()
    race_source_hash = _probe_sha256()
    now = datetime.now(tz=timezone.utc)
    active_run_id: str | None = None
    try:
        await database.acquire_runtime_lease()
        try:
            await competitor.acquire_runtime_lease()
        except ActiveRuntimeError:
            pass
        else:
            raise RuntimeError("PostgreSQL advisory runtime lease was not exclusive")
        async with database.engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    UPDATE system_runs
                    SET stopped_at = :now, stop_reason = 'ci-probe-recovery'
                    WHERE stopped_at IS NULL AND hostname = 'ci' AND app_version = 'ci'
                    """
                ),
                {"now": now},
            )
        await database.initialize_paper_account(
            account_id=account_id,
            starting_equity=Decimal("500"),
            now=now,
        )
        outcomes = await asyncio.gather(
            database.record_operational_cost(
                cost_id=_probe_id(),
                account_id=account_id,
                category="ci",
                amount_usd=Decimal("1"),
                incurred_at=now,
                source_reference_sha256=source_hash,
                recorded_at=now,
            ),
            database.record_operational_cost(
                cost_id=_probe_id(),
                account_id=account_id,
                category="ci",
                amount_usd=Decimal("1"),
                incurred_at=now,
                source_reference_sha256=source_hash,
                recorded_at=now,
            ),
        )
        if sorted(outcomes) != [False, True]:
            raise RuntimeError("PostgreSQL operational-cost idempotency failed")
        await database.register_strategy(
            strategy_id=strategy_id,
            version=strategy_id,
            config_hash="a" * 64,
            config_json={},
            git_commit="b" * 40,
            now=now,
        )
        run_id = await database.start_system_run(
            mode="paper",
            strategy_version_id=strategy_id,
            hostname="ci",
            app_version="ci",
            now=now,
            account_id=account_id,
        )
        active_run_id = run_id
        try:
            await database.start_system_run(
                mode="paper",
                strategy_version_id=strategy_id,
                hostname="ci",
                app_version="ci",
                now=now,
                account_id=account_id,
            )
        except ActiveRuntimeError:
            pass
        else:
            raise RuntimeError("PostgreSQL accepted two active runtimes")
        try:
            await database.record_operational_cost(
                cost_id=_probe_id(),
                account_id=account_id,
                category="ci",
                amount_usd=Decimal("1"),
                incurred_at=now,
                source_reference_sha256=active_source_hash,
                recorded_at=now,
            )
        except RuntimeError as exc:
            if "stop the bot" not in str(exc):
                raise
        else:
            raise RuntimeError("operational cost was recorded during an active runtime")
        await database.stop_system_run(run_id, reason="ci", now=now)
        active_run_id = None
        race_run, race_cost = await asyncio.gather(
            database.start_system_run(
                mode="paper",
                strategy_version_id=strategy_id,
                hostname="ci",
                app_version="ci",
                now=now,
                account_id=account_id,
            ),
            database.record_operational_cost(
                cost_id=_probe_id(),
                account_id=account_id,
                category="ci",
                amount_usd=Decimal("1"),
                incurred_at=now,
                source_reference_sha256=race_source_hash,
                recorded_at=now,
            ),
            return_exceptions=True,
        )
        if isinstance(race_run, str):
            active_run_id = race_run
        validated_race_run = _validate_race_results(race_run, race_cost)
        await database.stop_system_run(validated_race_run, reason="ci", now=now)
        active_run_id = None
        async with database.engine.connect() as connection:
            transaction = await connection.begin()
            try:
                await _must_reject(
                    connection,
                    "UPDATE operational_costs SET amount_usd = 2 WHERE source_reference_sha256 = :source",
                    {"source": source_hash},
                    "append-only",
                )
                await _must_reject(
                    connection,
                    "DELETE FROM operational_costs WHERE source_reference_sha256 = :source",
                    {"source": source_hash},
                    "append-only",
                )
                await _must_reject(
                    connection,
                    "UPDATE strategy_versions SET git_commit = :changed WHERE id = :strategy",
                    {"changed": "c" * 40, "strategy": strategy_id},
                    "append-only",
                )
                await _must_reject(
                    connection,
                    "DELETE FROM strategy_versions WHERE id = :strategy",
                    {"strategy": strategy_id},
                    "append-only",
                )
                await _must_reject(
                    connection,
                    """
                    INSERT INTO operational_costs
                        (id, account_id, category, amount_usd, incurred_at,
                         source_reference_sha256, created_at)
                    VALUES (:id, :account, 'ci', 1, :now, :source, :now)
                    """,
                    {
                        "id": _probe_id(),
                        "account": account_id,
                        "now": now,
                        "source": "z" * 64,
                    },
                    "ck_operational_cost_source_sha256",
                )
            finally:
                await transaction.rollback()
    finally:
        try:
            if active_run_id is not None:
                await database.stop_system_run(
                    active_run_id,
                    reason="ci-probe-cleanup",
                    now=datetime.now(tz=timezone.utc),
                )
        finally:
            await _close_all(competitor, database)
    print("POSTGRES_HARDENING_OK")


if __name__ == "__main__":
    asyncio.run(main())
