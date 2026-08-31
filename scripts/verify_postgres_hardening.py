from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Protocol
from uuid import uuid4

from sqlalchemy import delete, func, select, text
from sqlalchemy import event as sqlalchemy_event
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection

from sniper_bot.clustering import WalletRelation
from sniper_bot.database import ActiveRuntimeError, Database
from sniper_bot.db_models import (
    EventDedupRow,
    RawChainEventRow,
    TokenRow,
    WalletProfileRow,
    WalletRelationRow,
)
from sniper_bot.events import ChainEventType, EventEnvelope, EventSource
from sniper_bot.events import Protocol as EventProtocol
from sniper_bot.registry import TokenRecord
from sniper_bot.wallet_analysis import WalletProfile


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


POSTGRES_EVENT_BATCH_SIZE = 512
POSTGRES_EVENT_BATCH_MAX_SECONDS = 1.0
POSTGRES_EVENT_FAILURE_BATCH_MAX_SECONDS = 1.0
POSTGRES_EVENT_STATE_BATCH_MAX_SECONDS = 1.0


def _event_batch(prefix: str, now: datetime, *, count: int) -> list[EventEnvelope]:
    return [
        EventEnvelope(
            source=EventSource.REPLAY,
            protocol=EventProtocol.PUMPSWAP,
            event_type=ChainEventType.SWAP_BUY,
            slot=10_000 + index,
            signature=f"{prefix}-{index:04d}",
            instruction_index=0,
            block_time=now,
            observed_at=now,
            mint="CI_TOKEN",
            pool_address="CI_POOL",
            payload={"base_amount_out": "1", "quote_amount_in": "1"},
        )
        for index in range(count)
    ]


async def _probe_event_batch(
    database: Database,
    competitor: Database,
    now: datetime,
) -> None:
    batch = _event_batch(
        f"ci-batch-{uuid4().hex}",
        now,
        count=POSTGRES_EVENT_BATCH_SIZE,
    )
    overlap_forward = _event_batch(
        f"ci-overlap-{uuid4().hex}",
        now,
        count=2,
    )
    overlap_reverse = list(reversed(overlap_forward))
    tracked_events = [*batch, *overlap_forward]
    tracked_event_ids = [event.event_id for event in tracked_events]
    statements: list[tuple[str, bool]] = []
    listener_attached = False

    async with database.sessions() as session:
        status_index = await session.scalar(
            text(
                "SELECT pg_catalog.to_regclass("
                "'public.ix_event_dedup_processing_status'"
                ")"
            )
        )
        event_dedup_options = await session.scalar(
            text(
                "SELECT reloptions FROM pg_catalog.pg_class "
                "WHERE oid = 'public.event_dedup'::regclass"
            )
        )
    if status_index is not None:
        raise RuntimeError(
            "PostgreSQL event completion still maintains the hot status index"
        )
    if "fillfactor=70" not in set(event_dedup_options or []):
        raise RuntimeError(
            "PostgreSQL event_dedup fillfactor is not tuned for HOT updates"
        )

    def capture_sql(
        _connection: Any,
        _cursor: Any,
        statement: str,
        _parameters: Any,
        _context: Any,
        executemany: bool,
    ) -> None:
        statements.append((" ".join(statement.lower().split()), executemany))

    try:
        sqlalchemy_event.listen(
            database.engine.sync_engine,
            "before_cursor_execute",
            capture_sql,
        )
        listener_attached = True
        started = time.perf_counter()
        try:
            accepted = await database.record_events(batch)
        finally:
            sqlalchemy_event.remove(
                database.engine.sync_engine,
                "before_cursor_execute",
                capture_sql,
            )
            listener_attached = False
        elapsed = time.perf_counter() - started

        claim_inserts = [
            item
            for item in statements
            if item[0].startswith("insert into event_dedup")
            and "from pg_temp.sniper_event_ingest_stage as payload" in item[0]
        ]
        raw_sql_inserts = [
            item
            for item in statements
            if "insert into raw_chain_events" in item[0]
        ]
        staging_creates = [
            item
            for item in statements
            if item[0].startswith(
                "create temp table if not exists sniper_event_ingest_stage"
            )
        ]
        staging_truncates = [
            item
            for item in statements
            if item[0].startswith(
                "truncate table pg_temp.sniper_event_ingest_stage"
            )
        ]
        partition_ensures = [
            item
            for item in statements
            if item[0].startswith(
                "select public.ensure_raw_chain_events_partition"
            )
        ]
        sequence_allocations = [
            item
            for item in statements
            if "raw_chain_events_ingest_sequence_seq" in item[0]
        ]
        if accepted != [True] * len(batch):
            raise RuntimeError("PostgreSQL bulk event probe rejected a new event")
        if (
            len(statements) != 5
            or len(partition_ensures) != 1
            or len(staging_creates) != 1
            or len(staging_truncates) != 1
            or len(claim_inserts) != 1
            or len(raw_sql_inserts) != 0
            or len(sequence_allocations) != 1
        ):
            raise RuntimeError(
                "PostgreSQL event batch did not use transaction-local "
                "claim staging with direct raw COPY"
            )
        if any(executemany for _, executemany in statements):
            raise RuntimeError("PostgreSQL event batch unexpectedly used executemany")
        claim_sql = claim_inserts[0][0]
        required_claim_fragments = (
            "from pg_temp.sniper_event_ingest_stage as payload",
            "insert into event_dedup",
            "order by payload.event_id",
            "on conflict (event_id) do nothing",
            "returning event_id",
        )
        if any(
            fragment not in claim_sql
            for fragment in required_claim_fragments
        ) or "jsonb_to_recordset" in claim_sql:
            raise RuntimeError(
                "PostgreSQL event claim insert lost COPY staging or "
                "canonical ordering"
            )
        sequence_sql = sequence_allocations[0][0]
        required_sequence_fragments = (
            "select nextval(",
            "raw_chain_events_ingest_sequence_seq",
            "from generate_series(",
            "order by ordered.batch_order",
        )
        if any(
            fragment not in sequence_sql
            for fragment in required_sequence_fragments
        ):
            raise RuntimeError(
                "PostgreSQL raw COPY lost ordered ingest sequencing"
            )
        if "on commit delete rows" not in staging_creates[0][0]:
            raise RuntimeError(
                "PostgreSQL event staging is not transaction-local"
            )
        if elapsed > POSTGRES_EVENT_BATCH_MAX_SECONDS:
            raise RuntimeError(
                "PostgreSQL event batch exceeded the one-second performance budget"
            )

        async with database.sessions() as session:
            persisted = list(
                (
                    await session.execute(
                        select(
                            RawChainEventRow.signature,
                            RawChainEventRow.ingest_sequence,
                        )
                        .where(RawChainEventRow.event_id.in_(tracked_event_ids))
                        .order_by(RawChainEventRow.ingest_sequence)
                    )
                ).all()
            )
            partition_names = {
                str(name)
                for name in (
                    await session.scalars(
                        text(
                            "SELECT DISTINCT tableoid::regclass::text "
                            "FROM public.raw_chain_events "
                            "WHERE event_id::text = "
                            "ANY(CAST(:event_ids AS text[]))"
                        ),
                        {"event_ids": tracked_event_ids},
                    )
                ).all()
            }
            default_rows = int(
                (
                    await session.scalar(
                        text(
                            "SELECT count(*) FROM ONLY "
                            "public.raw_chain_events_default"
                        )
                    )
                )
                or 0
            )
        if [row.signature for row in persisted] != [
            event.signature for event in batch
        ]:
            raise RuntimeError("PostgreSQL event batch changed durable ingest order")
        if len({row.ingest_sequence for row in persisted}) != len(batch):
            raise RuntimeError("PostgreSQL event batch reused an ingest sequence")
        expected_partition = f"raw_chain_events_{now:%Y%m%d}"
        if partition_names != {expected_partition} or default_rows != 0:
            raise RuntimeError(
                "PostgreSQL event batch did not route into its daily partition"
            )

        race_results = await asyncio.wait_for(
            asyncio.gather(
                database.record_events(overlap_forward),
                competitor.record_events(overlap_reverse),
            ),
            timeout=5,
        )
        if sorted(race_results) != [[False, False], [True, True]]:
            raise RuntimeError(
                "PostgreSQL reversed-overlap event race was not exactly-once"
            )
        async with database.sessions() as session:
            overlap_count = int(
                (
                    await session.scalar(
                        select(func.count())
                        .select_from(RawChainEventRow)
                        .where(
                            RawChainEventRow.event_id.in_(
                                [event.event_id for event in overlap_forward]
                            )
                        )
                    )
                )
                or 0
            )
        if overlap_count != len(overlap_forward):
            raise RuntimeError(
                "PostgreSQL reversed-overlap event race persisted duplicates"
            )
        print(
            "POSTGRES_EVENT_BATCH_OK "
            f"rows={len(batch)} statements={len(statements)} "
            f"elapsed_ms={elapsed * 1000:.3f} partition={expected_partition}"
        )
    finally:
        if listener_attached:
            sqlalchemy_event.remove(
                database.engine.sync_engine,
                "before_cursor_execute",
                capture_sql,
            )
        for event in tracked_events:
            database.release_event_claim(event.event_id)
            competitor.release_event_claim(event.event_id)
        async with database.sessions.begin() as session:
            await session.execute(
                delete(RawChainEventRow).where(
                    RawChainEventRow.event_id.in_(tracked_event_ids)
                )
            )
            await session.execute(
                delete(EventDedupRow).where(
                    EventDedupRow.event_id.in_(tracked_event_ids)
                )
            )


async def _probe_event_failure_batch(
    database: Database,
    competitor: Database,
    now: datetime,
) -> None:
    failure_batch = _event_batch(
        f"ci-failure-batch-{uuid4().hex}",
        now,
        count=POSTGRES_EVENT_BATCH_SIZE,
    )
    overlap = _event_batch(
        f"ci-failure-overlap-{uuid4().hex}",
        now,
        count=2,
    )
    tracked_events = [*failure_batch, *overlap]
    tracked_event_ids = [event.event_id for event in tracked_events]
    statements: list[tuple[str, bool]] = []
    lock_orders: list[tuple[str, ...]] = []
    listeners: list[tuple[Any, Any]] = []

    def capture_failure_sql(
        _connection: Any,
        _cursor: Any,
        statement: str,
        parameters: Any,
        _context: Any,
        executemany: bool,
    ) -> None:
        normalized = " ".join(statement.lower().split())
        statements.append((normalized, executemany))

    def capture_lock_order(
        _connection: Any,
        _cursor: Any,
        statement: str,
        parameters: Any,
        _context: Any,
        _executemany: bool,
    ) -> None:
        normalized = " ".join(statement.lower().split())
        if (
            normalized.startswith("select event_dedup.event_id")
            and "order by event_dedup.event_id" in normalized
            and "for update" in normalized
        ):
            lock_orders.append(tuple(str(value) for value in parameters))

    try:
        accepted = await database.record_events(failure_batch)
        if accepted != [True] * len(failure_batch):
            raise RuntimeError(
                "PostgreSQL failure-batch probe rejected a new event"
            )

        sqlalchemy_event.listen(
            database.engine.sync_engine,
            "before_cursor_execute",
            capture_failure_sql,
        )
        listeners.append((database.engine.sync_engine, capture_failure_sql))
        started = time.perf_counter()
        await database.mark_events_failed(
            [event.event_id for event in reversed(failure_batch)],
            RuntimeError("ci failure batch"),
        )
        elapsed = time.perf_counter() - started
        sqlalchemy_event.remove(
            database.engine.sync_engine,
            "before_cursor_execute",
            capture_failure_sql,
        )
        listeners.clear()

        lock_selects = [
            statement
            for statement, _ in statements
            if statement.startswith("select event_dedup.event_id")
            and "order by event_dedup.event_id" in statement
            and "for update" in statement
        ]
        failure_updates = [
            statement
            for statement, _ in statements
            if statement.startswith("update event_dedup set")
        ]
        if (
            len(statements) != 2
            or len(lock_selects) != 1
            or len(failure_updates) != 1
        ):
            raise RuntimeError(
                "PostgreSQL event failure batch did not use the required "
                "two-statement shape"
            )
        if any(executemany for _, executemany in statements):
            raise RuntimeError(
                "PostgreSQL event failure batch unexpectedly used executemany"
            )
        if elapsed > POSTGRES_EVENT_FAILURE_BATCH_MAX_SECONDS:
            raise RuntimeError(
                "PostgreSQL event failure batch exceeded the one-second "
                "performance budget"
            )

        accepted = await database.record_events(overlap)
        if accepted != [True] * len(overlap):
            raise RuntimeError(
                "PostgreSQL failure-overlap probe rejected a new event"
            )
        overlap_ids = [event.event_id for event in overlap]
        competitor._event_claim_tokens.update(
            {
                event_id: database._event_claim_tokens[event_id]
                for event_id in overlap_ids
            }
        )
        for engine in (
            database.engine.sync_engine,
            competitor.engine.sync_engine,
        ):
            sqlalchemy_event.listen(
                engine,
                "before_cursor_execute",
                capture_lock_order,
            )
            listeners.append((engine, capture_lock_order))

        race_results = await asyncio.wait_for(
            asyncio.gather(
                database.mark_events_failed(
                    overlap_ids,
                    RuntimeError("ci overlap failure"),
                ),
                competitor.mark_events_failed(
                    list(reversed(overlap_ids)),
                    RuntimeError("ci overlap failure"),
                ),
                return_exceptions=True,
            ),
            timeout=5,
        )
        canonical_order = tuple(sorted(overlap_ids))
        if (
            sum(result is None for result in race_results) != 1
            or sum(
                isinstance(result, RuntimeError)
                and "superseded" in str(result)
                for result in race_results
            )
            != 1
        ):
            raise RuntimeError(
                "PostgreSQL failure-overlap race did not resolve exactly once"
            )
        if len(lock_orders) != 2 or any(
            order != canonical_order for order in lock_orders
        ):
            raise RuntimeError(
                "PostgreSQL failure-overlap locks were not canonical"
            )

        async with database.sessions() as session:
            rows = list(
                (
                    await session.execute(
                        select(
                            EventDedupRow.processing_status,
                            EventDedupRow.processing_token,
                        ).where(EventDedupRow.event_id.in_(tracked_event_ids))
                    )
                ).all()
            )
        if len(rows) != len(tracked_events):
            raise RuntimeError(
                "PostgreSQL event failure probe lost durable claims"
            )
        if any(
            row.processing_status != "FAILED"
            or row.processing_token is not None
            for row in rows
        ):
            raise RuntimeError(
                "PostgreSQL event failure probe left unresolved claims"
            )
        print(
            "POSTGRES_EVENT_FAILURE_BATCH_OK "
            f"rows={len(failure_batch)} statements={len(statements)} "
            f"elapsed_ms={elapsed * 1000:.3f} lock_race=resolved"
        )
    finally:
        for engine, listener in listeners:
            sqlalchemy_event.remove(
                engine,
                "before_cursor_execute",
                listener,
            )
        for event_id in tracked_event_ids:
            database.release_event_claim(event_id)
            competitor.release_event_claim(event_id)
        async with database.sessions.begin() as session:
            await session.execute(
                delete(RawChainEventRow).where(
                    RawChainEventRow.event_id.in_(tracked_event_ids)
                )
            )
            await session.execute(
                delete(EventDedupRow).where(
                    EventDedupRow.event_id.in_(tracked_event_ids)
                )
            )


async def _probe_event_state_batch(
    database: Database,
    now: datetime,
) -> None:
    batch = _event_batch(
        f"ci-state-batch-{uuid4().hex}",
        now,
        count=POSTGRES_EVENT_BATCH_SIZE,
    )
    event_ids = [event.event_id for event in batch]
    token_mint = f"CI{uuid4().hex}"
    wallet_prefix = f"CI{uuid4().hex}"
    wallet_addresses = [
        f"{wallet_prefix}{index:04x}" for index in range(len(batch))
    ]
    relation_anchor = f"CI{uuid4().hex}A"
    context_pool = f"CI{uuid4().hex}P"
    statements: list[tuple[str, bool]] = []
    listener_attached = False

    def capture_state_sql(
        _connection: Any,
        _cursor: Any,
        statement: str,
        _parameters: Any,
        _context: Any,
        executemany: bool,
    ) -> None:
        statements.append((" ".join(statement.lower().split()), executemany))

    try:
        accepted = await database.record_events(batch)
        if accepted != [True] * len(batch):
            raise RuntimeError(
                "PostgreSQL event-state batch probe rejected a new event"
            )

        sqlalchemy_event.listen(
            database.engine.sync_engine,
            "before_cursor_execute",
            capture_state_sql,
        )
        listener_attached = True
        started = time.perf_counter()
        try:
            async with database.event_state_batch_transaction():
                for index, event in enumerate(batch):
                    await database.update_raw_event_context(
                        event.model_copy(
                            update={
                                "mint": token_mint,
                                "pool_address": context_pool,
                            }
                        )
                    )
                    await database.upsert_token(
                        TokenRecord(
                            mint=token_mint,
                            creation_time=now,
                            updated_at=now,
                        )
                    )
                    await database.upsert_wallet_profile(
                        WalletProfile(
                            wallet_address=wallet_addresses[index],
                            first_seen_at=now,
                            tokens_traded=index,
                            median_peak_return=Decimal("1.25"),
                            profile_updated_at=now,
                        )
                    )
                    await database.upsert_wallet_relation(
                        WalletRelation(
                            wallet_a=relation_anchor,
                            wallet_b=wallet_addresses[index],
                            relation_score=Decimal("0.8"),
                            evidence=["same_slot_buying"],
                            eligible_for_cluster=True,
                        ),
                        now,
                    )
                    await database.mark_event_processed(
                        event.event_id,
                        processed_at=now,
                    )
        finally:
            sqlalchemy_event.remove(
                database.engine.sync_engine,
                "before_cursor_execute",
                capture_state_sql,
            )
            listener_attached = False
        elapsed = time.perf_counter() - started

        set_local = [
            statement
            for statement, _ in statements
            if statement.startswith("set local synchronous_commit")
        ]
        token_inserts = [
            statement
            for statement, _ in statements
            if statement.startswith("insert into tokens")
        ]
        raw_context_updates = [
            statement
            for statement, _ in statements
            if statement.startswith("with requested as materialized")
            and "update raw_chain_events as raw" in statement
        ]
        wallet_profile_inserts = [
            statement
            for statement, _ in statements
            if statement.startswith("insert into wallet_profiles")
        ]
        wallet_relation_inserts = [
            statement
            for statement, _ in statements
            if statement.startswith("insert into wallet_relations")
        ]
        claim_completions = [
            statement
            for statement, _ in statements
            if statement.startswith("with requested as materialized")
            and "update event_dedup as claim" in statement
        ]
        if (
            len(statements) != 6
            or len(set_local) != 1
            or len(raw_context_updates) != 1
            or len(token_inserts) != 1
            or len(wallet_profile_inserts) != 1
            or len(wallet_relation_inserts) != 1
            or len(claim_completions) != 1
        ):
            raise RuntimeError(
                "PostgreSQL event-state batch did not use the required "
                "six-statement bulk shape"
            )
        raw_context_sql = raw_context_updates[0]
        required_raw_context_fragments = (
            "from jsonb_to_recordset(",
            "requested.block_date = raw.block_date",
            "order by raw.event_id, raw.block_date",
            "for update of raw",
            "raw.mint is distinct from requested.mint",
            "raw.pool_address is distinct from requested.pool_address",
            "returning raw.event_id",
            "select event_id from locked order by event_id",
        )
        if any(
            fragment not in raw_context_sql
            for fragment in required_raw_context_fragments
        ):
            raise RuntimeError(
                "PostgreSQL raw-context update lost JSON bulk binding or "
                "canonical locking"
            )
        required_profile_fragments = (
            "from jsonb_populate_recordset(",
            "cast(null as wallet_profiles)",
            "on conflict (wallet_address) do update",
        )
        if any(
            fragment not in wallet_profile_inserts[0]
            for fragment in required_profile_fragments
        ):
            raise RuntimeError(
                "PostgreSQL wallet-profile upsert lost JSON bulk binding"
            )
        required_relation_fragments = (
            "from jsonb_populate_recordset(",
            "cast(null as wallet_relations)",
            "on conflict (wallet_a, wallet_b) do update",
        )
        if any(
            fragment not in wallet_relation_inserts[0]
            for fragment in required_relation_fragments
        ):
            raise RuntimeError(
                "PostgreSQL wallet-relation upsert lost JSON bulk binding"
            )
        completion_sql = claim_completions[0]
        required_completion_fragments = (
            "from jsonb_to_recordset(",
            "order by claim.event_id",
            "for update of claim",
            "claim.processing_token = requested_claim.processing_token",
            "returning claim.event_id",
        )
        if any(
            fragment not in completion_sql
            for fragment in required_completion_fragments
        ):
            raise RuntimeError(
                "PostgreSQL event-state batch completion lost JSON bulk "
                "binding, canonical locking, or claim-token fencing"
            )
        if any(executemany for _, executemany in statements):
            raise RuntimeError(
                "PostgreSQL event-state batch unexpectedly used executemany"
            )
        if elapsed > POSTGRES_EVENT_STATE_BATCH_MAX_SECONDS:
            raise RuntimeError(
                "PostgreSQL event-state batch exceeded the one-second "
                "performance budget"
            )

        async with database.sessions() as session:
            claims = list(
                (
                    await session.execute(
                        select(
                            EventDedupRow.processing_status,
                            EventDedupRow.processing_token,
                        ).where(EventDedupRow.event_id.in_(event_ids))
                    )
                ).all()
            )
            raw_contexts = list(
                (
                    await session.execute(
                        select(
                            RawChainEventRow.event_id,
                            RawChainEventRow.mint,
                            RawChainEventRow.pool_address,
                        ).where(RawChainEventRow.event_id.in_(event_ids))
                    )
                ).all()
            )
            token = await session.get(TokenRow, token_mint)
            profiles = list(
                (
                    await session.execute(
                        select(WalletProfileRow).where(
                            WalletProfileRow.wallet_address.in_(
                                wallet_addresses
                            )
                        )
                    )
                ).scalars()
            )
            relations = list(
                (
                    await session.execute(
                        select(WalletRelationRow).where(
                            WalletRelationRow.wallet_a == relation_anchor
                        )
                    )
                ).scalars()
            )
        if len(claims) != len(batch):
            raise RuntimeError(
                "PostgreSQL event-state batch lost durable claims"
            )
        if any(
            claim.processing_status != "PROCESSED"
            or claim.processing_token is not None
            for claim in claims
        ):
            raise RuntimeError(
                "PostgreSQL event-state batch left unresolved claims"
            )
        if (
            len(raw_contexts) != len(batch)
            or any(
                raw_context.mint != token_mint
                or raw_context.pool_address != context_pool
                for raw_context in raw_contexts
            )
        ):
            raise RuntimeError(
                "PostgreSQL event-state batch lost changed raw context"
            )
        if token is None:
            raise RuntimeError(
                "PostgreSQL event-state batch lost the coalesced token upsert"
            )
        if (
            len(profiles) != len(batch)
            or any(
                profile.median_peak_return != Decimal("1.25")
                for profile in profiles
            )
        ):
            raise RuntimeError(
                "PostgreSQL event-state batch lost wallet profiles"
            )
        if (
            len(relations) != len(batch)
            or any(
                relation.relation_score != Decimal("0.8")
                or relation.relation_types_json != ["same_slot_buying"]
                for relation in relations
            )
        ):
            raise RuntimeError(
                "PostgreSQL event-state batch lost wallet relations"
            )
        print(
            "POSTGRES_EVENT_STATE_BATCH_OK "
            f"rows={len(batch)} statements={len(statements)} "
            f"elapsed_ms={elapsed * 1000:.3f}"
        )
    finally:
        if listener_attached:
            sqlalchemy_event.remove(
                database.engine.sync_engine,
                "before_cursor_execute",
                capture_state_sql,
            )
        for event_id in event_ids:
            database.release_event_claim(event_id)
        async with database.sessions.begin() as session:
            await session.execute(
                delete(RawChainEventRow).where(
                    RawChainEventRow.event_id.in_(event_ids)
                )
            )
            await session.execute(
                delete(EventDedupRow).where(
                    EventDedupRow.event_id.in_(event_ids)
                )
            )
            await session.execute(
                delete(TokenRow).where(TokenRow.mint == token_mint)
            )
            await session.execute(
                delete(WalletRelationRow).where(
                    WalletRelationRow.wallet_a == relation_anchor
                )
            )
            await session.execute(
                delete(WalletProfileRow).where(
                    WalletProfileRow.wallet_address.in_(wallet_addresses)
                )
            )


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
        await _probe_event_batch(database, competitor, now)
        await _probe_event_failure_batch(database, competitor, now)
        await _probe_event_state_batch(database, now)
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
