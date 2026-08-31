"""Async persistence facade with race-safe event and outbox idempotency."""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import and_, case, delete, func, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from .candidates import Candidate
from .clustering import WalletRelation
from .db_models import (
    Base,
    CandidateRow,
    DailyReportRow,
    EventDedupRow,
    ExternalApiCallRow,
    MarketSnapshotRow,
    OperationalCostRow,
    OutboxEventRow,
    PaperAccountRow,
    PaperEquityMarkRow,
    PaperFillRow,
    PaperOrderRow,
    PaperPositionRow,
    PoolRow,
    RawChainEventRow,
    ReplayRunRow,
    RiskEventRow,
    RuntimeCheckpointRow,
    SignalEvaluationRow,
    StrategyVersionRow,
    SystemRunRow,
    TokenRow,
    TokenSecurityCheckRow,
    WalletProfileRow,
    WalletRelationRow,
)
from .events import EventEnvelope
from .features import FeatureSnapshot
from .metrics import BotMetrics
from .models import QuoteResponse
from .registry import PoolRecord, TokenRecord
from .scoring import ScoreBreakdown
from .security import SecurityContext, SecurityResult
from .wallet_analysis import WalletProfile


class ActiveRuntimeError(RuntimeError):
    """Raised when another healthy runtime already owns the paper account."""


RUNTIME_ADVISORY_LOCK_KEY = 0x534E49504552
RUNTIME_ADVISORY_LOCK_CLASS_ID = 21326
RUNTIME_ADVISORY_LOCK_OBJECT_ID = 1229997394
MAX_EVENT_PROCESSING_ATTEMPTS = 3
MAX_EVENT_BATCH_SIZE = 1024
SQLITE_SAFE_BOUND_PARAMETER_BUDGET = 900
POSTGRES_SAFE_BOUND_PARAMETER_BUDGET = 30_000

logger = logging.getLogger(__name__)


def _parameter_budget(dialect: str) -> int:
    return (
        SQLITE_SAFE_BOUND_PARAMETER_BUDGET
        if dialect == "sqlite"
        else POSTGRES_SAFE_BOUND_PARAMETER_BUDGET
    )


def _bulk_insert_chunks(
    rows: list[dict[str, Any]], *, dialect: str
) -> list[list[dict[str, Any]]]:
    """Bound multi-row statements below each driver's parameter ceiling."""
    if not rows:
        return []
    chunk_size = max(1, _parameter_budget(dialect) // len(rows[0]))
    return [
        rows[offset : offset + chunk_size]
        for offset in range(0, len(rows), chunk_size)
    ]


def _event_id_chunks(
    event_ids: list[str],
    *,
    dialect: str,
    parameters_per_id: int = 1,
    reserved_parameters: int = 0,
) -> list[list[str]]:
    if not event_ids:
        return []
    if parameters_per_id <= 0 or reserved_parameters < 0:
        raise ValueError("event-id parameter widths must be positive")
    available_parameters = _parameter_budget(dialect) - reserved_parameters
    if available_parameters < parameters_per_id:
        raise ValueError("database parameter budget is too small for one event id")
    chunk_size = available_parameters // parameters_per_id
    return [
        event_ids[offset : offset + chunk_size]
        for offset in range(0, len(event_ids), chunk_size)
    ]


async def _ensure_raw_event_partitions(
    session: AsyncSession,
    block_dates: set[date],
) -> None:
    """Use the migration-owned narrow DDL capability before durable inserts."""
    for block_date in sorted(block_dates):
        await session.execute(
            text("SELECT public.ensure_raw_chain_events_partition(:block_date)"),
            {"block_date": block_date},
        )


def _processed_prefix_condition() -> Any:
    first_unresolved = (
        select(func.min(RawChainEventRow.ingest_sequence))
        .join(EventDedupRow, EventDedupRow.event_id == RawChainEventRow.event_id)
        .where(EventDedupRow.processing_status != "PROCESSED")
        .correlate(None)
        .scalar_subquery()
    )
    return first_unresolved.is_(None) | (
        RawChainEventRow.ingest_sequence < first_unresolved
    )


@dataclass(slots=True)
class _PendingUpsertGroup:
    model: Any
    keys: tuple[str, ...]
    rows: dict[tuple[Any, ...], dict[str, Any]] = field(
        default_factory=dict
    )


@dataclass(slots=True)
class _EventStateBatch:
    raw_contexts: dict[str, EventEnvelope] = field(default_factory=dict)
    processed_claims: dict[str, tuple[str, datetime]] = field(
        default_factory=dict
    )
    upsert_groups: dict[
        tuple[Any, tuple[str, ...]],
        _PendingUpsertGroup,
    ] = field(default_factory=dict)

class Database:
    def __init__(self, dsn: str, *, metrics: BotMetrics | None = None) -> None:
        self.dsn = _async_dsn(dsn)
        self.engine: AsyncEngine = create_async_engine(
            self.dsn,
            pool_pre_ping=True,
            pool_recycle=300,
        )
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        self.metrics = metrics
        self._event_claim_tokens: dict[str, str] = {}
        self._event_batch_lock = asyncio.Lock()
        self._system_run_lock = asyncio.Lock()
        self._runtime_lease_connection: AsyncConnection | None = None
        self._owned_system_run_id: str | None = None
        self._event_write_session: ContextVar[AsyncSession | None] = ContextVar(
            f"event_write_session_{id(self)}",
            default=None,
        )
        self._event_state_batch: ContextVar[_EventStateBatch | None] = (
            ContextVar(
                f"event_state_batch_{id(self)}",
                default=None,
            )
        )

    @asynccontextmanager
    async def event_state_transaction(self) -> AsyncIterator[None]:
        """Commit all secondary state changes for one decoded event atomically."""
        if self._event_write_session.get() is not None:
            raise RuntimeError("event state transactions must not be nested")
        async with self.sessions.begin() as session:
            if session.bind is not None and session.bind.dialect.name == "postgresql":
                await session.execute(text("SET LOCAL synchronous_commit TO OFF"))
            token = self._event_write_session.set(session)
            try:
                yield
            finally:
                self._event_write_session.reset(token)

    @asynccontextmanager
    async def event_state_batch_transaction(self) -> AsyncIterator[None]:
        """Commit one ordered batch with coalesced secondary-state writes."""
        if (
            self._event_write_session.get() is not None
            or self._event_state_batch.get() is not None
        ):
            raise RuntimeError("event state transactions must not be nested")
        batch = _EventStateBatch()
        async with self.sessions.begin() as session:
            if (
                session.bind is not None
                and session.bind.dialect.name == "postgresql"
            ):
                await session.execute(
                    text("SET LOCAL synchronous_commit TO OFF")
                )
            session_token = self._event_write_session.set(session)
            batch_token = self._event_state_batch.set(batch)
            try:
                yield
                await self._flush_event_state_batch(session, batch)
            finally:
                self._event_state_batch.reset(batch_token)
                self._event_write_session.reset(session_token)

    def _stage_upsert(
        self,
        model: Any,
        values: dict[str, Any],
        keys: list[str],
    ) -> None:
        batch = self._event_state_batch.get()
        if batch is None:
            raise RuntimeError("event state batch is unavailable")
        key_tuple = tuple(keys)
        group_key = (model, key_tuple)
        group = batch.upsert_groups.get(group_key)
        if group is None:
            group = _PendingUpsertGroup(model=model, keys=key_tuple)
            batch.upsert_groups[group_key] = group
        identity = tuple(values[key] for key in key_tuple)
        group.rows[identity] = dict(values)

    async def _flush_event_state_batch(
        self,
        session: AsyncSession,
        batch: _EventStateBatch,
    ) -> None:
        started = time.perf_counter()
        dialect = (
            session.bind.dialect.name
            if session.bind is not None
            else ""
        )
        if dialect not in {"postgresql", "sqlite"}:
            raise RuntimeError(
                f"unsupported event-state batch dialect: "
                f"{dialect or 'unknown'}"
            )
        raw_event_ids = sorted(batch.raw_contexts)
        for event_id_chunk in _event_id_chunks(
            raw_event_ids,
            dialect=dialect,
            parameters_per_id=6,
        ):
            events = [
                batch.raw_contexts[event_id]
                for event_id in event_id_chunk
            ]
            result = await session.execute(
                update(RawChainEventRow)
                .where(
                    or_(
                        *[
                            and_(
                                RawChainEventRow.event_id
                                == event.event_id,
                                RawChainEventRow.block_date
                                == event.block_time.date(),
                            )
                            for event in events
                        ]
                    )
                )
                .values(
                    mint=case(
                        {
                            event.event_id: event.mint
                            for event in events
                        },
                        value=RawChainEventRow.event_id,
                    ),
                    pool_address=case(
                        {
                            event.event_id: event.pool_address
                            for event in events
                        },
                        value=RawChainEventRow.event_id,
                    ),
                )
            )
            if getattr(result, "rowcount", None) != len(events):
                raise RuntimeError(
                    "raw event batch context update affected "
                    f"{getattr(result, 'rowcount', None)} rows"
                )
        for group in batch.upsert_groups.values():
            await self._execute_upsert_rows(
                session,
                group.model,
                list(group.rows.values()),
                group.keys,
            )
        await self._execute_mark_events_processed(
            session,
            batch.processed_claims,
        )
        self._observe_query(started)

    async def _execute_mark_events_processed(
        self,
        session: AsyncSession,
        claims: dict[str, tuple[str, datetime]],
    ) -> None:
        if not claims:
            return
        dialect = (
            session.bind.dialect.name
            if session.bind is not None
            else ""
        )
        if dialect not in {"postgresql", "sqlite"}:
            raise RuntimeError(
                f"unsupported processed-event database dialect: "
                f"{dialect or 'unknown'}"
            )
        event_ids = sorted(claims)
        for event_id_chunk in _event_id_chunks(
            event_ids,
            dialect=dialect,
            parameters_per_id=3,
            reserved_parameters=4,
        ):
            rows = (
                await session.execute(
                    select(
                        EventDedupRow.event_id,
                        EventDedupRow.processing_status,
                        EventDedupRow.processing_token,
                    )
                    .where(
                        EventDedupRow.event_id.in_(event_id_chunk)
                    )
                    .order_by(EventDedupRow.event_id)
                    .with_for_update()
                )
            ).all()
            durable_claims = {
                str(event_id): (
                    str(processing_status),
                    (
                        str(processing_token)
                        if processing_token is not None
                        else None
                    ),
                )
                for event_id, processing_status, processing_token in rows
            }
            if set(durable_claims) != set(event_id_chunk) or any(
                durable_claims[event_id]
                != ("PROCESSING", claims[event_id][0])
                for event_id in event_id_chunk
            ):
                raise RuntimeError(
                    "event processing claim was superseded"
                )
            processed_at = max(
                claims[event_id][1]
                for event_id in event_id_chunk
            )
            predicates = [
                and_(
                    EventDedupRow.event_id == event_id,
                    EventDedupRow.processing_status == "PROCESSING",
                    EventDedupRow.processing_token
                    == claims[event_id][0],
                )
                for event_id in event_id_chunk
            ]
            result = await session.execute(
                update(EventDedupRow)
                .where(or_(*predicates))
                .values(
                    processing_status="PROCESSED",
                    processed_at=processed_at,
                    last_attempt_at=processed_at,
                    last_error=None,
                    processing_token=None,
                )
            )
            if (
                getattr(result, "rowcount", None)
                != len(event_id_chunk)
            ):
                raise RuntimeError(
                    "event processing claim was superseded"
                )
    @asynccontextmanager
    async def _write_session(self) -> AsyncIterator[AsyncSession]:
        active = self._event_write_session.get()
        if active is not None:
            yield active
            return
        async with self.sessions.begin() as session:
            yield session

    def release_event_claim(self, event_id: str) -> None:
        """Forget an in-memory claim only after its outer transaction committed."""
        self._event_claim_tokens.pop(event_id, None)

    async def create_schema_for_tests(self) -> None:
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    async def ping(self) -> bool:
        started = time.perf_counter()
        try:
            async with self.engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
            return True
        finally:
            self._observe_query(started)

    async def close(self) -> None:
        try:
            await self.release_runtime_lease()
        finally:
            await self.engine.dispose()

    async def acquire_runtime_lease(self) -> None:
        if self._runtime_lease_connection is not None:
            raise ActiveRuntimeError("runtime lease is already acquired")
        if self.engine.dialect.name != "postgresql":
            return
        connection = await self.engine.connect()
        try:
            acquired = await connection.scalar(
                text(f"SELECT pg_try_advisory_lock({RUNTIME_ADVISORY_LOCK_KEY})")
            )
            if acquired is not True:
                raise ActiveRuntimeError("another runtime owns the PostgreSQL lease")
            await connection.commit()
        except Exception:
            await connection.close()
            raise
        self._runtime_lease_connection = connection

    async def assert_runtime_lease(self) -> None:
        if self.engine.dialect.name != "postgresql":
            return
        connection = self._runtime_lease_connection
        if connection is None:
            raise ActiveRuntimeError("PostgreSQL runtime lease is not held")
        try:
            lock_count = await connection.scalar(
                text(
                    "SELECT count(*) FROM pg_locks "
                    "WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND granted "
                    f"AND classid = {RUNTIME_ADVISORY_LOCK_CLASS_ID}::oid "
                    f"AND objid = {RUNTIME_ADVISORY_LOCK_OBJECT_ID}::oid AND objsubid = 1"
                )
            )
            await connection.commit()
        except Exception as exc:
            raise ActiveRuntimeError("PostgreSQL runtime lease connection was lost") from exc
        if int(lock_count or 0) < 1:
            raise ActiveRuntimeError("PostgreSQL runtime lease was lost")

    async def release_runtime_lease(self) -> None:
        connection = self._runtime_lease_connection
        self._runtime_lease_connection = None
        if connection is None:
            return
        try:
            released = await connection.scalar(
                text(f"SELECT pg_advisory_unlock({RUNTIME_ADVISORY_LOCK_KEY})")
            )
            await connection.commit()
            if released is not True:
                raise ActiveRuntimeError("PostgreSQL runtime lease was not owned at release")
        finally:
            await connection.close()

    async def register_strategy(
        self,
        *,
        strategy_id: str,
        version: str,
        config_hash: str,
        config_json: dict[str, Any],
        now: datetime,
        git_commit: str | None = None,
    ) -> None:
        async with self.sessions.begin() as session:
            existing = await session.get(StrategyVersionRow, strategy_id)
            if existing is None:
                session.add(
                    StrategyVersionRow(
                        id=strategy_id,
                        version=version,
                        config_hash=config_hash,
                        config_json=config_json,
                        git_commit=git_commit,
                        created_at=now,
                        activated_at=now,
                    )
                )
            elif (
                existing.config_hash != config_hash
                or existing.version != version
                or existing.git_commit != git_commit
            ):
                raise ValueError("strategy version rows are immutable")

    async def start_system_run(
        self,
        *,
        mode: str,
        strategy_version_id: str,
        hostname: str,
        app_version: str,
        now: datetime,
        account_id: str | None = None,
        stale_after: timedelta = timedelta(minutes=2),
    ) -> str:
        if now.tzinfo is None:
            raise ValueError("system run timestamp must be timezone-aware")
        if stale_after <= timedelta(0):
            raise ValueError("system run stale interval must be positive")
        if self.engine.dialect.name == "postgresql":
            await self.assert_runtime_lease()
        run_id = str(uuid4())
        async with self._system_run_lock:
            if self._owned_system_run_id is not None:
                raise ActiveRuntimeError("this runtime already owns an active system run")
            async with self.sessions.begin() as session:
                if account_id is not None:
                    account = await session.get(PaperAccountRow, account_id, with_for_update=True)
                    if account is None:
                        raise ValueError("paper account does not exist")
                active_runs = list(
                    (
                        await session.scalars(
                            select(SystemRunRow)
                            .where(SystemRunRow.stopped_at.is_(None))
                            .with_for_update()
                        )
                    ).all()
                )
                lease_fences_previous_owner = self.engine.dialect.name == "postgresql"
                stale_before = now.astimezone(timezone.utc) - stale_after
                for active_run in active_runs:
                    heartbeat = active_run.last_heartbeat_at
                    if heartbeat.tzinfo is None:
                        heartbeat = heartbeat.replace(tzinfo=timezone.utc)
                    if (
                        not lease_fences_previous_owner
                        and heartbeat.astimezone(timezone.utc) >= stale_before
                    ):
                        raise ActiveRuntimeError("another runtime is active")
                    active_run.stopped_at = now
                    active_run.last_heartbeat_at = now
                    active_run.stop_reason = "recovered_stale_runtime"
                session.add(
                    SystemRunRow(
                        id=run_id, started_at=now, stopped_at=None, mode=mode,
                        strategy_version_id=strategy_version_id, hostname=hostname,
                        app_version=app_version, stop_reason=None, last_heartbeat_at=now,
                    )
                )
            self._owned_system_run_id = run_id
        return run_id

    async def heartbeat_system_run(self, run_id: str, now: datetime) -> None:
        if self._owned_system_run_id != run_id:
            raise ActiveRuntimeError("runtime does not own the requested system run")
        await self.assert_runtime_lease()
        async with self.sessions.begin() as session:
            result = await session.execute(
                update(SystemRunRow)
                .where(SystemRunRow.id == run_id, SystemRunRow.stopped_at.is_(None))
                .values(last_heartbeat_at=now)
            )
            if getattr(result, "rowcount", None) != 1:
                raise ActiveRuntimeError("runtime ownership row was lost")

    async def stop_system_run(self, run_id: str, *, reason: str, now: datetime) -> None:
        async with self.sessions.begin() as session:
            await session.execute(
                update(SystemRunRow).where(SystemRunRow.id == run_id).values(
                    stopped_at=now, last_heartbeat_at=now, stop_reason=reason[:500]
                )
            )
        if self._owned_system_run_id == run_id:
            self._owned_system_run_id = None

    async def _assert_transaction_runtime_owner(self, session: AsyncSession) -> None:
        owned_run_id = self._owned_system_run_id
        active_run_id = await session.scalar(
            select(SystemRunRow.id).where(SystemRunRow.stopped_at.is_(None)).limit(1)
        )
        if owned_run_id is None:
            if active_run_id is not None:
                raise ActiveRuntimeError("paper-account transaction is owned by another runtime")
            return
        if active_run_id != owned_run_id:
            raise ActiveRuntimeError("paper-account transaction lost runtime ownership")

    async def load_wallet_analysis(self) -> tuple[list[WalletProfile], list[WalletRelation]]:
        async with self.sessions() as session:
            profile_rows = list((await session.scalars(select(WalletProfileRow))).all())
            relation_rows = list((await session.scalars(select(WalletRelationRow))).all())
        profiles = [
            WalletProfile(
                wallet_address=row.wallet_address, first_seen_at=row.first_seen_at,
                initial_funder=row.initial_funder, funding_signature=row.funding_signature,
                known_creator=row.known_creator, tokens_created_total=row.tokens_created,
                tokens_created_7d=row.tokens_created_7d, tokens_created_30d=row.tokens_created_30d,
                tokens_reaching_pumpswap=row.tokens_reaching_pumpswap,
                tokens_reaching_2x_executable=row.tokens_reaching_2x_executable,
                tokens_with_liquidity_rug=row.tokens_with_liquidity_rug,
                tokens_with_dev_dump_5m=row.tokens_with_dev_dump_5m,
                median_peak_return=row.median_peak_return,
                median_token_lifetime_seconds=row.median_token_lifetime_seconds,
                median_dev_sell_delay_seconds=row.median_dev_sell_delay_seconds,
                last_token_created_at=row.last_token_created_at,
                known_funding_cluster=row.known_funding_cluster,
                tokens_traded=row.tokens_traded, profile_updated_at=row.profile_updated_at,
            )
            for row in profile_rows
        ]
        relations = [
            WalletRelation(
                wallet_a=row.wallet_a, wallet_b=row.wallet_b,
                relation_score=row.relation_score, evidence=row.relation_types_json,
                eligible_for_cluster=row.relation_score >= Decimal("0.70"),
            )
            for row in relation_rows
        ]
        return profiles, relations

    async def _reconcile_event_claim_tokens(
        self,
        claim_tokens: dict[str, str],
    ) -> None:
        """Keep only tokens whose ownership is durably visible after a failed commit."""
        if not claim_tokens:
            return
        durable_owned: set[tuple[str, str]] = set()
        async with self.sessions() as session:
            dialect = session.bind.dialect.name if session.bind is not None else ""
            for chunk in _event_id_chunks(
                sorted(claim_tokens),
                dialect=dialect,
            ):
                rows = (
                    await session.execute(
                        select(
                            EventDedupRow.event_id,
                            EventDedupRow.processing_status,
                            EventDedupRow.processing_token,
                        ).where(EventDedupRow.event_id.in_(chunk))
                    )
                ).all()
                durable_owned.update(
                    (str(event_id), str(processing_token))
                    for event_id, processing_status, processing_token in rows
                    if (
                        processing_status == "PROCESSING"
                        and processing_token is not None
                    )
                )
        for event_id, claim_token in claim_tokens.items():
            if (
                (event_id, claim_token) not in durable_owned
                and self._event_claim_tokens.get(event_id) == claim_token
            ):
                self._event_claim_tokens.pop(event_id, None)

    @asynccontextmanager
    async def _event_claim_transaction(
        self,
        claim_tokens: dict[str, str],
    ) -> AsyncIterator[AsyncSession]:
        body_completed = False
        try:
            async with self.sessions() as session, session.begin():
                yield session
                body_completed = True
        except BaseException:
            if not body_completed:
                try:
                    await self._reconcile_event_claim_tokens(claim_tokens)
                except Exception:
                    logger.exception(
                        "failed to reconcile event claim tokens after transaction failure"
                    )
            raise

    async def record_events(
        self,
        events: list[EventEnvelope],
        *,
        reclaim: bool = False,
        resume_owned: bool = False,
    ) -> list[bool]:
        """Durably claim an ordered event batch in one synchronous transaction."""
        if not events:
            return []
        if len(events) > MAX_EVENT_BATCH_SIZE:
            raise ValueError(
                f"record_events accepts at most {MAX_EVENT_BATCH_SIZE} events"
            )
        started = time.perf_counter()
        async with self._event_batch_lock:
            now = datetime.now(tz=timezone.utc)
            unique_events: list[EventEnvelope] = []
            unique_event_ids: set[str] = set()
            for event in events:
                if event.event_id not in unique_event_ids:
                    unique_event_ids.add(event.event_id)
                    unique_events.append(event)

            claim_tokens: dict[str, str] = {}
            for event in unique_events:
                existing_token = self._event_claim_tokens.get(event.event_id)
                if existing_token is None:
                    existing_token = str(uuid4())
                claim_tokens[event.event_id] = existing_token
            self._event_claim_tokens.update(claim_tokens)

            claimed_event_ids: set[str] = set()
            durably_owned_event_ids: set[str] = set()
            inserted_event_ids: set[str] = set()
            async with self._event_claim_transaction(claim_tokens) as session:
                dialect = session.bind.dialect.name if session.bind is not None else ""
                if dialect not in {"postgresql", "sqlite"}:
                    raise RuntimeError(
                        f"unsupported event-claim database dialect: {dialect or 'unknown'}"
                    )
                if dialect == "postgresql":
                    await _ensure_raw_event_partitions(
                        session,
                        {event.block_time.date() for event in unique_events},
                    )

                dedup_rows = [
                    {
                        "event_id": event.event_id,
                        "block_date": event.block_time.date(),
                        "first_seen_at": event.observed_at,
                        "processing_status": "PROCESSING",
                        "processing_attempts": 1,
                        "last_attempt_at": now,
                        "processed_at": None,
                        "last_error": None,
                        "processing_token": claim_tokens[event.event_id],
                    }
                    for event in sorted(unique_events, key=lambda item: item.event_id)
                ]
                for chunk in _bulk_insert_chunks(dedup_rows, dialect=dialect):
                    statement: Any
                    if dialect == "sqlite" and len(chunk) == 1:
                        statement = (
                            sqlite_insert(EventDedupRow)
                            .values(**chunk[0])
                            .on_conflict_do_nothing(
                                index_elements=[EventDedupRow.event_id]
                            )
                        )
                        result = await session.execute(statement)
                        rowcount = getattr(result, "rowcount", None)
                        if rowcount not in {0, 1}:
                            raise RuntimeError(
                                "single event claim returned an invalid row count"
                            )
                        returned_ids = (
                            {str(chunk[0]["event_id"])} if rowcount == 1 else set()
                        )
                    elif dialect == "postgresql":
                        statement = (
                            pg_insert(EventDedupRow)
                            .values(chunk)
                            .on_conflict_do_nothing(
                                index_elements=[EventDedupRow.event_id]
                            )
                            .returning(EventDedupRow.event_id)
                        )
                        returned_ids = {
                            str(event_id)
                            for event_id in (await session.scalars(statement)).all()
                        }
                    else:
                        statement = (
                            sqlite_insert(EventDedupRow)
                            .values(chunk)
                            .on_conflict_do_nothing(
                                index_elements=[EventDedupRow.event_id]
                            )
                            .returning(EventDedupRow.event_id)
                        )
                        returned_ids = {
                            str(event_id)
                            for event_id in (await session.scalars(statement)).all()
                        }
                    chunk_ids = {str(row["event_id"]) for row in chunk}
                    if (
                        not returned_ids <= chunk_ids
                        or inserted_event_ids & returned_ids
                    ):
                        raise RuntimeError(
                            "event claim insert returned an invalid event-id set"
                        )
                    inserted_event_ids.update(returned_ids)

                if not inserted_event_ids <= unique_event_ids:
                    raise RuntimeError("event claim insert returned an unknown event id")
                conflict_event_ids = unique_event_ids - inserted_event_ids
                conflict_rows: dict[str, EventDedupRow] = {}
                if conflict_event_ids:
                    rows: list[EventDedupRow] = []
                    for event_id_chunk in _event_id_chunks(
                        sorted(conflict_event_ids),
                        dialect=dialect,
                    ):
                        conflict_statement = (
                            select(EventDedupRow)
                            .where(EventDedupRow.event_id.in_(event_id_chunk))
                            .order_by(EventDedupRow.event_id)
                            .with_for_update()
                        )
                        rows.extend(
                            (await session.scalars(conflict_statement)).all()
                        )
                    conflict_rows = {row.event_id: row for row in rows}
                    if (
                        len(rows) != len(conflict_rows)
                        or set(conflict_rows) != conflict_event_ids
                    ):
                        raise RuntimeError(
                            "event claim conflict set changed during canonical locking"
                        )

                for event in unique_events:
                    if event.event_id in inserted_event_ids:
                        claimed_event_ids.add(event.event_id)
                        continue
                    existing = conflict_rows[event.event_id]
                    owned_token = self._event_claim_tokens.get(event.event_id)
                    if (
                        resume_owned
                        and owned_token is not None
                        and existing.processing_status == "PROCESSING"
                        and existing.processing_token == owned_token
                    ):
                        claimed_event_ids.add(event.event_id)
                        continue
                    stale_claim = (
                        existing.last_attempt_at is None
                        or now - _as_utc(existing.last_attempt_at)
                        >= timedelta(minutes=2)
                    )
                    if existing.processing_status == "PROCESSED" or (
                        existing.processing_status == "PROCESSING"
                        and not stale_claim
                        and not reclaim
                    ):
                        if (
                            existing.processing_status == "PROCESSING"
                            and existing.processing_token
                            == claim_tokens[event.event_id]
                        ):
                            durably_owned_event_ids.add(event.event_id)
                        continue
                    existing.processing_status = "PROCESSING"
                    existing.processing_attempts += 1
                    existing.last_attempt_at = now
                    existing.last_error = None
                    existing.processing_token = claim_tokens[event.event_id]
                    claimed_event_ids.add(event.event_id)

                new_events = [
                    event
                    for event in unique_events
                    if event.event_id in inserted_event_ids
                ]
                if new_events:
                    if dialect == "postgresql":
                        sequence_rows = (
                            await session.scalars(
                                text(
                                    "SELECT "
                                    "nextval('raw_chain_events_ingest_sequence_seq') "
                                    "FROM generate_series(1, :batch_size)"
                                ),
                                {"batch_size": len(new_events)},
                            )
                        ).all()
                        ingest_sequences = [int(value) for value in sequence_rows]
                    else:
                        last_sequence = await session.scalar(
                            select(func.max(RawChainEventRow.ingest_sequence))
                        )
                        first_sequence = int(last_sequence or 0) + 1
                        ingest_sequences = list(
                            range(first_sequence, first_sequence + len(new_events))
                        )
                    if len(ingest_sequences) != len(new_events):
                        raise RuntimeError(
                            "database returned an incomplete ingest-sequence allocation"
                        )

                    raw_rows = [
                        {
                            "id": str(uuid4()),
                            "ingest_sequence": ingest_sequence,
                            "block_date": event.block_time.date(),
                            "event_id": event.event_id,
                            "source": event.source.value,
                            "protocol": event.protocol.value,
                            "event_type": event.event_type.value,
                            "slot": event.slot,
                            "signature": event.signature,
                            "instruction_index": event.instruction_index,
                            "inner_instruction_index": (
                                event.inner_instruction_index
                            ),
                            "block_time": event.block_time,
                            "observed_at": event.observed_at,
                            "commitment": event.commitment,
                            "mint": event.mint,
                            "pool_address": event.pool_address,
                            "payload_json": event.payload,
                            "created_at": now,
                        }
                        for event, ingest_sequence in zip(
                            new_events, ingest_sequences, strict=True
                        )
                    ]
                    for chunk in _bulk_insert_chunks(raw_rows, dialect=dialect):
                        if len(chunk) == 1:
                            session.add(RawChainEventRow(**chunk[0]))
                        elif dialect == "postgresql":
                            raw_statement: Any = pg_insert(RawChainEventRow).values(
                                chunk
                            )
                            await session.execute(raw_statement)
                        else:
                            raw_statement = sqlite_insert(RawChainEventRow).values(
                                chunk
                            )
                            await session.execute(raw_statement)

            retained_event_ids = claimed_event_ids | durably_owned_event_ids
            for event_id, claim_token in claim_tokens.items():
                if (
                    event_id not in retained_event_ids
                    and self._event_claim_tokens.get(event_id) == claim_token
                ):
                    self._event_claim_tokens.pop(event_id, None)
            accepted: list[bool] = []
            emitted_event_ids: set[str] = set()
            for event in events:
                first_occurrence = event.event_id not in emitted_event_ids
                accepted.append(
                    first_occurrence and event.event_id in claimed_event_ids
                )
                emitted_event_ids.add(event.event_id)
        self._observe_query(started)
        return accepted

    async def record_event(self, event: EventEnvelope, *, reclaim: bool = False) -> bool:
        """Claim one event through the ordered batch implementation."""
        return (await self.record_events([event], reclaim=reclaim))[0]

    async def mark_event_processed(
        self,
        event_id: str,
        *,
        processed_at: datetime,
    ) -> None:
        claim_token = self._event_claim_tokens.get(event_id)
        if claim_token is None:
            raise RuntimeError("event claim token is missing")
        batch = self._event_state_batch.get()
        if batch is not None:
            previous = batch.processed_claims.get(event_id)
            if previous is not None and previous[0] != claim_token:
                raise RuntimeError(
                    "event processing claim was superseded"
                )
            batch.processed_claims[event_id] = (
                claim_token,
                max(
                    processed_at,
                    previous[1] if previous is not None else processed_at,
                ),
            )
            return
        shared_transaction = self._event_write_session.get() is not None
        async with self._write_session() as session:
            result = await session.execute(
                update(EventDedupRow)
                .where(
                    EventDedupRow.event_id == event_id,
                    EventDedupRow.processing_status == "PROCESSING",
                    EventDedupRow.processing_token == claim_token,
                )
                .values(
                    processing_status="PROCESSED",
                    processed_at=processed_at,
                    last_attempt_at=processed_at,
                    last_error=None,
                    processing_token=None,
                )
            )
            if getattr(result, "rowcount", None) != 1:
                raise RuntimeError(
                    "event processing claim was superseded"
                )
        if not shared_transaction:
            self.release_event_claim(event_id)

    async def mark_events_processed(
        self,
        event_ids: list[str],
        *,
        processed_at: datetime,
    ) -> None:
        unique_event_ids = sorted(set(event_ids))
        if not unique_event_ids:
            return
        claims: dict[str, tuple[str, datetime]] = {}
        for event_id in unique_event_ids:
            claim_token = self._event_claim_tokens.get(event_id)
            if claim_token is None:
                raise RuntimeError("event claim token is missing")
            claims[event_id] = (claim_token, processed_at)
        batch = self._event_state_batch.get()
        if batch is not None:
            for event_id, claim in claims.items():
                previous = batch.processed_claims.get(event_id)
                if previous is not None and previous[0] != claim[0]:
                    raise RuntimeError(
                        "event processing claim was superseded"
                    )
                batch.processed_claims[event_id] = claim
            return
        active_session = self._event_write_session.get()
        if active_session is not None:
            await self._execute_mark_events_processed(
                active_session,
                claims,
            )
            return
        async with self._event_batch_lock:
            async with self.sessions.begin() as session:
                await self._execute_mark_events_processed(
                    session,
                    claims,
                )
            for event_id, (claim_token, _) in claims.items():
                if (
                    self._event_claim_tokens.get(event_id)
                    == claim_token
                ):
                    self._event_claim_tokens.pop(event_id, None)
    async def mark_events_failed(
        self,
        event_ids: list[str],
        error: BaseException,
    ) -> None:
        unique_event_ids = sorted(set(event_ids))
        if not unique_event_ids:
            return
        async with self._event_batch_lock:
            claim_tokens: dict[str, str] = {}
            for event_id in unique_event_ids:
                claim_token = self._event_claim_tokens.get(event_id)
                if claim_token is None:
                    raise RuntimeError("event claim token is missing")
                claim_tokens[event_id] = claim_token
            failed_at = datetime.now(tz=timezone.utc)
            async with self.sessions() as session, session.begin():
                dialect = session.bind.dialect.name if session.bind is not None else ""
                if dialect not in {"postgresql", "sqlite"}:
                    raise RuntimeError(
                        f"unsupported event-failure database dialect: {dialect or 'unknown'}"
                    )
                for event_id_chunk in _event_id_chunks(
                    unique_event_ids,
                    dialect=dialect,
                    parameters_per_id=3,
                    reserved_parameters=4,
                ):
                    rows = (
                        await session.execute(
                            select(
                                EventDedupRow.event_id,
                                EventDedupRow.processing_status,
                                EventDedupRow.processing_token,
                            )
                            .where(EventDedupRow.event_id.in_(event_id_chunk))
                            .order_by(EventDedupRow.event_id)
                            .with_for_update()
                        )
                    ).all()
                    durable_claims = {
                        str(event_id): (
                            str(processing_status),
                            (
                                str(processing_token)
                                if processing_token is not None
                                else None
                            ),
                        )
                        for event_id, processing_status, processing_token in rows
                    }
                    if set(durable_claims) != set(event_id_chunk) or any(
                        durable_claims[event_id]
                        != ("PROCESSING", claim_tokens[event_id])
                        for event_id in event_id_chunk
                    ):
                        raise RuntimeError("event processing claim was superseded")
                    predicates = [
                        and_(
                            EventDedupRow.event_id == event_id,
                            EventDedupRow.processing_status == "PROCESSING",
                            EventDedupRow.processing_token == claim_tokens[event_id],
                        )
                        for event_id in event_id_chunk
                    ]
                    result = await session.execute(
                        update(EventDedupRow)
                        .where(or_(*predicates))
                        .values(
                            processing_status="FAILED",
                            last_attempt_at=failed_at,
                            last_error=type(error).__name__[:128],
                            processing_token=None,
                        )
                    )
                    if getattr(result, "rowcount", None) != len(event_id_chunk):
                        raise RuntimeError("event processing claim was superseded")
            for event_id, claim_token in claim_tokens.items():
                if self._event_claim_tokens.get(event_id) == claim_token:
                    self._event_claim_tokens.pop(event_id, None)

    async def mark_event_failed(self, event_id: str, error: BaseException) -> None:
        await self.mark_events_failed([event_id], error)

    async def update_raw_event_context(self, event: EventEnvelope) -> None:
        """Persist decoder-enriched context within the active event transaction."""
        batch = self._event_state_batch.get()
        if batch is not None:
            batch.raw_contexts[event.event_id] = event
            return
        started = time.perf_counter()
        async with self._write_session() as session:
            result = await session.execute(
                update(RawChainEventRow)
                .where(
                    RawChainEventRow.event_id == event.event_id,
                    RawChainEventRow.block_date == event.block_time.date(),
                )
                .values(mint=event.mint, pool_address=event.pool_address)
            )
            rowcount = getattr(result, "rowcount", None)
            if rowcount != 1:
                raise RuntimeError(
                    f"raw event context update affected {rowcount} rows"
                )
        self._observe_query(started)

    async def load_processed_events_since(self, since: datetime) -> list[EventEnvelope]:
        async with self.sessions() as session:
            rows = (
                await session.scalars(
                    select(RawChainEventRow)
                    .join(EventDedupRow, EventDedupRow.event_id == RawChainEventRow.event_id)
                    .where(
                        EventDedupRow.processing_status == "PROCESSED",
                        RawChainEventRow.block_time >= since,
                        _processed_prefix_condition(),
                    )
                    .order_by(RawChainEventRow.ingest_sequence)

                )
            ).all()
        return [_event_from_row(row) for row in rows]

    async def load_processed_pool_creation_events(
        self, pool_addresses: set[str]
    ) -> list[EventEnvelope]:
        addresses = sorted(pool_addresses)
        if not addresses:
            return []
        rows: list[RawChainEventRow] = []
        async with self.sessions() as session:
            for offset in range(0, len(addresses), 500):
                chunk = addresses[offset : offset + 500]
                rows.extend(
                    (
                        await session.scalars(
                            select(RawChainEventRow)
                            .join(
                                EventDedupRow,
                                EventDedupRow.event_id == RawChainEventRow.event_id,
                            )
                            .where(
                                EventDedupRow.processing_status == "PROCESSED",
                                RawChainEventRow.event_type == "pool_created",
                                RawChainEventRow.pool_address.in_(chunk),
                                _processed_prefix_condition(),
                            )
                        )
                    ).all()
                )
        rows.sort(key=lambda row: row.ingest_sequence)
        return [_event_from_row(row) for row in rows]

    async def load_unprocessed_events(
        self, *, include_owned_processing: bool = False
    ) -> list[EventEnvelope]:
        stale_before = datetime.now(tz=timezone.utc) - timedelta(minutes=2)
        processing_filter: Any = EventDedupRow.processing_status == "PROCESSING"
        if not include_owned_processing:
            processing_filter = processing_filter & (
                (EventDedupRow.last_attempt_at.is_(None))
                | (EventDedupRow.last_attempt_at <= stale_before)
            )
        async with self.sessions() as session:
            rows = (
                await session.scalars(
                    select(RawChainEventRow)
                    .join(EventDedupRow, EventDedupRow.event_id == RawChainEventRow.event_id)
                    .where(
                        EventDedupRow.processing_attempts
                        < MAX_EVENT_PROCESSING_ATTEMPTS,
                        (EventDedupRow.processing_status == "FAILED")
                        | processing_filter,
                    )
                    .order_by(RawChainEventRow.ingest_sequence)

                )
            ).all()
        return [_event_from_row(row) for row in rows]

    async def load_quarantined_event_protocols(self) -> set[str]:
        async with self.sessions() as session:
            protocols = set(
                str(protocol)
                for protocol in (
                    await session.scalars(
                        select(RawChainEventRow.protocol)
                        .join(
                            EventDedupRow,
                            EventDedupRow.event_id == RawChainEventRow.event_id,
                        )
                        .where(
                            EventDedupRow.processing_status.in_(
                                ("FAILED", "PROCESSING")
                            ),
                            EventDedupRow.processing_attempts
                            >= MAX_EVENT_PROCESSING_ATTEMPTS,
                        )
                        .distinct()
                    )
                ).all()
            )
            first_unresolved = await session.scalar(
                select(func.min(RawChainEventRow.ingest_sequence))
                .join(
                    EventDedupRow,
                    EventDedupRow.event_id == RawChainEventRow.event_id,
                )
                .where(EventDedupRow.processing_status != "PROCESSED")
            )
            if first_unresolved is None:
                return protocols
            processed_suffix_count = await session.scalar(
                select(func.count())
                .select_from(RawChainEventRow)
                .join(
                    EventDedupRow,
                    EventDedupRow.event_id == RawChainEventRow.event_id,
                )
                .where(
                    RawChainEventRow.ingest_sequence > int(first_unresolved),
                    EventDedupRow.processing_status == "PROCESSED",
                )
            )
            if not processed_suffix_count:
                return protocols
            protocols.update(
                str(protocol)
                for protocol in (
                    await session.scalars(
                        select(RawChainEventRow.protocol)
                        .where(
                            RawChainEventRow.ingest_sequence
                            >= int(first_unresolved)
                        )
                        .distinct()
                    )
                ).all()
            )
        return protocols

    async def load_stream_checkpoint(self) -> tuple[int, str | None, datetime | None]:
        async with self.sessions() as session:
            row = await session.scalar(
                select(RawChainEventRow)
                .join(EventDedupRow, EventDedupRow.event_id == RawChainEventRow.event_id)
                .where(
                    EventDedupRow.processing_status == "PROCESSED",
                    _processed_prefix_condition(),
                )
                .order_by(RawChainEventRow.ingest_sequence.desc())
                .limit(1)
            )
        if row is None:
            return 0, None, None
        return row.slot, row.signature, _as_utc(row.block_time)

    async def load_protocol_checkpoints(self) -> dict[str, str]:
        checkpoints: dict[str, str] = {}
        async with self.sessions() as session:
            protocols = list(
                (await session.scalars(select(RawChainEventRow.protocol).distinct())).all()
            )
            for protocol in protocols:
                signature = await session.scalar(
                    select(RawChainEventRow.signature)
                    .join(
                        EventDedupRow,
                        EventDedupRow.event_id == RawChainEventRow.event_id,
                    )
                    .where(
                        RawChainEventRow.protocol == protocol,
                        EventDedupRow.processing_status == "PROCESSED",
                        _processed_prefix_condition(),
                    )
                    .order_by(RawChainEventRow.ingest_sequence.desc())
                    .limit(1)
                )
                if signature:
                    checkpoints[str(protocol)] = str(signature)
        return checkpoints

    async def upsert_token(self, token: TokenRecord) -> None:
        values = {
            "mint": token.mint,
            "token_program": token.token_program,
            "name": token.name,
            "symbol": token.symbol,
            "decimals": token.decimals,
            "total_supply_raw": token.total_supply_raw,
            "creator_address": token.creator_address,
            "creation_signature": token.creation_signature,
            "creation_slot": token.creation_slot,
            "creation_time": token.creation_time,
            "metadata_uri": token.metadata_uri,
            "metadata_mutable": token.metadata_mutable,
            "enrichment_json": token.enrichment,
            "enriched_at": token.enriched_at,
            "first_seen_at": token.creation_time or token.updated_at,
            "updated_at": token.updated_at,
        }
        await self._upsert(TokenRow, values, ["mint"])

    async def upsert_wallet_profile(self, profile: WalletProfile) -> None:
        await self._upsert(
            WalletProfileRow,
            {
                "wallet_address": profile.wallet_address,
                "first_seen_at": profile.first_seen_at,
                "initial_funder": profile.initial_funder,
                "funding_signature": profile.funding_signature,
                "known_creator": profile.known_creator,
                "tokens_created": profile.tokens_created_total,
                "tokens_traded": profile.tokens_traded,
                "tokens_created_7d": profile.tokens_created_7d,
                "tokens_created_30d": profile.tokens_created_30d,
                "tokens_reaching_pumpswap": profile.tokens_reaching_pumpswap,
                "tokens_reaching_2x_executable": profile.tokens_reaching_2x_executable,
                "tokens_with_liquidity_rug": profile.tokens_with_liquidity_rug,
                "tokens_with_dev_dump_5m": profile.tokens_with_dev_dump_5m,
                "median_peak_return": profile.median_peak_return,
                "median_token_lifetime_seconds": profile.median_token_lifetime_seconds,
                "median_dev_sell_delay_seconds": profile.median_dev_sell_delay_seconds,
                "last_token_created_at": profile.last_token_created_at,
                "known_funding_cluster": profile.known_funding_cluster,
                "profile_updated_at": profile.profile_updated_at,
            },
            ["wallet_address"],
        )

    async def upsert_wallet_relation(
        self, relation: WalletRelation, observed_at: datetime
    ) -> None:
        await self._upsert(
            WalletRelationRow,
            {
                "wallet_a": relation.wallet_a,
                "wallet_b": relation.wallet_b,
                "relation_score": relation.relation_score,
                "relation_types_json": relation.evidence,
                "first_detected_at": observed_at,
                "last_detected_at": observed_at,
            },
            ["wallet_a", "wallet_b"],
        )

    async def record_external_api_call(
        self,
        *,
        provider: str,
        endpoint: str,
        request_json: dict[str, Any],
        response_json: dict[str, Any] | None,
        requested_at: datetime,
        received_at: datetime | None,
        latency_ms: int | None,
        http_status: int | None,
        error_code: str | None,
    ) -> str:
        import hashlib
        import json

        request_hash = hashlib.sha256(
            json.dumps(request_json, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()
        row_id = str(uuid4())
        async with self.sessions.begin() as session:
            session.add(
                ExternalApiCallRow(
                    id=row_id,
                    provider=provider,
                    endpoint=endpoint,
                    request_hash=request_hash,
                    requested_at=requested_at,
                    received_at=received_at,
                    latency_ms=latency_ms,
                    http_status=http_status,
                    request_json=request_json,
                    response_json=response_json,
                    error_code=error_code,
                )
            )
        return row_id

    async def upsert_pool(self, pool: PoolRecord) -> None:
        values = {
            "pool_address": pool.pool_address,
            "mint": pool.base_mint,
            "protocol": pool.protocol,
            "quote_mint": pool.quote_mint,
            "base_vault": pool.base_vault,
            "quote_vault": pool.quote_vault,
            "creation_signature": pool.creation_signature,
            "creation_slot": pool.creation_slot,
            "creation_time": pool.creation_time,
            "migration_signature": pool.migration_signature,
            "status": pool.status,
            "updated_at": pool.updated_at,
        }
        await self._upsert(PoolRow, values, ["pool_address"])

    async def record_snapshot(self, snapshot: FeatureSnapshot) -> None:
        async with self.sessions.begin() as session:
            session.add(
                MarketSnapshotRow(
                    id=str(uuid4()),
                    snapshot_date=snapshot.snapshot_time.date(),
                    pool_address=snapshot.pool_address,
                    snapshot_time=snapshot.snapshot_time,
                    price_usd=snapshot.current_price_usd,
                    quote_liquidity_usd=snapshot.quote_liquidity_usd,
                    base_liquidity_usd=snapshot.quote_liquidity_usd,
                    market_cap_estimate=(
                        snapshot.market_cap_to_quote_liquidity * snapshot.quote_liquidity_usd
                    ),
                    volume_15s=snapshot.buy_volume_usd_15s + snapshot.sell_volume_usd_15s,
                    volume_30s=snapshot.buy_volume_usd_30s + snapshot.sell_volume_usd_30s,
                    volume_60s=snapshot.buy_volume_usd_60s + snapshot.sell_volume_usd_60s,
                    unique_buyers_60s=snapshot.unique_buyers_60s,
                    unique_sellers_60s=snapshot.unique_sellers_60s,
                    holder_count=snapshot.holder_count,
                    features_json=snapshot.model_dump(mode="json"),
                    data_quality_flags=[],
                )
            )

    async def upsert_candidate(self, candidate: Candidate, strategy_id: str) -> None:
        values = {
            "id": candidate.candidate_id,
            "mint": candidate.mint,
            "pool_address": candidate.pool_address,
            "state": candidate.state.value,
            "detected_at": candidate.detected_at,
            "eligible_at": candidate.eligible_at,
            "armed_at": candidate.armed_at,
            "expired_at": candidate.expired_at,
            "rejected_at": candidate.rejected_at,
            "reject_reason": candidate.reject_reason.value if candidate.reject_reason else None,
            "strategy_version_id": strategy_id,
            "config_hash": candidate.config_hash,
            "runtime_state_json": candidate.model_dump(mode="json"),
        }
        await self._upsert(CandidateRow, values, ["id"])

    async def load_active_candidates(self, strategy_id: str) -> list[Candidate]:
        async with self.sessions() as session:
            rows = (
                await session.scalars(
                    select(CandidateRow).where(
                        CandidateRow.strategy_version_id == strategy_id
                    )
                )
            ).all()
        return [
            Candidate.model_validate(row.runtime_state_json)
            for row in rows
            if row.runtime_state_json
        ]

    async def load_candidate_score_totals(
        self, strategy_id: str
    ) -> dict[str, Decimal]:
        async with self.sessions() as session:
            rows = (
                await session.execute(
                    select(
                        SignalEvaluationRow.candidate_id,
                        SignalEvaluationRow.score,
                    )
                    .join(
                        CandidateRow,
                        CandidateRow.id == SignalEvaluationRow.candidate_id,
                    )
                    .where(CandidateRow.strategy_version_id == strategy_id)
                    .order_by(SignalEvaluationRow.evaluated_at)
                )
            ).all()
        return {str(candidate_id): Decimal(str(score)) for candidate_id, score in rows}

    async def record_security(
        self,
        context: SecurityContext,
        result: SecurityResult,
    ) -> str:
        row_id = str(uuid4())
        holders = context.holders
        async with self.sessions.begin() as session:
            session.add(
                TokenSecurityCheckRow(
                    id=row_id,
                    mint=context.mint.mint,
                    checked_at=result.checked_at,
                    token_program=context.mint.token_program,
                    mint_authority=context.mint.mint_authority,
                    freeze_authority=context.mint.freeze_authority,
                    extensions_json=[],
                    largest_holder_pct=holders.largest_holder_pct if holders else None,
                    top_5_pct=holders.top_5_holders_pct if holders else None,
                    top_10_pct=holders.top_10_holders_pct if holders else None,
                    dev_holding_pct=holders.dev_holding_pct if holders else None,
                    dev_cluster_pct=holders.dev_cluster_holding_pct if holders else None,
                    largest_related_cluster_pct=(holders.related_cluster_holding_pct if holders else None),
                    unknown_supply_pct=holders.unknown_owner_supply_pct if holders else None,
                    buy_route_available=context.execution.buy_route_available,
                    sell_route_available=context.execution.sell_route_available,
                    hard_reject=result.hard_reject,
                    reject_reasons_json=[reason.value for reason in result.reject_reasons],
                )
            )
        return row_id

    async def record_signal(
        self,
        candidate: Candidate,
        snapshot: FeatureSnapshot,
        score: ScoreBreakdown,
    ) -> str:
        row_id = str(uuid4())
        async with self.sessions.begin() as session:
            session.add(
                SignalEvaluationRow(
                    id=row_id,
                    candidate_id=candidate.candidate_id,
                    evaluated_at=snapshot.snapshot_time,
                    score=score.total_score,
                    organic_score=score.organic_score,
                    distribution_score=score.distribution_score,
                    execution_score=score.execution_score,
                    liquidity_score=score.liquidity_score,
                    developer_score=score.developer_score,
                    price_structure_score=score.price_structure_score,
                    features_json=snapshot.model_dump(mode="json"),
                    rules_json=score.explanations,
                )
            )
        return row_id

    async def enqueue_outbox(
        self,
        *,
        idempotency_key: str,
        event_type: str,
        payload: dict[str, Any],
        session: AsyncSession | None = None,
    ) -> bool:
        values = {
            "id": str(uuid4()),
            "idempotency_key": idempotency_key,
            "event_type": event_type,
            "payload_json": payload,
            "created_at": datetime.now(tz=timezone.utc),
            "available_at": datetime.now(tz=timezone.utc),
            "attempts": 0,
        }
        if session is not None:
            return await self._insert_outbox(session, values)
        async with self.sessions.begin() as own_session:
            return await self._insert_outbox(own_session, values)

    async def pending_outbox(self, limit: int = 100) -> list[OutboxEventRow]:
        async with self.sessions.begin() as session:
            now = datetime.now(tz=timezone.utc)
            await session.execute(
                update(OutboxEventRow)
                .where(
                    OutboxEventRow.delivery_state == "SENDING",
                    OutboxEventRow.claimed_at <= now - timedelta(seconds=30),
                )
                .values(
                    delivery_state="UNCERTAIN",
                    last_error="DELIVERY_OUTCOME_UNKNOWN_AFTER_LEASE_EXPIRY",
                    claim_token=None,
                )
            )
            statement = (
                select(OutboxEventRow)
                .where(
                    OutboxEventRow.delivered_at.is_(None),
                    OutboxEventRow.delivery_state.in_(["PENDING", "FAILED"]),
                    OutboxEventRow.available_at <= now,
                )
                .order_by(OutboxEventRow.created_at)
                .limit(limit)
            )
            if session.bind is not None and session.bind.dialect.name == "postgresql":
                statement = statement.with_for_update(skip_locked=True)
            rows = list((await session.scalars(statement)).all())
            lease_until = now + timedelta(seconds=30)
            for row in rows:
                row.available_at = lease_until
                row.attempts += 1
                row.delivery_state = "SENDING"
                row.claimed_at = now
                row.claim_token = str(uuid4())
            if self.metrics is not None:
                self.metrics.outbox_pending.set(len(rows))
            return rows

    async def mark_outbox_delivered(
        self,
        event_id: str,
        claim_token: str,
        telegram_message_id: str | None = None,
    ) -> None:
        async with self.sessions.begin() as session:
            delivered_at = datetime.now(tz=timezone.utc)
            event = await session.get(OutboxEventRow, event_id, with_for_update=True)
            if event is None:
                raise RuntimeError("outbox event is missing")
            self._validate_outbox_claim(event, claim_token)
            await self._apply_outbox_delivery(
                session, event, delivered_at, telegram_message_id
            )

    @staticmethod
    async def _apply_outbox_delivery(
        session: AsyncSession,
        event: OutboxEventRow,
        delivered_at: datetime,
        telegram_message_id: str | None,
    ) -> None:
            event.delivered_at = delivered_at
            event.telegram_message_id = telegram_message_id
            event.last_error = None
            event.delivery_state = "DELIVERED"
            event.claimed_at = None
            event.claim_token = None
            metadata = event.payload_json.get("_daily_report")
            if isinstance(metadata, dict):
                report_date = datetime.strptime(
                    str(metadata["date"]), "%Y-%m-%d"
                ).date()
                report = await session.get(
                    DailyReportRow,
                    (
                        report_date,
                        str(metadata["timezone"]),
                        str(metadata["strategy_version"]),
                    ),
                    with_for_update=True,
                )
                if report is not None:
                    report.telegram_message_id = telegram_message_id
                    report.sent_at = delivered_at

    async def resolve_uncertain_outbox(
        self,
        event_id: str,
        *,
        action: str,
        telegram_message_id: str | None = None,
    ) -> str:
        if action not in {"retry", "delivered", "dead"}:
            raise ValueError("action must be retry, delivered, or dead")
        if action == "delivered" and not telegram_message_id:
            raise ValueError("telegram_message_id is required for delivered")
        async with self.sessions.begin() as session:
            event = await session.get(OutboxEventRow, event_id, with_for_update=True)
            if event is None:
                raise RuntimeError("outbox event is missing")
            if event.delivery_state != "UNCERTAIN":
                raise RuntimeError("only UNCERTAIN outbox events can be reconciled")
            now = datetime.now(tz=timezone.utc)
            if action == "delivered":
                await self._apply_outbox_delivery(
                    session, event, now, telegram_message_id
                )
            elif action == "retry":
                event.delivery_state = "FAILED"
                event.available_at = now
                event.claimed_at = None
                event.claim_token = None
                event.last_error = "OPERATOR_RETRY_AFTER_RECONCILIATION"
            else:
                event.delivery_state = "DEAD"
                event.claimed_at = None
                event.claim_token = None
                event.last_error = "OPERATOR_CONFIRMED_DEAD"
            return event.delivery_state

    @staticmethod
    def _validate_outbox_claim(event: OutboxEventRow, claim_token: str) -> None:
        if (
            event.delivery_state != "SENDING"
            or event.claim_token is None
            or event.claim_token != claim_token
        ):
            raise RuntimeError("outbox claim was superseded")

    async def mark_outbox_failed(
        self, event_id: str, claim_token: str, error_code: str
    ) -> None:
        async with self.sessions.begin() as session:
            event = await session.get(OutboxEventRow, event_id, with_for_update=True)
            if event is None:
                raise RuntimeError("outbox event is missing")
            self._validate_outbox_claim(event, claim_token)
            event.last_error = error_code[:500]
            event.claimed_at = None
            event.claim_token = None
            event.delivery_state = "DEAD" if event.attempts >= 10 else "FAILED"
            delay = min(300, 2 ** min(event.attempts, 8))
            event.available_at = datetime.now(tz=timezone.utc) + timedelta(seconds=delay)

    async def mark_outbox_uncertain(
        self, event_id: str, claim_token: str, error_code: str
    ) -> None:
        async with self.sessions.begin() as session:
            event = await session.get(OutboxEventRow, event_id, with_for_update=True)
            if event is None:
                raise RuntimeError("outbox event is missing")
            self._validate_outbox_claim(event, claim_token)
            event.delivery_state = "UNCERTAIN"
            event.last_error = error_code[:500]
            event.claimed_at = None
            event.claim_token = None

    async def mark_outbox_dead(
        self, event_id: str, claim_token: str, error_code: str
    ) -> None:
        async with self.sessions.begin() as session:
            event = await session.get(OutboxEventRow, event_id, with_for_update=True)
            if event is None:
                raise RuntimeError("outbox event is missing")
            self._validate_outbox_claim(event, claim_token)
            event.delivery_state = "DEAD"
            event.last_error = error_code[:500]
            event.claimed_at = None
            event.claim_token = None

    async def outbox_count(self) -> int:
        async with self.sessions() as session:
            result = await session.scalar(
                select(func.count()).select_from(OutboxEventRow).where(OutboxEventRow.delivered_at.is_(None))
            )
            return int(result or 0)

    async def deliverable_outbox_count(self) -> int:
        async with self.sessions() as session:
            result = await session.scalar(
                select(func.count())
                .select_from(OutboxEventRow)
                .where(
                    OutboxEventRow.delivery_state.in_(("PENDING", "FAILED", "SENDING"))
                )
            )
            return int(result or 0)

    async def load_daily_report(
        self,
        report_date: str,
        *,
        timezone_name: str,
        strategy_version: str,
    ) -> dict[str, Any] | None:
        day = datetime.strptime(report_date, "%Y-%m-%d").date()
        async with self.sessions() as session:
            row = await session.get(
                DailyReportRow,
                (day, timezone_name, strategy_version),
            )
            return dict(row.report_json) if row is not None else None

    async def store_daily_report(
        self,
        *,
        report: dict[str, Any],
        include_all_time: dict[str, Any] | None = None,
    ) -> bool:
        report_date = datetime.strptime(str(report["date"]), "%Y-%m-%d").date()
        now = datetime.now(tz=timezone.utc)
        async with self.sessions.begin() as session:
            existing = await session.get(
                DailyReportRow,
                (report_date, str(report["timezone"]), str(report["strategy_version"])),
            )
            if existing is not None:
                return False
            session.add(
                DailyReportRow(
                    report_date=report_date,
                    timezone=str(report["timezone"]),
                    strategy_version_id=str(report["strategy_version"]),
                    report_json=report,
                    telegram_message_id=None,
                    generated_at=now,
                    sent_at=None,
                )
            )
            await self._insert_outbox(
                session,
                {
                    "id": str(uuid4()),
                    "idempotency_key": f"telegram:daily-report:{report['report_id']}",
                    "event_type": "daily_report",
                    "payload_json": {
                        "text": _telegram_report_text(report),
                        "_daily_report": {
                            "date": report["date"],
                            "timezone": report["timezone"],
                            "strategy_version": report["strategy_version"],
                        },
                    },
                    "created_at": now,
                    "available_at": now,
                    "attempts": 0,
                },
            )
            if include_all_time is not None:
                await self._insert_outbox(
                    session,
                    {
                        "id": str(uuid4()),
                        "idempotency_key": f"telegram:all-time-report:{report['report_id']}",
                        "event_type": "all_time_report",
                        "payload_json": {"text": _telegram_report_text(include_all_time)},
                        "created_at": now,
                        "available_at": now,
                        "attempts": 0,
                    },
                )
            return True

    async def run_retention(self, *, raw_retention_days: int) -> None:
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=max(90, raw_retention_days))
        async with self.sessions.begin() as session:
            await session.execute(
                delete(RawChainEventRow).where(RawChainEventRow.block_time < cutoff)
            )
            if session.bind is not None and session.bind.dialect.name == "postgresql":
                await session.execute(
                    text(
                        """
                        DELETE FROM market_snapshots AS s
                        USING pools AS p
                        WHERE s.pool_address = p.pool_address
                          AND (
                            s.snapshot_time < now() - interval '24 hours'
                            OR (
                              s.snapshot_time >= p.creation_time + interval '10 minutes'
                              AND s.snapshot_time < p.creation_time + interval '60 minutes'
                              AND mod(extract(epoch FROM s.snapshot_time)::bigint, 5) <> 0
                            )
                            OR (
                              s.snapshot_time >= p.creation_time + interval '60 minutes'
                              AND mod(extract(epoch FROM s.snapshot_time)::bigint, 60) <> 0
                            )
                          )
                        """
                    )
                )

    async def record_replay_run(
        self,
        *,
        run_id: str,
        strategy_version_id: str,
        config_hash: str,
        random_seed: int | None,
        input_hash: str,
        output_hash: str,
        speed: str,
        started_at: datetime,
        finished_at: datetime,
        result_json: dict[str, Any],
    ) -> None:
        async with self.sessions.begin() as session:
            row = await session.get(ReplayRunRow, run_id)
            values = {
                "strategy_version_id": strategy_version_id,
                "config_hash": config_hash,
                "random_seed": random_seed,
                "input_hash": input_hash,
                "output_hash": output_hash,
                "speed": speed,
                "started_at": started_at,
                "finished_at": finished_at,
                "result_json": result_json,
            }
            if row is None:
                session.add(ReplayRunRow(id=run_id, **values))
            else:
                for key, value in values.items():
                    setattr(row, key, value)

    async def save_runtime_checkpoint(
        self,
        *,
        checkpoint_key: str,
        state: dict[str, Any],
        updated_at: datetime,
    ) -> None:
        await self._upsert(
            RuntimeCheckpointRow,
            {
                "checkpoint_key": checkpoint_key,
                "state_json": state,
                "updated_at": updated_at,
            },
            ["checkpoint_key"],
        )

    async def load_runtime_checkpoint(
        self, checkpoint_key: str
    ) -> dict[str, Any] | None:
        async with self.sessions() as session:
            row = await session.get(RuntimeCheckpointRow, checkpoint_key)
        return dict(row.state_json) if row is not None else None

    async def initialize_paper_account(
        self, *, account_id: str, starting_equity: Decimal, now: datetime
    ) -> None:
        async with self.sessions.begin() as session:
            account = await session.get(PaperAccountRow, account_id)
            if account is None:
                account = PaperAccountRow(
                        id=account_id,
                        base_currency="USDC",
                        starting_equity=starting_equity,
                        cash_balance=starting_equity,
                        locked_capital=Decimal("0"),
                        realized_pnl=Decimal("0"),
                        unrealized_pnl=Decimal("0"),
                        simulated_costs=Decimal("0"),
                        operational_costs=Decimal("0"),
                        equity=starting_equity,
                        peak_equity=starting_equity,
                        drawdown_pct=Decimal("0"),
                        halt_reason=None,
                        pause_until=None,
                        daily_halt_date=None,
                        updated_at=now,
                    )
                session.add(account)
                self._add_paper_equity_mark(session, account, now)

    async def record_operational_cost(
        self,
        *,
        cost_id: str,
        account_id: str,
        category: str,
        amount_usd: Decimal,
        incurred_at: datetime,
        source_reference_sha256: str,
        recorded_at: datetime,
    ) -> bool:
        if amount_usd <= 0:
            raise ValueError("operational cost must be positive")
        source_reference_sha256 = source_reference_sha256.lower()
        if re.fullmatch(r"[0-9a-f]{64}", source_reference_sha256) is None:
            raise ValueError("operational cost source reference must be SHA-256")
        if incurred_at.tzinfo is None or recorded_at.tzinfo is None:
            raise ValueError("operational cost timestamps must be timezone-aware")
        async with self.sessions.begin() as session:
            account = await session.get(PaperAccountRow, account_id, with_for_update=True)
            if account is None:
                raise ValueError("paper account does not exist")
            active_run = await session.scalar(
                select(SystemRunRow.id).where(SystemRunRow.stopped_at.is_(None)).limit(1)
            )
            if active_run is not None:
                raise RuntimeError("stop the bot before recording operational costs")
            values = {
                "id": cost_id,
                "account_id": account_id,
                "category": category,
                "amount_usd": amount_usd,
                "incurred_at": incurred_at,
                "source_reference_sha256": source_reference_sha256,
                "created_at": recorded_at,
            }
            if session.bind is not None and session.bind.dialect.name == "postgresql":
                statement: Any = (
                    pg_insert(OperationalCostRow)
                    .values(**values)
                    .on_conflict_do_nothing()
                    .returning(OperationalCostRow.id)
                )
            else:
                statement = (
                    sqlite_insert(OperationalCostRow)
                    .values(**values)
                    .on_conflict_do_nothing()
                    .returning(OperationalCostRow.id)
                )
            inserted_id = (await session.execute(statement)).scalar_one_or_none()
            if inserted_id is None:
                existing = await session.scalar(
                    select(OperationalCostRow).where(
                        OperationalCostRow.source_reference_sha256 == source_reference_sha256
                    )
                )
                if existing is None:
                    raise ValueError("operational cost id conflicts with another source")
                existing_incurred_at = existing.incurred_at
                if existing_incurred_at.tzinfo is None:
                    existing_incurred_at = existing_incurred_at.replace(tzinfo=timezone.utc)
                if (
                    existing.account_id != account_id
                    or existing.category != category
                    or Decimal(existing.amount_usd).quantize(Decimal("0.000001"))
                    != amount_usd.quantize(Decimal("0.000001"))
                    or existing_incurred_at.astimezone(timezone.utc)
                    != incurred_at.astimezone(timezone.utc)
                ):
                    raise ValueError("operational cost retry does not match the original record")
                return False
            account.operational_costs += amount_usd
            account.cash_balance -= amount_usd
            account.equity -= amount_usd
            account.drawdown_pct = (
                Decimal("100")
                if account.peak_equity <= 0
                else max(
                    Decimal("0"),
                    (account.peak_equity - account.equity) / account.peak_equity * Decimal("100"),
                )
            )
            account.updated_at = recorded_at
            self._add_paper_equity_mark(session, account, recorded_at)
        return True

    async def load_paper_ledger(self, *, account_id: str) -> dict[str, Any] | None:
        async with self.sessions() as session:
            account = await session.get(PaperAccountRow, account_id)
            if account is None:
                return None
            orders = list((await session.scalars(select(PaperOrderRow))).all())
            fills = list((await session.scalars(select(PaperFillRow))).all())
            positions = list((await session.scalars(select(PaperPositionRow))).all())
            signals = list(
                (
                    await session.scalars(
                        select(SignalEvaluationRow).order_by(
                            SignalEvaluationRow.evaluated_at
                        )
                    )
                ).all()
            )
            return {
                "account": {
                    "starting_equity": account.starting_equity,
                    "equity": account.equity,
                    "peak_equity": account.peak_equity,
                    "realized_pnl": account.realized_pnl,
                    "unrealized_pnl": account.unrealized_pnl,
                    "locked_capital": account.locked_capital,
                    "halt_reason": account.halt_reason,
                    "pause_until": account.pause_until,
                    "daily_halt_date": account.daily_halt_date,
                },
                "orders": [
                    {
                        "id": row.id,
                        "position_id": row.position_id,
                        "side": row.side,
                        "quote_request_id": row.quote_request_id,
                        "candidate_id": row.candidate_id,
                    }
                    for row in orders
                ],
                "fills": [
                    {
                        "id": row.id,
                        "order_id": row.order_id,
                        "side": row.side,
                        "input_raw_amount": row.input_raw_amount,
                        "output_raw_amount": row.output_raw_amount,
                        "input_usd": row.input_usd,
                        "output_usd": row.output_usd,
                        "cost_basis_usd": row.cost_basis_usd,
                        "exit_reason": row.exit_reason,
                        "realized_pnl_usd": row.realized_pnl_usd,
                        "price_impact_pct": row.price_impact_pct,
                        "platform_fee_usd": row.platform_fee_usd,
                        "network_fee_usd": row.network_fee_usd,
                        "other_cost_usd": row.other_cost_usd,
                        "adverse_fill_bps": row.adverse_fill_bps,
                        "quote_latency_ms": 0,
                        "filled_at": row.filled_at,
                    }
                    for row in fills
                ],
                "positions": [
                    {
                        "id": row.id,
                        "mint": row.mint,
                        "status": row.status,
                        "entry_time": row.entry_time,
                        "closed_at": row.closed_at,
                        "initial_cost_usd": row.initial_cost_usd,
                        "remaining_cost_usd": row.remaining_cost_usd,
                        "initial_token_amount_raw": row.initial_token_amount_raw,
                        "token_amount_raw": row.token_amount_raw,
                        "open_fill_id": row.open_fill_id,
                        "realized_pnl": row.realized_pnl,
                        "mfe_pct": row.mfe_pct,
                        "mae_pct": row.mae_pct,
                        "highest_executable_value": row.highest_executable_value,
                        "lowest_executable_value": row.lowest_executable_value,
                        "tp1_taken": row.tp1_taken,
                        "tp2_taken": row.tp2_taken,
                        "last_new_high_at": row.last_new_high_at,
                        "exit_reason": row.exit_reason,
                        "pool_address": row.pool_address,
                        "strategy_version": row.strategy_version_id,
                        "candidate_id": next(
                            (
                                order.candidate_id
                                for order in orders
                                if order.position_id == row.id
                                and order.side == "BUY"
                            ),
                            None,
                        ),
                        "entry_score": next(
                            (
                                signal.score
                                for signal in reversed(signals)
                                if signal.candidate_id
                                == next(
                                    (
                                        order.candidate_id
                                        for order in orders
                                        if order.position_id == row.id
                                        and order.side == "BUY"
                                    ),
                                    None,
                                )
                            ),
                            None,
                        ),
                        "entry_liquidity_usd": next(
                            (
                                signal.features_json.get("quote_liquidity_usd")
                                for signal in reversed(signals)
                                if signal.candidate_id
                                == next(
                                    (
                                        order.candidate_id
                                        for order in orders
                                        if order.position_id == row.id
                                        and order.side == "BUY"
                                    ),
                                    None,
                                )
                            ),
                            None,
                        ),
                        "entry_pool_age_seconds": next(
                            (
                                signal.features_json.get("pool_age_seconds")
                                for signal in reversed(signals)
                                if signal.candidate_id
                                == next(
                                    (
                                        order.candidate_id
                                        for order in orders
                                        if order.position_id == row.id
                                        and order.side == "BUY"
                                    ),
                                    None,
                                )
                            ),
                            None,
                        ),
                    }
                    for row in positions
                ],
            }

    async def update_paper_marks(
        self,
        *,
        account_id: str,
        positions: list[Any],
        executable_values: dict[str, Decimal],
        account_snapshot: dict[str, Any],
        observed_at: datetime,
    ) -> None:
        async with self.sessions.begin() as session:
            account = await session.get(PaperAccountRow, account_id, with_for_update=True)
            if account is None:
                raise RuntimeError("paper account is unavailable")
            await self._assert_transaction_runtime_owner(session)
            account.equity = Decimal(str(account_snapshot["equity_usd"]))
            account.peak_equity = Decimal(str(account_snapshot["peak_equity_usd"]))
            account.unrealized_pnl = Decimal(str(account_snapshot["unrealized_pnl_usd"]))
            account.locked_capital = Decimal(str(account_snapshot["locked_capital_usd"]))
            account.drawdown_pct = (
                (account.peak_equity - account.equity) / account.peak_equity
                if account.peak_equity > 0 else Decimal("0")
            )
            account.updated_at = observed_at
            self._add_paper_equity_mark(session, account, observed_at)
            for item in positions:
                row = await session.get(PaperPositionRow, item.position_id, with_for_update=True)
                if row is None:
                    continue
                row.unrealized_pnl = (
                    executable_values.get(item.position_id, Decimal("0"))
                    - item.remaining_cost_usd
                )
                row.mfe_pct = item.mfe_pct
                row.mae_pct = item.mae_pct
                row.highest_executable_value = item.highest_executable_value_usd
                row.lowest_executable_value = item.lowest_executable_value_usd or Decimal("0")
                row.tp1_taken = item.tp1_taken
                row.tp2_taken = item.tp2_taken
                row.last_new_high_at = item.last_new_high_at

    @staticmethod
    def _add_paper_equity_mark(
        session: AsyncSession,
        account: PaperAccountRow,
        observed_at: datetime,
    ) -> None:
        session.add(
            PaperEquityMarkRow(
                id=str(uuid4()),
                account_id=account.id,
                equity=account.equity,
                realized_pnl=account.realized_pnl,
                unrealized_pnl=account.unrealized_pnl,
                locked_capital=account.locked_capital,
                observed_at=observed_at,
            )
        )

    async def load_daily_equity_bounds(
        self,
        *,
        account_id: str,
        report_date: str,
        timezone_name: str,
    ) -> dict[str, Any] | None:
        zone = ZoneInfo(timezone_name)
        start_local = datetime.combine(
            datetime.strptime(report_date, "%Y-%m-%d").date(),
            datetime.min.time(),
            tzinfo=zone,
        )
        start_utc = start_local.astimezone(timezone.utc)
        end_utc = (start_local + timedelta(days=1)).astimezone(timezone.utc)
        async with self.sessions() as session:
            start_mark = await session.scalar(
                select(PaperEquityMarkRow)
                .where(
                    PaperEquityMarkRow.account_id == account_id,
                    PaperEquityMarkRow.observed_at <= start_utc,
                )
                .order_by(
                    PaperEquityMarkRow.observed_at.desc(),
                    PaperEquityMarkRow.id.desc(),
                )
                .limit(1)
            )
            end_mark = await session.scalar(
                select(PaperEquityMarkRow)
                .where(
                    PaperEquityMarkRow.account_id == account_id,
                    PaperEquityMarkRow.observed_at < end_utc,
                )
                .order_by(
                    PaperEquityMarkRow.observed_at.desc(),
                    PaperEquityMarkRow.id.desc(),
                )
                .limit(1)
            )
            marks = list(
                (
                    await session.scalars(
                        select(PaperEquityMarkRow)
                        .where(
                            PaperEquityMarkRow.account_id == account_id,
                            PaperEquityMarkRow.observed_at >= start_utc,
                            PaperEquityMarkRow.observed_at < end_utc,
                        )
                        .order_by(
                            PaperEquityMarkRow.observed_at,
                            PaperEquityMarkRow.id,
                        )
                    )
                ).all()
            )
        if end_mark is None:
            return None
        first_mark = marks[0] if marks else end_mark
        baseline_mark = start_mark if start_mark is not None else first_mark
        return {
            "starting_equity_usd": baseline_mark.equity,
            "starting_unrealized_pnl_usd": baseline_mark.unrealized_pnl,
            "ending_equity_usd": end_mark.equity,
            "ending_unrealized_pnl_usd": end_mark.unrealized_pnl,
            "equity_path_usd": [baseline_mark.equity, *(mark.equity for mark in marks)],
            "ending_observed_at": end_mark.observed_at,
        }

    async def load_all_time_max_drawdown_pct(
        self, *, account_id: str
    ) -> Decimal:
        peak = Decimal("0")
        maximum = Decimal("0")
        async with self.sessions() as session:
            stream = await session.stream_scalars(
                select(PaperEquityMarkRow.equity)
                .where(PaperEquityMarkRow.account_id == account_id)
                .order_by(PaperEquityMarkRow.observed_at, PaperEquityMarkRow.id)
            )
            async for value in stream:
                equity = Decimal(str(value))
                peak = max(peak, equity)
                if peak > 0:
                    maximum = max(maximum, (peak - equity) / peak)
        return maximum

    async def persist_risk_state(
        self,
        *,
        account_id: str,
        halt_reason: str | None,
        pause_until: datetime | None,
        daily_halt_date: str | None,
        updated_at: datetime,
    ) -> None:
        async with self.sessions.begin() as session:
            account = await session.get(PaperAccountRow, account_id, with_for_update=True)
            if account is None:
                raise RuntimeError("paper account is unavailable")
            await self._assert_transaction_runtime_owner(session)
            account.halt_reason = halt_reason
            account.pause_until = pause_until
            account.daily_halt_date = daily_halt_date
            account.updated_at = updated_at

    async def commit_paper_entry(
        self,
        *,
        account_id: str,
        strategy_version_id: str,
        config_hash: str,
        candidate_id: str,
        pool_address: str,
        mint: str,
        order_id: str,
        position_id: str,
        fill_id: str,
        quote: QuoteResponse,
        requested_usd: Decimal,
        filled_token_amount: Decimal,
        adverse_fill_bps: int,
        filled_at: datetime,
        outbox_text: str,
        max_position_usdc: Decimal = Decimal("20"),
        max_exposure_usdc: Decimal = Decimal("50"),
        max_open_positions: int = 3,
        daily_loss_limit_usdc: Decimal = Decimal("10"),
        max_trades_per_day: int = 12,
        risk_day_key: str | None = None,
        risk_day_start: datetime | None = None,
        risk_day_end: datetime | None = None,
    ) -> dict[str, Any]:
        """Atomically commit order, fill, position, account, risk audit and outbox."""
        risk_day_key = risk_day_key or filled_at.date().isoformat()
        risk_day_start = risk_day_start or filled_at.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        risk_day_end = risk_day_end or (risk_day_start + timedelta(days=1))
        async with self.sessions.begin() as session:
            existing = await session.scalar(
                select(PaperOrderRow).where(PaperOrderRow.idempotency_key == order_id)
            )
            if existing is not None:
                fill = await session.scalar(
                    select(PaperFillRow).where(PaperFillRow.order_id == existing.id)
                )
                if fill is None or existing.position_id is None:
                    raise RuntimeError("paper entry idempotency record is incomplete")
                return {
                    "position_id": existing.position_id,
                    "fill_id": fill.id,
                    "quote_id": existing.quote_request_id or "",
                    "filled_at": fill.filled_at,
                }
            account = await session.get(PaperAccountRow, account_id, with_for_update=True)
            if account is None:
                raise RuntimeError("paper account is not initialized")
            await self._assert_transaction_runtime_owner(session)
            if account.pause_until is not None and _as_utc(account.pause_until) <= filled_at:
                account.pause_until = None
            if account.daily_halt_date and account.daily_halt_date != risk_day_key:
                if account.halt_reason and account.halt_reason.startswith("daily:"):
                    account.halt_reason = None
                account.daily_halt_date = None
            if account.halt_reason is not None:
                raise RuntimeError("risk limit: account halted")
            if account.pause_until is not None:
                raise RuntimeError("risk limit: consecutive-loss pause")
            entry_cost = requested_usd + quote.estimated_network_fee_usd
            if requested_usd > max_position_usdc:
                raise RuntimeError("risk limit: maximum position")
            if account.locked_capital + entry_cost > max_exposure_usdc:
                raise RuntimeError("risk limit: maximum exposure")
            open_count = await session.scalar(
                select(func.count(PaperPositionRow.id)).where(
                    PaperPositionRow.status.in_(["OPEN", "PARTIAL"])
                )
            )
            if int(open_count or 0) >= max_open_positions:
                raise RuntimeError("risk limit: maximum open positions")
            trades_today = await session.scalar(
                select(func.count(PaperOrderRow.id)).where(
                    PaperOrderRow.side == "BUY",
                    PaperOrderRow.filled_at >= risk_day_start,
                    PaperOrderRow.filled_at < risk_day_end,
                )
            )
            if int(trades_today or 0) >= max_trades_per_day:
                raise RuntimeError("risk limit: maximum trades per day")
            baseline_equity = await session.scalar(
                select(PaperEquityMarkRow.equity)
                .where(
                    PaperEquityMarkRow.account_id == account_id,
                    PaperEquityMarkRow.observed_at <= risk_day_start,
                )
                .order_by(PaperEquityMarkRow.observed_at.desc())
                .limit(1)
            )
            if baseline_equity is None:
                baseline_equity = await session.scalar(
                    select(PaperEquityMarkRow.equity)
                    .where(
                        PaperEquityMarkRow.account_id == account_id,
                        PaperEquityMarkRow.observed_at < risk_day_end,
                    )
                    .order_by(PaperEquityMarkRow.observed_at)
                    .limit(1)
                )
            if baseline_equity is not None and (
                account.equity - Decimal(str(baseline_equity))
                <= -abs(daily_loss_limit_usdc)
            ):
                account.daily_halt_date = risk_day_key
                account.halt_reason = f"daily:{risk_day_key}:DAILY_LOSS_LIMIT"
                raise RuntimeError("risk limit: daily loss")
            duplicate_position = await session.scalar(
                select(PaperPositionRow.id).where(
                    PaperPositionRow.mint == mint,
                    PaperPositionRow.status.in_(["OPEN", "PARTIAL"]),
                )
            )
            if duplicate_position is not None:
                raise RuntimeError("an open paper position already exists for mint")
            if account.cash_balance < entry_cost:
                raise RuntimeError("paper account has insufficient cash")

            quote_id = str(uuid4())
            session.add(_external_quote_row(quote_id, quote))
            session.add(
                PaperOrderRow(
                    id=order_id,
                    idempotency_key=order_id,
                    candidate_id=candidate_id,
                    position_id=position_id,
                    side="BUY",
                    status="FILLED",
                    requested_usd=requested_usd,
                    requested_token_raw=None,
                    quote_request_id=quote_id,
                    created_at=quote.requested_at,
                    filled_at=filled_at,
                    rejected_at=None,
                    reject_reason=None,
                )
            )
            quoted_price = requested_usd / quote.out_amount if quote.out_amount > 0 else Decimal("0")
            execution_price = entry_cost / filled_token_amount
            other_cost = requested_usd * Decimal(adverse_fill_bps) / Decimal("10000")
            session.add(
                PaperFillRow(
                    id=fill_id,
                    order_id=order_id,
                    side="BUY",
                    input_raw_amount=quote.in_amount,
                    output_raw_amount=filled_token_amount,
                    input_usd=entry_cost,
                    output_usd=requested_usd,
                    execution_price=execution_price,
                    quoted_price=quoted_price,
                    adverse_fill_bps=adverse_fill_bps,
                    price_impact_pct=quote.price_impact_pct,
                    platform_fee_usd=quote.platform_fee_usd,
                    network_fee_usd=quote.estimated_network_fee_usd,
                    other_cost_usd=other_cost,
                    cost_basis_usd=execution_price,
                    realized_pnl_usd=Decimal("0"),
                    exit_reason=None,
                    strategy_version_id=strategy_version_id,
                    config_hash=config_hash,
                    filled_at=filled_at,
                )
            )
            session.add(
                PaperPositionRow(
                    id=position_id,
                    mint=mint,
                    pool_address=pool_address,
                    status="OPEN",
                    strategy_version_id=strategy_version_id,
                    entry_time=filled_at,
                    closed_at=None,
                    initial_cost_usd=entry_cost,
                    remaining_cost_usd=entry_cost,
                    initial_token_amount_raw=filled_token_amount,
                    token_amount_raw=filled_token_amount,
                    open_fill_id=fill_id,
                    realized_pnl=Decimal("0"),
                    unrealized_pnl=Decimal("0"),
                    mfe_pct=Decimal("0"),
                    mae_pct=Decimal("0"),
                    highest_executable_value=entry_cost,
                    lowest_executable_value=entry_cost,
                    tp1_taken=False,
                    tp2_taken=False,
                    last_new_high_at=filled_at,
                    config_hash=config_hash,
                    exit_reason=None,
                )
            )
            account.cash_balance -= entry_cost
            account.locked_capital += entry_cost
            account.simulated_costs += quote.platform_fee_usd + quote.estimated_network_fee_usd + other_cost
            account.updated_at = filled_at
            self._add_paper_equity_mark(session, account, filled_at)
            session.add(
                RiskEventRow(
                    id=str(uuid4()), event_type="ENTRY_RISK_APPROVED", severity="INFO",
                    position_id=position_id, candidate_id=candidate_id,
                    details_json={"requested_usd": str(requested_usd)},
                    created_at=filled_at, resolved_at=filled_at,
                )
            )
            await self._insert_outbox(
                session,
                {
                    "id": str(uuid4()),
                    "idempotency_key": f"telegram:entry:{fill_id}",
                    "event_type": "paper_position_opened",
                    "payload_json": {"text": outbox_text},
                    "created_at": filled_at,
                    "available_at": filled_at,
                    "attempts": 0,
                },
            )
            return {
                "position_id": position_id,
                "fill_id": fill_id,
                "quote_id": quote_id,
                "filled_at": filled_at,
            }

    async def commit_paper_exit(
        self,
        *,
        account_id: str,
        strategy_version_id: str,
        config_hash: str,
        mint: str,
        order_id: str,
        fill_id: str,
        token_amount: Decimal,
        usd_received: Decimal,
        quote: QuoteResponse | None,
        adverse_fill_bps: int,
        exit_reason: str,
        filled_at: datetime,
        outbox_text: str,
    ) -> dict[str, Any]:
        async with self.sessions.begin() as session:
            existing = await session.scalar(
                select(PaperOrderRow).where(PaperOrderRow.idempotency_key == order_id)
            )
            if existing is not None:
                fill = await session.scalar(
                    select(PaperFillRow).where(PaperFillRow.order_id == existing.id)
                )
                if fill is None or existing.position_id is None:
                    raise RuntimeError("paper exit idempotency record is incomplete")
                return {
                    "position_id": existing.position_id,
                    "fill_id": fill.id,
                    "quote_id": existing.quote_request_id or "",
                    "filled_at": fill.filled_at,
                }
            position = await session.scalar(
                select(PaperPositionRow)
                .where(
                    PaperPositionRow.mint == mint,
                    PaperPositionRow.status.in_(["OPEN", "PARTIAL"]),
                )
                .with_for_update()
            )
            account = await session.get(PaperAccountRow, account_id, with_for_update=True)
            if position is None or account is None:
                raise RuntimeError("paper position or account is unavailable")
            await self._assert_transaction_runtime_owner(session)
            if token_amount <= 0 or token_amount > position.token_amount_raw:
                raise RuntimeError("invalid paper exit token amount")
            previous_token_amount = position.token_amount_raw
            closed_fraction = token_amount / previous_token_amount
            remaining_fraction = Decimal("1") - closed_fraction
            closed_unrealized = position.unrealized_pnl * closed_fraction
            cost_per_token = position.remaining_cost_usd / position.token_amount_raw
            proportional_cost = cost_per_token * token_amount
            network_fee = (
                quote.estimated_network_fee_usd if quote is not None else Decimal("0")
            )
            net_received = max(Decimal("0"), usd_received - network_fee)
            realized_delta = net_received - proportional_cost
            quote_id: str | None = None
            if quote is not None:
                quote_id = str(uuid4())
                session.add(_external_quote_row(quote_id, quote))
            session.add(
                PaperOrderRow(
                    id=order_id, idempotency_key=order_id, candidate_id=None,
                    position_id=position.id, side="SELL",
                    status="FILLED" if quote is not None else "EXPIRED",
                    requested_usd=None, requested_token_raw=token_amount,
                    quote_request_id=quote_id, created_at=filled_at, filled_at=filled_at,
                    rejected_at=None, reject_reason=None if quote is not None else "NO_SELL_ROUTE",
                )
            )
            quoted_output = (
                (quote.out_amount_usd if quote.out_amount_usd and quote.out_amount_usd > 0 else quote.out_amount)
                if quote is not None else Decimal("0")
            )
            quoted_price = quoted_output / token_amount if token_amount > 0 else Decimal("0")
            execution_price = net_received / token_amount if token_amount > 0 else Decimal("0")
            other_cost = max(Decimal("0"), quoted_output - usd_received)
            session.add(
                PaperFillRow(
                    id=fill_id, order_id=order_id, side="SELL",
                    input_raw_amount=token_amount,
                    output_raw_amount=(
                        quote.out_amount
                        * (Decimal("1") - Decimal(adverse_fill_bps) / Decimal("10000"))
                        if quote else Decimal("0")
                    ),
                    input_usd=proportional_cost, output_usd=net_received,
                    execution_price=execution_price, quoted_price=quoted_price,
                    adverse_fill_bps=adverse_fill_bps,
                    price_impact_pct=quote.price_impact_pct if quote else Decimal("1"),
                    platform_fee_usd=quote.platform_fee_usd if quote else Decimal("0"),
                    network_fee_usd=quote.estimated_network_fee_usd if quote else Decimal("0"),
                    other_cost_usd=other_cost, cost_basis_usd=cost_per_token,
                    realized_pnl_usd=realized_delta, exit_reason=exit_reason,
                    strategy_version_id=strategy_version_id, config_hash=config_hash,
                    filled_at=filled_at,
                )
            )
            position.token_amount_raw -= token_amount
            position.remaining_cost_usd -= proportional_cost
            position.realized_pnl += realized_delta
            position.unrealized_pnl -= closed_unrealized
            position.highest_executable_value *= remaining_fraction
            position.lowest_executable_value *= remaining_fraction
            if exit_reason == "TP1":
                position.tp1_taken = True
            elif exit_reason == "TP2":
                position.tp2_taken = True
            final = position.token_amount_raw <= 0
            position.status = "UNRECOVERABLE" if quote is None else ("CLOSED" if final else "PARTIAL")
            position.closed_at = filled_at if final or quote is None else None
            position.exit_reason = exit_reason if final or quote is None else None
            account.cash_balance += net_received
            account.locked_capital -= proportional_cost
            account.realized_pnl += realized_delta
            account.unrealized_pnl -= closed_unrealized
            account.equity += realized_delta - closed_unrealized
            account.peak_equity = max(account.peak_equity, account.equity)
            account.drawdown_pct = (
                (account.peak_equity - account.equity) / account.peak_equity
                if account.peak_equity > 0 else Decimal("0")
            )
            account.simulated_costs += (
                (quote.platform_fee_usd if quote is not None else Decimal("0"))
                + network_fee
                + other_cost
            )
            account.updated_at = filled_at
            self._add_paper_equity_mark(session, account, filled_at)
            if quote is None:
                session.add(
                    RiskEventRow(
                        id=str(uuid4()), event_type="UNRECOVERABLE_EXIT", severity="CRITICAL",
                        position_id=position.id, candidate_id=None,
                        details_json={"mint": mint, "loss_usd": str(proportional_cost)},
                        created_at=filled_at, resolved_at=None,
                    )
                )
            await self._insert_outbox(
                session,
                {
                    "id": str(uuid4()),
                    "idempotency_key": f"telegram:exit:{fill_id}",
                    "event_type": "paper_position_closed" if final else "paper_position_reduced",
                    "payload_json": {"text": outbox_text},
                    "created_at": filled_at, "available_at": filled_at, "attempts": 0,
                },
            )
            return {
                "position_id": position.id,
                "fill_id": fill_id,
                "quote_id": quote_id or "unrecoverable",
                "filled_at": filled_at,
            }

    async def _upsert(
        self,
        model: Any,
        values: dict[str, Any],
        keys: list[str],
    ) -> None:
        if self._event_state_batch.get() is not None:
            self._stage_upsert(model, values, keys)
            return
        async with self._write_session() as session:
            await self._execute_upsert_rows(
                session,
                model,
                [values],
                tuple(keys),
            )

    async def _execute_upsert_rows(
        self,
        session: AsyncSession,
        model: Any,
        rows: list[dict[str, Any]],
        keys: tuple[str, ...],
    ) -> None:
        if not rows:
            return
        dialect = (
            session.bind.dialect.name
            if session.bind is not None
            else ""
        )
        for chunk in _bulk_insert_chunks(rows, dialect=dialect):
            if dialect == "postgresql":
                statement: Any = pg_insert(model).values(chunk)
                update_values = {
                    column: statement.excluded[column]
                    for column in chunk[0]
                    if column not in keys
                }
                statement = statement.on_conflict_do_update(
                    index_elements=list(keys),
                    set_=update_values,
                )
                await session.execute(statement)
            elif dialect == "sqlite":
                statement = sqlite_insert(model).values(chunk)
                update_values = {
                    column: statement.excluded[column]
                    for column in chunk[0]
                    if column not in keys
                }
                statement = statement.on_conflict_do_update(
                    index_elements=list(keys),
                    set_=update_values,
                )
                await session.execute(statement)
            else:
                for values in chunk:
                    await session.merge(model(**values))
    async def _insert_outbox(self, session: AsyncSession, values: dict[str, Any]) -> bool:
        dialect = session.bind.dialect.name if session.bind is not None else ""
        if dialect == "postgresql":
            statement: Any = pg_insert(OutboxEventRow).values(**values).on_conflict_do_nothing(
                index_elements=[OutboxEventRow.idempotency_key]
            )
        elif dialect == "sqlite":
            statement = sqlite_insert(OutboxEventRow).values(**values).on_conflict_do_nothing(
                index_elements=[OutboxEventRow.idempotency_key]
            )
        else:
            existing = await session.scalar(
                select(OutboxEventRow.id).where(OutboxEventRow.idempotency_key == values["idempotency_key"])
            )
            if existing:
                return False
            session.add(OutboxEventRow(**values))
            return True
        result = await session.execute(statement)
        rowcount = getattr(result, "rowcount", None)
        return isinstance(rowcount, int) and rowcount > 0

    def _observe_query(self, started: float) -> None:
        if self.metrics is not None:
            self.metrics.database_query_latency_ms.observe((time.perf_counter() - started) * 1000)


def _async_dsn(dsn: str) -> str:
    if dsn.startswith("postgresql+asyncpg://") or dsn.startswith("sqlite+aiosqlite://"):
        return dsn
    if dsn.startswith("postgresql://"):
        return dsn.replace("postgresql://", "postgresql+asyncpg://", 1)
    if dsn.startswith("sqlite://"):
        return dsn.replace("sqlite://", "sqlite+aiosqlite://", 1)
    return dsn


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _event_from_row(row: RawChainEventRow) -> EventEnvelope:
    return EventEnvelope.model_validate(
        {
            "event_id": row.event_id,
            "source": row.source,
            "protocol": row.protocol,
            "event_type": row.event_type,
            "slot": row.slot,
            "signature": row.signature,
            "instruction_index": row.instruction_index,
            "inner_instruction_index": row.inner_instruction_index,
            "block_time": _as_utc(row.block_time),
            "observed_at": _as_utc(row.observed_at),
            "commitment": row.commitment,
            "mint": row.mint,
            "pool_address": row.pool_address,
            "payload": row.payload_json,
        }
    )


def _external_quote_row(row_id: str, quote: QuoteResponse) -> ExternalApiCallRow:
    import hashlib
    import json

    request = {
        "token_in": quote.token_in,
        "token_out": quote.token_out,
        "in_amount": str(quote.in_amount),
    }
    return ExternalApiCallRow(
        id=row_id,
        provider="jupiter",
        endpoint="/swap/v2/order",
        request_hash=hashlib.sha256(
            json.dumps(request, sort_keys=True).encode("utf-8")
        ).hexdigest(),
        requested_at=quote.requested_at,
        received_at=quote.received_at,
        latency_ms=quote.latency_ms,
        http_status=200,
        request_json=request,
        response_json=quote.raw,
        error_code=None,
    )


def _telegram_report_text(report: dict[str, Any]) -> str:
    if report.get("period") == "daily":
        capital = report.get("capital") or {}
        signals = report.get("signals") or {}
        trades = report.get("trades") or {}
        starting = _report_decimal(capital.get("starting_equity_usd"))
        ending = _report_decimal(capital.get("ending_equity_usd"))
        day_result = ending - starting
        open_positions = report.get("open_positions")
        open_count = len(open_positions) if isinstance(open_positions, list) else 0
        return (
            "Щоденний звіт про тестову торгівлю\n"
            f"Дата: {report.get('date')}\n"
            f"Баланс: {_format_usd(starting)} -> {_format_usd(ending)}\n"
            f"Результат дня: {_format_usd(day_result, signed=True)}\n"
            "Закритий PnL: "
            f"{_format_usd(capital.get('realized_pnl_usd'), signed=True)}\n"
            "Відкритий PnL: "
            f"{_format_usd(capital.get('unrealized_pnl_usd'), signed=True)}\n"
            f"Угоди: відкрито {_report_int(signals.get('paper_entries'))}, "
            f"закрито {_report_int(trades.get('closed'))}\n"
            f"Результати: прибуткових {_report_int(trades.get('profitable'))}, "
            f"збиткових {_report_int(trades.get('losing'))}\n"
            f"Частка прибуткових: {_format_percent(trades.get('win_rate'))}\n"
            f"Відкриті позиції: {open_count}"
        )
    stats = report.get("trade_statistics") or {}
    return (
        "ALL-TIME PAPER REPORT\n"
        f"equity={report.get('current_equity_usd')} pnl={report.get('net_pnl_usd')} "
        f"return={report.get('return_pct')} drawdown={report.get('max_drawdown_pct')}\n"
        f"entries={report.get('paper_entries')} closed={stats.get('closed')} "
        f"win_rate={stats.get('win_rate')} profit_factor={stats.get('profit_factor')}\n"
        f"report_id={report.get('report_id')}"
    )


def _report_decimal(value: object) -> Decimal:
    try:
        return Decimal(str(value if value is not None else "0"))
    except (ArithmeticError, TypeError, ValueError):
        return Decimal("0")


def _report_int(value: object) -> int:
    try:
        if isinstance(value, (str, int, float, Decimal)):
            return int(value)
        return 0
    except (TypeError, ValueError):
        return 0


def _format_usd(value: object, *, signed: bool = False) -> str:
    amount = _report_decimal(value)
    sign = "-" if amount < 0 else "+" if signed and amount > 0 else ""
    return f"{sign}${abs(amount):,.2f}"


def _format_percent(value: object) -> str:
    return f"{_report_decimal(value) * Decimal('100'):.1f}%"
