"""End-to-end record/feature/decision pipeline for decoded chain events."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from .candidates import Candidate, CandidateState, CandidateStateMachine
from .config import AppConfig
from .database import MAX_EVENT_BATCH_SIZE, Database, EventRecordResult
from .events import (
    ChainEventType,
    EventDeduplicator,
    EventEnvelope,
    EventSource,
    Protocol,
    RawEventRecorder,
)
from .features import (
    EventTimeFeatureEngine,
    FeatureSnapshot,
    LiquidityObservation,
    TradeObservation,
    TradeSide,
)
from .metrics import BotMetrics
from .protocols import AnchorDecodeError
from .protocols.pump import PumpDecoder
from .protocols.pumpswap import PumpSwapDecoder
from .registry import (
    SUPPORTED_QUOTE_MINTS,
    PoolState,
    PoolStateTracker,
    TokenRegistry,
)
from .scoring import DeveloperHistory, ScoreBreakdown, ScoreContext, ScoringEngine
from .security import RejectReason, SecurityContext, SecurityEngine, SecurityResult
from .stream import EntryGate

NON_TRADABLE_EVENT_SOURCES = frozenset(
    {
        EventSource.BASELINE_WSS,
        EventSource.RPC_RECOVERY,
    }
)

logger = logging.getLogger(__name__)
EVENT_LOOP_YIELD_INTERVAL = 32

SecurityProvider = Callable[[Candidate, FeatureSnapshot], Awaitable[SecurityContext]]
EntryHandler = Callable[
    [Candidate, FeatureSnapshot, ScoreBreakdown, SecurityContext],
    Awaitable[RejectReason | None],
]
EventObserver = Callable[[EventEnvelope], Awaitable[None]]
FatalHandler = Callable[[BaseException], None]


@dataclass(slots=True)
class _StageBatch:
    events: list[EventEnvelope]
    enqueued_at: float


class ConfirmationPipeline:
    def __init__(
        self,
        *,
        data_dir: str,
        strategy_version: str,
        config_hash: str,
        entry_gate: EntryGate,
        metrics: BotMetrics,
        database: Database | None = None,
        security_provider: SecurityProvider | None = None,
        entry_handler: EntryHandler | None = None,
        event_observer: EventObserver | None = None,
        fatal_handler: FatalHandler | None = None,
        record_raw: bool = True,
        config: AppConfig | None = None,
    ) -> None:
        self.strategy_version = strategy_version
        self.config_hash = config_hash
        self.entry_gate = entry_gate
        self.metrics = metrics
        self.database = database
        self.recorder = RawEventRecorder(f"{data_dir}/raw")
        self.deduplicator = EventDeduplicator()
        self.tokens = TokenRegistry()
        self.pools = PoolStateTracker()
        self.features = EventTimeFeatureEngine()
        self.config = config
        self.security = SecurityEngine(
            minimum_quote_liquidity_usd=(
                config.liquidity.min_quote_liquidity_usd if config else Decimal("40000")
            ),
            minimum_pool_age_seconds=(
                Decimal(config.candidate.min_pool_age_seconds) if config else Decimal("45")
            ),
            maximum_pool_age_seconds=(
                Decimal(config.candidate.max_pool_age_seconds) if config else Decimal("180")
            ),
            maximum_round_trip_loss_pct=(
                config.execution.max_round_trip_loss_pct if config else Decimal("0.08")
            ),
            maximum_buy_price_impact_pct=(
                config.execution.max_buy_price_impact_pct if config else Decimal("0.025")
            ),
            maximum_sell_price_impact_pct=(
                config.execution.max_sell_price_impact_pct if config else Decimal("0.035")
            ),
            minimum_external_sellers=(config.execution.min_external_sellers if config else 5),
            maximum_largest_holder_pct=(
                config.holders.max_largest_holder_pct if config else Decimal("0.07")
            ),
            maximum_top_5_pct=(config.holders.max_top_5_pct if config else Decimal("0.22")),
            maximum_top_10_pct=(config.holders.max_top_10_pct if config else Decimal("0.30")),
            maximum_dev_holding_pct=(
                config.holders.max_dev_holding_pct if config else Decimal("0.02")
            ),
            maximum_dev_cluster_pct=(
                config.holders.max_dev_cluster_pct if config else Decimal("0.05")
            ),
            maximum_related_cluster_pct=(
                config.holders.max_related_cluster_pct if config else Decimal("0.15")
            ),
            maximum_unknown_supply_pct=(
                config.holders.max_unknown_supply_pct if config else Decimal("0.05")
            ),
            maximum_liquidity_drop_pct=(
                config.liquidity.max_liquidity_drop_entry_30s_pct
                if config
                else Decimal("0.03")
            ),
            maximum_return_since_creation=(
                config.candidate.max_return_since_pool_creation_pct
                if config
                else Decimal("2.50")
            ),
            maximum_stream_age_seconds=(
                Decimal(config.chain.max_stream_lag_ms) / Decimal("1000")
                if config
                else Decimal("3")
            ),
            maximum_quote_age_seconds=(
                Decimal(config.execution.max_quote_age_ms) / Decimal("1000")
                if config
                else Decimal("1.5")
            ),
        )
        self.scoring = ScoringEngine()
        self.state_machine = CandidateStateMachine(
            collect_seconds=(config.candidate.min_observation_seconds if config else 45),
            expiry_seconds=(config.candidate.max_pool_age_seconds if config else 180),
            minimum_score=(config.candidate.score_entry if config else Decimal("80")),
            required_confirmations=(
                config.candidate.score_confirmation_windows if config else 2
            ),
            score_window_seconds=(config.candidate.score_window_seconds if config else 5),
            minimum_pullback=(config.candidate.min_pullback_pct if config else Decimal("0.10")),
            maximum_pullback=(config.candidate.max_pullback_pct if config else Decimal("0.25")),
            maximum_liquidity_drop=(
                config.liquidity.max_liquidity_drop_entry_30s_pct
                if config
                else Decimal("0.03")
            ),
            minimum_buyer_acceleration=(
                config.flow.min_buyer_acceleration if config else Decimal("1.3")
            ),
        )
        self.candidates: dict[str, Candidate] = {}
        self.security_provider = security_provider
        self.entry_handler = entry_handler
        self.event_observer = event_observer
        self.fatal_handler = fatal_handler
        self.record_raw = record_raw
        self._pump = PumpDecoder()
        self._pumpswap = PumpSwapDecoder()
        self._security_results: dict[str, tuple[SecurityContext, SecurityResult]] = {}
        self._scores: dict[str, ScoreBreakdown] = {}
        self._persisted_score_totals: dict[str, Decimal] = {}
        self._state_poisoned = False
        self._durable_queue: asyncio.Queue[_StageBatch | None] = (
            asyncio.Queue(maxsize=128)
        )
        self._state_queue: asyncio.Queue[_StageBatch | None] = (
            asyncio.Queue(maxsize=128)
        )
        self._archive_queue: asyncio.Queue[_StageBatch | None] = (
            asyncio.Queue(maxsize=128)
        )
        self._stage_tasks: list[asyncio.Task[None]] = []
        self._stage_metrics_task: asyncio.Task[None] | None = None
        self._stage_pending: dict[str, deque[_StageBatch]] = {
            "durable": deque(),
            "state": deque(),
            "archive": deque(),
        }
        self._background_workers_started = False

    def _require_consistent_state(self) -> None:
        if self._state_poisoned:
            raise RuntimeError("pipeline state is inconsistent; restart required")

    def _poison_state(self) -> None:
        self._state_poisoned = True
        self.entry_gate.block("event_processing_error")

    async def start_background_workers(self) -> None:
        if self._background_workers_started or self.database is None:
            return
        self.entry_gate.block("archive_recovery")
        try:
            if self.record_raw:
                after_sequence = (
                    await self.database.last_archived_sequence()
                )
                while True:
                    events = await self.database.load_events_for_archive(
                        after_sequence=after_sequence,
                        limit=MAX_EVENT_BATCH_SIZE,
                    )
                    if not events:
                        break
                    segments = await self.recorder.write_segments(events)
                    await self.database.record_raw_archive_segments(
                        segments
                    )
                    after_sequence = max(
                        int(event.ingest_sequence or 0)
                        for event in events
                    )
        finally:
            self.entry_gate.unblock("archive_recovery")
        self._background_workers_started = True
        self._stage_tasks = [
            asyncio.create_task(
                self._durable_worker(),
                name="durable-ingest-worker",
            ),
            asyncio.create_task(
                self._state_worker(),
                name="state-apply-worker",
            ),
        ]
        if self.record_raw:
            self._stage_tasks.append(
                asyncio.create_task(
                    self._archive_worker(),
                    name="raw-archive-worker",
                )
            )
        self._stage_metrics_task = asyncio.create_task(
            self._stage_metrics_worker(),
            name="ordered-stage-metrics",
        )
        self._sync_stage_metrics()

    async def stop_background_workers(
        self,
        *,
        timeout_seconds: float = 120.0,
    ) -> None:
        if not self._background_workers_started:
            return
        started = asyncio.get_running_loop().time()
        try:
            async with asyncio.timeout(timeout_seconds):
                await self._durable_queue.join()
                await self._state_queue.join()
                if self.record_raw:
                    await self._archive_queue.join()
                await self._durable_queue.put(None)
                await self._state_queue.put(None)
                if self.record_raw:
                    await self._archive_queue.put(None)
                await asyncio.gather(*self._stage_tasks)
        except TimeoutError as exc:
            self.entry_gate.block("shutdown_drain_timeout")
            raise RuntimeError(
                "ordered ingestion queues did not drain within "
                f"{timeout_seconds:g} seconds"
            ) from exc
        finally:
            metrics_task = self._stage_metrics_task
            if metrics_task is not None:
                metrics_task.cancel()
                try:
                    await metrics_task
                except asyncio.CancelledError:
                    pass
                self._stage_metrics_task = None
            self.metrics.shutdown_drain_seconds.labels(
                stage="pipeline"
            ).observe(
                asyncio.get_running_loop().time() - started
            )
            self._sync_stage_metrics()
        self._stage_tasks = []
        self._background_workers_started = False

    async def _stage_metrics_worker(self) -> None:
        while True:
            self._sync_stage_metrics()
            await asyncio.sleep(1)

    async def _enqueue_stage(
        self,
        stage: str,
        queue: asyncio.Queue[_StageBatch | None],
        events: list[EventEnvelope],
    ) -> None:
        item = _StageBatch(
            events=list(events),
            enqueued_at=time.monotonic(),
        )
        pending = self._stage_pending[stage]
        pending.append(item)
        self._sync_stage_metrics()
        try:
            await queue.put(item)
        except BaseException:
            pending.remove(item)
            self._sync_stage_metrics()
            raise

    def _complete_stage(self, stage: str, item: _StageBatch) -> None:
        pending = self._stage_pending[stage]
        if pending and pending[0] is item:
            pending.popleft()
        else:
            self.entry_gate.block("stage_tracking_error")
            logger.error("ordered stage tracking lost FIFO identity stage=%s", stage)
            try:
                pending.remove(item)
            except ValueError:
                pass
        self._sync_stage_metrics()

    def _sync_stage_metrics(self) -> None:
        now = time.monotonic()
        for stage, pending in self._stage_pending.items():
            self.metrics.ingestion_backlog_events.labels(
                stage=stage
            ).set(sum(len(item.events) for item in pending))
            oldest_age = (
                max(0.0, now - pending[0].enqueued_at)
                if pending
                else 0.0
            )
            self.metrics.ingestion_oldest_event_age_seconds.labels(
                stage=stage
            ).set(oldest_age)

    async def _durable_worker(self) -> None:
        while True:
            item = await self._durable_queue.get()
            if item is None:
                self._durable_queue.task_done()
                return
            try:
                while True:
                    try:
                        if self.database is None:
                            raise RuntimeError(
                                "durable worker requires a database"
                            )
                        results = await self.database.record_events(
                            item.events,
                            resume_owned=True,
                        )
                        claimed = [
                            event
                            for event, result in zip(
                                item.events,
                                results,
                                strict=True,
                            )
                            if result
                        ]
                        if claimed:
                            await self.database.save_stream_protocol_checkpoints(
                                claimed,
                                stage="durable",
                            )
                            await self._enqueue_stage(
                                "state",
                                self._state_queue,
                                claimed,
                            )
                            if self.record_raw:
                                await self._enqueue_stage(
                                    "archive",
                                    self._archive_queue,
                                    claimed,
                                )
                        self.entry_gate.unblock(
                            "durable_ingest_error"
                        )
                        break
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        self.entry_gate.block(
                            "durable_ingest_error"
                        )
                        logger.exception(
                            "durable ingest failed; "
                            "retrying ordered batch"
                        )
                        await asyncio.sleep(1)
            finally:
                self._durable_queue.task_done()
                self._complete_stage("durable", item)

    async def _state_worker(self) -> None:
        while True:
            item = await self._state_queue.get()
            if item is None:
                self._state_queue.task_done()
                return
            try:
                results = [
                    EventRecordResult(
                        True,
                        event.event_id,
                        int(event.ingest_sequence or 0),
                    )
                    for event in item.events
                ]
                await self._process_claimed_event_batch(
                    item.events,
                    results,
                    archive_raw=False,
                )
                if self.database is not None:
                    await self.database.save_stream_protocol_checkpoints(
                        item.events,
                        stage="state",
                    )
                self.entry_gate.unblock("state_apply_error")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.entry_gate.block("state_apply_error")
                raise
            finally:
                self._state_queue.task_done()
                self._complete_stage("state", item)

    async def _archive_worker(self) -> None:
        while True:
            item = await self._archive_queue.get()
            if item is None:
                self._archive_queue.task_done()
                return
            try:
                while True:
                    try:
                        segments = await self.recorder.write_segments(
                            item.events
                        )
                        if self.database is not None:
                            await self.database.record_raw_archive_segments(
                                segments
                            )
                        self.entry_gate.unblock(
                            "raw_archive_error"
                        )
                        break
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        self.entry_gate.block(
                            "raw_archive_error"
                        )
                        logger.exception(
                            "raw archive segment failed; "
                            "retrying ordered batch"
                        )
                        await asyncio.sleep(1)
            finally:
                self._archive_queue.task_done()
                self._complete_stage("archive", item)

    async def process_transaction(
        self,
        protocol: Protocol,
        transaction: dict[str, Any],
        source: EventSource = EventSource.HELIUS_WSS,
    ) -> None:
        await self.process_transactions([(protocol, transaction, source)])

    async def process_transactions(
        self,
        transactions: list[tuple[Protocol, dict[str, Any], EventSource]],
    ) -> None:
        self._require_consistent_state()
        loop = asyncio.get_running_loop()
        decode_started = loop.time()
        self.metrics.chain_transaction_batch_size.observe(len(transactions))
        event_batches: list[list[EventEnvelope]] = []
        current_batch: list[EventEnvelope] = []
        for transaction_index, (protocol, transaction, source) in enumerate(
            transactions,
            start=1,
        ):
            decoder = self._pump if protocol == Protocol.PUMP else self._pumpswap
            try:
                decoded_events = decoder.decode_transaction(
                    transaction,
                    source=source,
                )
            except AnchorDecodeError:
                self.entry_gate.block_protocol(protocol)
                await self._record_unknown(protocol, transaction, source)
                raise
            if len(decoded_events) > MAX_EVENT_BATCH_SIZE:
                raise RuntimeError(
                    "one decoded transaction exceeds the durable event batch limit"
                )
            if (
                current_batch
                and len(current_batch) + len(decoded_events)
                > MAX_EVENT_BATCH_SIZE
            ):
                event_batches.append(current_batch)
                current_batch = []
            current_batch.extend(decoded_events)
            if transaction_index % EVENT_LOOP_YIELD_INTERVAL == 0:
                await asyncio.sleep(0)
        self.metrics.chain_batch_phase_seconds.labels(phase="decode").observe(
            loop.time() - decode_started
        )
        if current_batch:
            event_batches.append(current_batch)
        for events in event_batches:
            await self._process_decoded_event_batch(events)

    async def _process_decoded_event_batch(
        self,
        events: list[EventEnvelope],
    ) -> None:
        if self._background_workers_started:
            await self._enqueue_stage(
                "durable",
                self._durable_queue,
                events,
            )
            return
        await self._process_decoded_event_batch_inline(events)

    async def _process_decoded_event_batch_inline(
        self,
        events: list[EventEnvelope],
    ) -> None:
        loop = asyncio.get_running_loop()
        self.metrics.chain_decoded_event_batch_size.observe(len(events))
        durable_started = loop.time()
        try:
            durable_results = (
                await self.database.record_events(events, resume_owned=True)
                if self.database
                else [True] * len(events)
            )
        finally:
            self.metrics.chain_batch_phase_seconds.labels(
                phase="durable_claim"
            ).observe(loop.time() - durable_started)
        claimed_count = sum(1 for accepted in durable_results if accepted)
        if self.database is None or claimed_count < 2:
            for event, durable_accepted in zip(events, durable_results, strict=True):
                await self.process_event(event, durable_claim=bool(durable_accepted))
            return
        await self._process_claimed_event_batch(events, durable_results)

    async def _process_claimed_event_batch(
        self,
        events: list[EventEnvelope],
        durable_results: list[bool] | list[EventRecordResult],
        *,
        archive_raw: bool = True,
    ) -> None:
        database = self.database
        if database is None:
            raise RuntimeError("durable event batch requires a database")
        claimed_events = [
            event
            for event, durable_accepted in zip(events, durable_results, strict=True)
            if durable_accepted
        ]
        try:
            if self.record_raw and archive_raw:
                archive_started = asyncio.get_running_loop().time()
                try:
                    await self.recorder.record_many(claimed_events)
                finally:
                    self.metrics.chain_batch_phase_seconds.labels(
                        phase="raw_archive"
                    ).observe(asyncio.get_running_loop().time() - archive_started)
            state_started = asyncio.get_running_loop().time()
            try:
                async with database.event_state_batch_transaction():
                    for event_index, (event, durable_accepted) in enumerate(
                        zip(events, durable_results, strict=True),
                        start=1,
                    ):
                        processed = await self.process_event(
                            event,
                            durable_claim=bool(durable_accepted),
                            _batch_state_transaction=True,
                            _defer_failure_cleanup=True,
                            _raw_already_recorded=self.record_raw,
                        )
                        if durable_accepted and not processed:
                            raise RuntimeError(
                                "durably claimed batch event was rejected by local deduplication"
                            )
                        if event_index % EVENT_LOOP_YIELD_INTERVAL == 0:
                            await asyncio.sleep(0)
            finally:
                self.metrics.chain_batch_phase_seconds.labels(
                    phase="state_commit"
                ).observe(asyncio.get_running_loop().time() - state_started)
        except BaseException as error:
            self._poison_state()
            try:
                async with asyncio.timeout(5):
                    await database.mark_events_failed(
                        [event.event_id for event in claimed_events],
                        error,
                    )
            except BaseException:
                logger.exception(
                    "failed to persist bounded batch event cleanup; "
                    "durable claim tokens retained"
                )
            if self.fatal_handler is not None:
                self.fatal_handler(error)
            raise
        for event in claimed_events:
            database.release_event_claim(event.event_id)

    def _with_known_pool_mint(self, event: EventEnvelope) -> EventEnvelope:
        if event.mint is not None or not event.pool_address:
            return event
        pool = self.pools.pool(event.pool_address)
        if pool is None:
            return event
        return event.model_copy(update={"mint": pool.base_mint})

    def _event_state_filter_reason(self, event: EventEnvelope) -> str | None:
        if event.event_type not in {
            ChainEventType.SWAP_BUY,
            ChainEventType.SWAP_SELL,
        }:
            return None
        if not event.pool_address:
            return "unknown_pool"
        pool = self.pools.pool(event.pool_address)
        if pool is None:
            return "unknown_pool"
        candidate = self.candidates.get(
            _candidate_id(pool.base_mint, pool.pool_address, self.strategy_version)
        )
        if candidate is None:
            return "no_candidate"
        if event.block_time < candidate.detected_at:
            return "before_candidate"
        if candidate.state in {
            CandidateState.POSITION_OPEN,
            CandidateState.POSITION_PARTIAL,
            CandidateState.EXIT_PENDING,
            CandidateState.RETRYING_EXIT,
        }:
            return None
        if candidate.state in {
            CandidateState.CLOSED,
            CandidateState.REJECTED,
        }:
            return "terminal"
        maximum_age_seconds = (
            self.config.candidate.max_pool_age_seconds if self.config else 180
        )
        if event.block_time > candidate.detected_at + timedelta(
            seconds=maximum_age_seconds
        ):
            return "expired"
        return None

    async def process_event(
        self,
        event: EventEnvelope,
        *,
        recovering: bool = False,
        durable_claim: bool | None = None,
        _batch_state_transaction: bool = False,
        _defer_failure_cleanup: bool = False,
        _raw_already_recorded: bool = False,
    ) -> bool:
        self._require_consistent_state()
        original_mint = event.mint
        event = self._with_known_pool_mint(event)
        raw_context_changed = event.mint != original_mint
        self.metrics.chain_events_received.inc()
        lag_ms = max(
            Decimal("0"),
            Decimal(str((event.observed_at - event.block_time).total_seconds())) * Decimal("1000"),
        )
        self.metrics.chain_event_processing_lag_ms.observe(float(lag_ms))
        durable_accepted = (
            durable_claim
            if durable_claim is not None
            else (
                await self.database.record_event(event, reclaim=recovering)
                if self.database
                else True
            )
        )
        if not durable_accepted:
            self.metrics.chain_events_duplicate.inc()
            return False
        local_accepted = await self.deduplicator.accept(event.event_id)
        if not local_accepted:
            self.metrics.chain_events_duplicate.inc()
            return False
        filter_reason = self._event_state_filter_reason(event)
        apply_state = filter_reason is None
        requires_state_rebuild = False
        try:
            if self.record_raw and not recovering and not _raw_already_recorded:
                await self.recorder.record(event)
            if self.database is None:
                if apply_state:
                    await self._apply_event(
                        event,
                        persist=True,
                        observe=True,
                        allow_candidate=event.source not in NON_TRADABLE_EVENT_SOURCES,
                    )
                else:
                    self.metrics.chain_event_state_filter_decisions.labels(
                        reason=filter_reason
                    ).inc()
            else:
                transaction_entered = False
                try:
                    if _batch_state_transaction:
                        transaction_entered = True
                        await self._persist_claimed_event_state(
                            event,
                            raw_context_changed=raw_context_changed,
                            filter_reason=filter_reason,
                        )
                    else:
                        async with self.database.event_state_transaction():
                            transaction_entered = True
                            await self._persist_claimed_event_state(
                                event,
                                raw_context_changed=raw_context_changed,
                                filter_reason=filter_reason,
                            )
                except BaseException:
                    requires_state_rebuild = transaction_entered
                    raise
                if not _batch_state_transaction:
                    self.database.release_event_claim(event.event_id)
            return True
        except BaseException as error:
            if _defer_failure_cleanup:
                raise
            try:
                await self.deduplicator.forget(event.event_id)
                if self.database is not None:
                    try:
                        await self.database.mark_event_failed(event.event_id, error)
                    except Exception:
                        logger.exception(
                            "failed to persist event processing failure",
                            extra={"event_id": event.event_id},
                        )
            finally:
                if self.database is not None:
                    self.database.release_event_claim(event.event_id)
                if requires_state_rebuild:
                    self._poison_state()
                    if self.fatal_handler is not None:
                        self.fatal_handler(error)
            raise

    async def _persist_claimed_event_state(
        self,
        event: EventEnvelope,
        *,
        raw_context_changed: bool,
        filter_reason: str | None,
    ) -> None:
        if self.database is None:
            raise RuntimeError("durable event state requires a database")
        if raw_context_changed:
            await self.database.update_raw_event_context(event)
        if filter_reason is None:
            await self._apply_event(
                event,
                persist=True,
                observe=True,
                allow_candidate=event.source not in NON_TRADABLE_EVENT_SOURCES,
            )
        else:
            self.metrics.chain_event_state_filter_decisions.labels(
                reason=filter_reason
            ).inc()
        await self.database.mark_event_processed(
            event.event_id, processed_at=datetime.now(tz=timezone.utc)
        )

    async def rehydrate_event(self, event: EventEnvelope) -> bool:
        """Rebuild bounded in-memory state from an already processed durable event."""
        if self._event_state_filter_reason(event) is not None:
            return False
        await self._apply_event(
            event,
            persist=False,
            observe=False,
            allow_candidate=event.source not in NON_TRADABLE_EVENT_SOURCES,
        )
        return True

    def restore_candidates(self, candidates: list[Candidate]) -> None:
        for candidate in candidates:
            self.candidates[candidate.candidate_id] = candidate
        self.metrics.candidate_count.set(len(self.candidates))

    def restore_score_totals(self, scores: dict[str, Decimal]) -> None:
        self._persisted_score_totals.update(scores)

    async def _apply_event(
        self,
        event: EventEnvelope,
        *,
        persist: bool,
        observe: bool,
        allow_candidate: bool = True,
    ) -> None:
        pool_state = self.pools.apply(event)
        pool_record = self.pools.pool(event.pool_address) if event.pool_address else None
        effective_event = event
        if pool_record is not None and event.mint != pool_record.base_mint:
            effective_event = event.model_copy(update={"mint": pool_record.base_mint})
        supported_quote_pair = bool(
            pool_record is not None
            and pool_record.quote_mint in SUPPORTED_QUOTE_MINTS
            and pool_record.base_mint not in SUPPORTED_QUOTE_MINTS
        )
        if observe and self.event_observer is not None:
            await self.event_observer(effective_event)
        token = self.tokens.apply(effective_event)
        if persist and token is not None and self.database is not None:
            await self.database.upsert_token(token)
        if persist and pool_record is not None and self.database is not None:
            await self.database.upsert_pool(pool_record)
        if (
            effective_event.event_type == ChainEventType.POOL_CREATED
            and effective_event.pool_address
            and effective_event.mint
        ):
            self.features.register_pool(
                effective_event.pool_address, effective_event.block_time
            )
        if (
            allow_candidate
            and supported_quote_pair
            and effective_event.event_type == ChainEventType.POOL_CREATED
            and effective_event.pool_address
            and effective_event.mint
        ):
            candidate = Candidate(
                candidate_id=_candidate_id(
                    effective_event.mint,
                    effective_event.pool_address,
                    self.strategy_version,
                ),
                mint=effective_event.mint,
                pool_address=effective_event.pool_address,
                detected_at=effective_event.block_time,
                updated_at=effective_event.observed_at,
                strategy_version=self.strategy_version,
                config_hash=self.config_hash,
            )
            self.candidates.setdefault(candidate.candidate_id, candidate)
            self.metrics.candidate_count.set(len(self.candidates))
            if persist and self.database is not None:
                await self.database.upsert_candidate(candidate, self.strategy_version)
        if pool_state is not None:
            self._ingest_pool_features(effective_event, pool_state)
        if token is not None and effective_event.mint and effective_event.pool_address:
            current = self.candidates.get(
                _candidate_id(
                    effective_event.mint,
                    effective_event.pool_address,
                    self.strategy_version,
                )
            )
            if current is not None:
                self.candidates[current.candidate_id] = current.model_copy(
                    update={"updated_at": effective_event.observed_at}
                )

    async def evaluate_candidates(self, at: datetime | None = None) -> list[Candidate]:
        at = at or datetime.now(tz=timezone.utc)
        changed: list[Candidate] = []
        for candidate_id, candidate in list(self.candidates.items()):
            snapshot = self.features.snapshot(candidate.pool_address, at)
            if self.database is not None and self.pools.pool(candidate.pool_address) is not None:
                await self.database.record_snapshot(snapshot)
            security_context: SecurityContext | None = None
            security_result: SecurityResult | None = None
            score: ScoreBreakdown | None = None
            if self.security_provider is not None and candidate.state in {
                CandidateState.SECURITY_CHECK,
                CandidateState.ELIGIBLE,
                CandidateState.WAITING_PULLBACK,
                CandidateState.ARMED,
                CandidateState.ENTRY_PENDING,
            }:
                security_context = await self.security_provider(candidate, snapshot)
                snapshot = self.features.snapshot(candidate.pool_address, at)
                security_result = self.security.evaluate(security_context, now=at)
                self._security_results[candidate_id] = (security_context, security_result)
                if self.database is not None:
                    await self.database.record_security(security_context, security_result)
                score = self.scoring.score(
                    ScoreContext(
                        features=snapshot,
                        round_trip_loss_pct=security_context.execution.round_trip_loss_pct,
                        buy_price_impact_pct=security_context.execution.buy_price_impact_pct,
                        sell_price_impact_pct=security_context.execution.sell_price_impact_pct,
                        sell_route_reliability=Decimal("1") if security_context.execution.sell_route_available else Decimal("0"),
                        developer_history=DeveloperHistory(
                            known=security_context.developer_history_known,
                            previous_rugs=security_context.previous_rugs,
                            previous_dev_dumps_5m=security_context.previous_dev_dumps_5m,
                            tokens_created_7d=security_context.developer_tokens_created_7d,
                            successful_tokens=security_context.developer_successful_tokens,
                        ),
                        vwap_reclaimed=snapshot.current_price_usd > snapshot.rolling_vwap_30s,
                    )
                )
                self._scores[candidate_id] = score
                self._persisted_score_totals[candidate_id] = score.total_score
                if self.database is not None:
                    await self.database.record_signal(candidate, snapshot, score)
            before = candidate.state
            if candidate.state == CandidateState.ENTRY_PENDING:
                if not self.entry_gate.enabled:
                    candidate = self.state_machine.transition(
                        candidate,
                        CandidateState.REJECTED,
                        at,
                        reject_reason=RejectReason.API_UNAVAILABLE,
                    )
                elif (
                    not score
                    or not security_context
                    or security_result is None
                    or not _entry_rules(
                        snapshot,
                        score,
                        security_context,
                        security_result,
                        self.config,
                    )
                ):
                    candidate = self.state_machine.transition(
                        candidate,
                        CandidateState.REJECTED,
                        at,
                        reject_reason=RejectReason.SCORE_TOO_LOW,
                    )
                elif self.entry_handler is not None:
                    reject_reason = await self.entry_handler(candidate, snapshot, score, security_context)
                    if reject_reason is None:
                        candidate = self.state_machine.transition(
                            candidate, CandidateState.POSITION_OPEN, at
                        )
                    else:
                        candidate = self.state_machine.transition(
                            candidate,
                            CandidateState.REJECTED,
                            at,
                            reject_reason=reject_reason,
                        )
                self.candidates[candidate_id] = candidate
                if self.database is not None:
                    await self.database.upsert_candidate(candidate, self.strategy_version)
                if candidate.state != before:
                    changed.append(candidate)
                    if candidate.state == CandidateState.REJECTED:
                        reason = candidate.reject_reason or RejectReason.API_UNAVAILABLE
                        self.metrics.candidate_rejections.labels(reason=reason.value).inc()
                continue
            candidate = self.state_machine.evaluate(
                candidate,
                snapshot,
                security=security_result,
                score=score,
                sell_route_available=(
                    security_context.execution.sell_route_available if security_context else False
                ),
                dev_sold=security_context.dev_sold if security_context else False,
            )
            self.candidates[candidate_id] = candidate
            if self.database is not None:
                await self.database.upsert_candidate(candidate, self.strategy_version)
            if candidate.state != before:
                changed.append(candidate)
                if candidate.state == CandidateState.REJECTED:
                    reason = candidate.reject_reason or RejectReason.API_UNAVAILABLE
                    self.metrics.candidate_rejections.labels(reason=reason.value).inc()
                if candidate.state == CandidateState.ENTRY_PENDING:
                    self.metrics.signals.inc()
        return changed

    def list_candidates(self) -> list[Candidate]:
        return list(self.candidates.values())

    async def transition_candidate(
        self, candidate_id: str, target: CandidateState, at: datetime
    ) -> Candidate | None:
        candidate = self.candidates.get(candidate_id)
        if candidate is None:
            return None
        updated = self.state_machine.transition(candidate, target, at)
        self.candidates[candidate_id] = updated
        if self.database is not None:
            await self.database.upsert_candidate(updated, self.strategy_version)
        return updated

    def list_rejections(self) -> list[Candidate]:
        return [candidate for candidate in self.candidates.values() if candidate.state == CandidateState.REJECTED]

    def update_pool_supply(
        self,
        pool_address: str,
        total_supply_raw: Decimal,
        observed_at: datetime,
    ) -> PoolState | None:
        state = self.pools.apply_base_supply(
            pool_address,
            total_supply_raw=total_supply_raw,
        )
        if state is not None:
            self.features.ingest_liquidity(
                LiquidityObservation(
                    event_id=(
                        f"{pool_address}:supply:{total_supply_raw.normalize()}"
                    ),
                    pool_address=pool_address,
                    event_time=observed_at,
                    quote_liquidity_usd=max(state.quote_reserve_usd, Decimal("0")),
                    market_cap_usd=state.market_cap_estimate_usd,
                )
            )
        return state

    def _ingest_pool_features(self, event: EventEnvelope, state: PoolState) -> None:
        self.features.ingest_liquidity(
            LiquidityObservation(
                event_id=f"{event.event_id}:liquidity",
                pool_address=state.pool_address,
                event_time=event.block_time,
                quote_liquidity_usd=max(state.quote_reserve_usd, Decimal("0")),
                market_cap_usd=state.market_cap_estimate_usd,
            )
        )
        if event.event_type not in {ChainEventType.SWAP_BUY, ChainEventType.SWAP_SELL}:
            return
        pool = self.pools.pool(state.pool_address)
        if pool is None or state.marginal_price_usd <= 0:
            return
        if pool.source_orientation_reversed:
            if event.event_type == ChainEventType.SWAP_BUY:
                quote_amount = event.payload.get("base_amount_out") or 0
                trade_side = TradeSide.SELL
            else:
                quote_amount = event.payload.get("base_amount_in") or 0
                trade_side = TradeSide.BUY
        else:
            quote_amount = (
                event.payload.get("quote_amount_in")
                or event.payload.get("quote_amount_out")
                or 0
            )
            trade_side = (
                TradeSide.BUY
                if event.event_type == ChainEventType.SWAP_BUY
                else TradeSide.SELL
            )
        quote_price = self.pools.quote_price(pool.quote_mint)
        quote_usd = quote_price.price_usd if quote_price else Decimal("0")
        volume_usd = Decimal(str(quote_amount)) / (Decimal(10) ** pool.quote_decimals) * quote_usd
        wallet = str(event.payload.get("user") or "unknown")
        token = self.tokens.get(state.base_mint)
        self.features.ingest_trade(
            TradeObservation(
                event_id=f"{event.event_id}:trade",
                pool_address=state.pool_address,
                event_time=event.block_time,
                side=trade_side,
                wallet=wallet,
                volume_usd=max(volume_usd, Decimal("0")),
                price_usd=state.marginal_price_usd,
                external=bool(wallet != "unknown" and (token is None or wallet != token.creator_address)),
                same_funder_cluster=bool(event.payload.get("same_funder_cluster", False)),
            )
        )

    async def _record_unknown(
        self,
        protocol: Protocol,
        transaction: dict[str, Any],
        source: EventSource,
    ) -> None:
        signature = str(transaction.get("signature") or "missing-signature")
        signatures = transaction.get("transaction", {}).get("signatures") or []
        if signatures:
            signature = str(signatures[0])
        block_time_raw = transaction.get("blockTime")
        block_time = (
            datetime.fromtimestamp(int(block_time_raw), tz=timezone.utc)
            if block_time_raw is not None
            else datetime.now(tz=timezone.utc)
        )
        event = EventEnvelope(
            source=source,
            protocol=protocol,
            event_type=ChainEventType.UNKNOWN,
            slot=int(transaction.get("slot", 0)),
            signature=signature,
            instruction_index=0,
            inner_instruction_index=-1,
            block_time=block_time,
            observed_at=datetime.now(tz=timezone.utc),
            payload={"transaction": transaction, "reason": "UNKNOWN_PROTOCOL_LAYOUT"},
        )
        await self.recorder.record(event)


def _candidate_id(mint: str, pool_address: str, strategy_version: str) -> str:
    return hashlib.sha256(f"{mint}:{pool_address}:{strategy_version}".encode("utf-8")).hexdigest()[:24]


def _entry_rules(
    features: FeatureSnapshot,
    score: ScoreBreakdown,
    security: SecurityContext,
    security_result: SecurityResult,
    config: AppConfig | None = None,
) -> bool:
    holders = security.holders
    if holders is None:
        return False
    return all(
        (
            Decimal(config.candidate.min_pool_age_seconds if config else 45)
            <= features.pool_age_seconds
            <= Decimal(config.candidate.max_pool_age_seconds if config else 180),
            not security_result.hard_reject,
            score.total_score >= (config.candidate.score_entry if config else Decimal("80")),
            security.quote_liquidity_usd >= (
                config.liquidity.min_quote_liquidity_usd if config else Decimal("40000")
            ),
            features.market_cap_to_quote_liquidity <= (
                config.liquidity.max_market_cap_to_quote_liquidity if config else Decimal("25")
            ),
            features.unique_buyers_60s >= (config.flow.min_unique_buyers_60s if config else 25),
            features.buyer_acceleration >= (
                config.flow.min_buyer_acceleration if config else Decimal("1.3")
            ),
            features.unique_buyer_ratio >= (
                config.flow.min_unique_buyer_ratio if config else Decimal("0.30")
            ),
            features.transactions_per_trader <= (
                config.flow.max_transactions_per_trader if config else Decimal("4")
            ),
            features.top_5_buyer_volume_share <= (
                config.flow.max_top_5_buy_volume_share if config else Decimal("0.35")
            ),
            features.same_funder_buy_share <= (
                config.flow.max_same_funder_buy_share if config else Decimal("0.20")
            ),
            (config.flow.min_buy_sell_volume_ratio if config else Decimal("1.5"))
            <= features.buy_sell_volume_ratio
            <= (config.flow.max_buy_sell_volume_ratio if config else Decimal("5")),
            features.quote_liquidity_change_30s >= -(
                config.liquidity.max_liquidity_drop_entry_30s_pct
                if config
                else Decimal("0.03")
            ),
            (config.candidate.min_pullback_pct if config else Decimal("0.10"))
            <= features.drawdown_from_local_high
            <= (config.candidate.max_pullback_pct if config else Decimal("0.25")),
            features.return_since_pool_creation <= (
                config.candidate.max_return_since_pool_creation_pct
                if config
                else Decimal("2.50")
            ),
            not security.dev_sold,
            security.execution.buy_price_impact_pct
            <= (
                config.execution.max_buy_price_impact_pct
                if config
                else Decimal("0.025")
            ),
            security.execution.sell_price_impact_pct
            <= (
                config.execution.max_sell_price_impact_pct
                if config
                else Decimal("0.035")
            ),
            security.execution.round_trip_loss_pct
            <= (
                config.execution.max_round_trip_loss_pct
                if config
                else Decimal("0.08")
            ),
            holders.largest_holder_pct
            <= (
                config.holders.max_largest_holder_pct
                if config
                else Decimal("0.07")
            ),
            holders.top_5_holders_pct
            <= (config.holders.max_top_5_pct if config else Decimal("0.22")),
            holders.top_10_holders_pct
            <= (config.holders.max_top_10_pct if config else Decimal("0.30")),
            holders.dev_holding_pct
            <= (config.holders.max_dev_holding_pct if config else Decimal("0.02")),
            holders.dev_cluster_holding_pct
            <= (config.holders.max_dev_cluster_pct if config else Decimal("0.05")),
            holders.related_cluster_holding_pct
            <= (
                config.holders.max_related_cluster_pct
                if config
                else Decimal("0.15")
            ),
            security.external_successful_sellers
            >= (config.execution.min_external_sellers if config else 5),
        )
    )
