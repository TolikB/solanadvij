"""Helius stream gateway with fail-closed reconnect and slot-gap recovery."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import websockets

from .events import EventSource, Protocol
from .metrics import BotMetrics
from .protocols.pump import PUMP_PROGRAM_ID
from .protocols.pumpswap import PUMPSWAP_PROGRAM_ID
from .solana_rpc import SolanaRpcClient, SolanaRpcError

logger = logging.getLogger(__name__)

TransactionItem = tuple[Protocol, dict[str, Any], EventSource]
TransactionHandler = Callable[[Protocol, dict[str, Any], EventSource], Awaitable[None]]
TransactionBatchHandler = Callable[[list[TransactionItem]], Awaitable[None]]
FatalHandler = Callable[[BaseException], None]
GapHandler = Callable[[str], Awaitable[None]]
GapResolvedHandler = Callable[[], Awaitable[None]]


@dataclass(slots=True)
class _TransactionNotification:
    transaction: dict[str, Any]
    received_at: datetime
    generation: int


@dataclass(slots=True)
class _LogNotification:
    signature: str
    slot: int
    logs: list[str]
    received_at: datetime
    generation: int


NotificationDispatch = _TransactionNotification | _LogNotification


class EntryGate:
    def __init__(self, metrics: BotMetrics | None = None) -> None:
        self._reasons: set[str] = {"startup"}
        self._protocol_reasons: set[Protocol] = set()
        self._metrics = metrics

    @property
    def enabled(self) -> bool:
        return not self._reasons and not self._protocol_reasons

    @property
    def reasons(self) -> list[str]:
        return sorted(self._reasons | {f"protocol:{item.value}" for item in self._protocol_reasons})

    def block(self, reason: str) -> None:
        self._reasons.add(reason)
        self._sync_metric()

    def unblock(self, reason: str) -> None:
        self._reasons.discard(reason)
        self._sync_metric()

    def block_protocol(self, protocol: Protocol) -> None:
        self._protocol_reasons.add(protocol)
        self._sync_metric()

    def unblock_protocol(self, protocol: Protocol) -> None:
        self._protocol_reasons.discard(protocol)
        self._sync_metric()

    def _sync_metric(self) -> None:
        if self._metrics is not None:
            self._metrics.system_entry_enabled.set(1 if self.enabled else 0)


class _SubscriptionHandshakeError(RuntimeError):
    def __init__(self, message: str, buffered_messages: list[dict[str, Any]]) -> None:
        super().__init__(message)
        self.buffered_messages = tuple(buffered_messages)


class _AcknowledgementCollectionError(_SubscriptionHandshakeError):
    pass


class _UseLogsFallback(_SubscriptionHandshakeError):
    pass


class _GapRecoveryTimeout(RuntimeError):
    def __init__(self, message: str, buffered_messages: list[dict[str, Any]]) -> None:
        super().__init__(message)
        self.buffered_messages = tuple(buffered_messages)


class HeliusStreamGateway:
    BACKOFF_SECONDS = (1, 2, 4, 8, 15)
    WEBSOCKET_PING_INTERVAL_SECONDS = 30.0
    WEBSOCKET_PING_TIMEOUT_SECONDS = 60.0
    WEBSOCKET_CLOSE_TIMEOUT_SECONDS = 5.0
    SUBSCRIPTION_ACK_TIMEOUT_SECONDS = 10.0
    NOTIFICATION_QUEUE_SIZE = 16_384
    SUBSCRIPTION_MESSAGE_BUFFER_LIMIT = NOTIFICATION_QUEUE_SIZE
    MAX_GAP_RECOVERY_AGE = timedelta(seconds=60)
    GAP_RECOVERY_TIMEOUT_SECONDS = 15.0
    LIVE_BASELINE_WARMUP = timedelta(seconds=60)
    LOG_FETCH_CONCURRENCY = 20
    SLOT_BLOCK_TIME_CACHE_SIZE = 512
    BLOCK_TIME_RETRY_DELAYS = (0.25, 0.5, 1.0)
    PROCESSING_BATCH_SIZE = 1024
    PROCESSING_BATCH_WINDOW_SECONDS = 0.02
    SHUTDOWN_DRAIN_TIMEOUT_SECONDS = 120.0
    PROCESSING_RETRY_LIMIT = 3
    PROCESSING_RETRY_DELAYS = (1, 2)

    def __init__(
        self,
        *,
        websocket_url: str,
        rpc: SolanaRpcClient,
        handler: TransactionHandler,
        entry_gate: EntryGate,
        metrics: BotMetrics,
        batch_handler: TransactionBatchHandler | None = None,
        fatal_handler: FatalHandler | None = None,
        gap_handler: GapHandler | None = None,
        gap_resolved_handler: GapResolvedHandler | None = None,
        queue_size: int = 2000,
        max_processing_lag_seconds: float = 3.0,
        log_fetch_concurrency: int = LOG_FETCH_CONCURRENCY,
        notification_queue_size: int = NOTIFICATION_QUEUE_SIZE,
    ) -> None:
        if max_processing_lag_seconds <= 0:
            raise ValueError("max_processing_lag_seconds must be greater than zero")
        if log_fetch_concurrency <= 0:
            raise ValueError("log_fetch_concurrency must be greater than zero")
        if notification_queue_size <= 0:
            raise ValueError("notification_queue_size must be greater than zero")
        self.websocket_url = websocket_url
        self.rpc = rpc
        self.handler = handler
        self.batch_handler = batch_handler
        self.fatal_handler = fatal_handler
        self.gap_handler = gap_handler
        self.gap_resolved_handler = gap_resolved_handler
        self.entry_gate = entry_gate
        self.metrics = metrics
        self.last_slot = 0
        self.last_signature: str | None = None
        self._last_signatures: dict[Protocol, str] = {}
        self.last_observed_at: datetime | None = None
        self.last_chain_block_time: datetime | None = None
        self.last_processed_block_time: datetime | None = None
        self.last_processing_lag_seconds: float | None = None
        self.max_processing_lag_seconds = max_processing_lag_seconds
        self._baseline_started_at: datetime | None = None
        self._queue: asyncio.Queue[TransactionItem | None] = asyncio.Queue(maxsize=queue_size)
        self._notification_queue: asyncio.Queue[NotificationDispatch] = asyncio.Queue(
            maxsize=notification_queue_size
        )
        self._notification_dispatch_task: asyncio.Task[None] | None = None
        self._run_task: asyncio.Task[None] | None = None
        self._worker_task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._stream_generation = 0
        self._log_fetch_sequence = 0
        self._log_fetch_semaphore = asyncio.Semaphore(log_fetch_concurrency)
        self._gap_recovery_semaphore = asyncio.Semaphore(log_fetch_concurrency)
        self._log_fetch_tasks: set[asyncio.Task[None]] = set()
        self._log_fetch_tail: asyncio.Task[None] | None = None
        self._fetch_error_sequences: set[int] = set()
        self._reconnect_requested = asyncio.Event()
        self._dispatch_recovery_pending = False
        self._slot_block_times: dict[int, int] = {}
        self._slot_block_time_tasks: dict[int, asyncio.Task[int | None]] = {}

    async def start(self) -> None:
        if self._run_task is not None:
            return
        self._stopping.clear()
        self._worker_task = asyncio.create_task(self._worker(), name="chain-event-worker")
        self._run_task = asyncio.create_task(self._run(), name="helius-stream")

    def restore_checkpoint(
        self, slot: int, signature: str | None, observed_at: datetime | None
    ) -> None:
        self.last_slot = max(0, slot)
        self.last_signature = signature
        self.last_observed_at = observed_at

    def restore_protocol_checkpoint(self, protocol: Protocol, signature: str) -> None:
        self._last_signatures[protocol] = signature

    async def stop(self) -> None:
        started = asyncio.get_running_loop().time()
        self._stopping.set()
        if self._run_task is not None:
            self._run_task.cancel()
            try:
                await self._run_task
            except asyncio.CancelledError:
                pass
            self._run_task = None
        try:
            async with asyncio.timeout(
                self.SHUTDOWN_DRAIN_TIMEOUT_SECONDS
            ):
                await self._notification_queue.join()
                if self._log_fetch_tasks:
                    await asyncio.gather(
                        *tuple(self._log_fetch_tasks)
                    )
        except TimeoutError as exc:
            self.entry_gate.block("stream_recovery_gap")
            self.metrics.stream_recovery_gap_active.set(1)
            raise RuntimeError(
                "Solana ingress queues did not drain within "
                f"{self.SHUTDOWN_DRAIN_TIMEOUT_SECONDS:g} seconds"
            ) from exc
        finally:
            self.metrics.shutdown_drain_seconds.labels(
                stage="stream_ingress"
            ).observe(
                asyncio.get_running_loop().time() - started
            )
        await self._cancel_log_fetch_tasks()
        if self._worker_task is not None:
            worker_task = self._worker_task
            drain_task: asyncio.Task[None] | None = None
            if not worker_task.done():
                drain_task = asyncio.create_task(
                    self._queue.join(), name="chain-event-queue-drain"
                )
                completed, _ = await asyncio.wait(
                    {worker_task, drain_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if worker_task in completed:
                    drain_task.cancel()
                    try:
                        await drain_task
                    except asyncio.CancelledError:
                        pass
                else:
                    worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("chain event worker had already failed during shutdown")
            self._worker_task = None

    async def _run(self) -> None:
        attempt = 0
        logs_only = False
        pending_handshake_messages: list[dict[str, Any]] = []
        while not self._stopping.is_set():
            self.entry_gate.block("stream_disconnected")
            try:
                async with websockets.connect(
                    self.websocket_url,
                    ping_interval=self.WEBSOCKET_PING_INTERVAL_SECONDS,
                    ping_timeout=self.WEBSOCKET_PING_TIMEOUT_SECONDS,
                    close_timeout=self.WEBSOCKET_CLOSE_TIMEOUT_SECONDS,
                    max_queue=1024,
                ) as websocket:
                    try:
                        self._reconnect_requested.clear()
                        self._begin_live_baseline()
                        buffered = await self._subscribe(websocket, logs_only=logs_only)
                        if (
                            len(pending_handshake_messages) + len(buffered)
                            > self.SUBSCRIPTION_MESSAGE_BUFFER_LIMIT
                        ):
                            logger.error("subscription handshake message buffer overflow")
                            return
                        pending_handshake_messages.extend(buffered)
                        recovered: list[tuple[Protocol, dict[str, Any], EventSource]] = []
                        if self.last_slot:
                            try:
                                recovered, pending_handshake_messages = (
                                    await self._recover_gap_with_live_buffer(
                                        websocket,
                                        pending_handshake_messages,
                                    )
                                )
                            except _GapRecoveryTimeout as exc:
                                logger.warning(str(exc))
                                pending_handshake_messages.clear()
                                self.entry_gate.block(
                                    "stream_recovery_gap"
                                )
                                self.metrics.stream_recovery_gap_active.set(1)
                                if self.gap_handler is not None:
                                    await self.gap_handler(
                                        "recovery_timeout"
                                    )
                                self.metrics.websocket_reconnects.inc()
                                continue
                        if recovered:
                            await self._commit_recovered_events(recovered)
                            self.metrics.websocket_gap_recoveries.inc()
                        if self.last_slot:
                            self._dispatch_recovery_pending = False
                            self.entry_gate.unblock(
                                "stream_recovery_gap"
                            )
                            self.metrics.stream_recovery_gap_active.set(0)
                            if self.gap_resolved_handler is not None:
                                await self.gap_resolved_handler()
                        dispatched_messages = 0
                        try:
                            for message in pending_handshake_messages:
                                if self._reconnect_requested.is_set():
                                    raise RuntimeError(
                                        "ordered Solana transaction dispatch "
                                        "requested reconnect"
                                    )
                                await self.handle_message(message)
                                dispatched_messages += 1
                        finally:
                            if dispatched_messages:
                                del pending_handshake_messages[:dispatched_messages]
                        self.entry_gate.unblock("startup")
                        self.entry_gate.unblock("stream_disconnected")
                        self.refresh_freshness()
                        attempt = 0
                        while True:
                            try:
                                raw_message = await self._receive_or_reconnect(websocket)
                            except StopAsyncIteration:
                                break
                            await self.handle_message(json.loads(raw_message))
                    finally:
                        self.entry_gate.block("stream_disconnected")
                        if not self._stopping.is_set():
                            await self._cancel_log_fetch_tasks()
            except _UseLogsFallback as exc:
                if not self._extend_handshake_buffer(
                    pending_handshake_messages, exc.buffered_messages
                ):
                    return
                logs_only = True
                self.metrics.websocket_reconnects.inc()
                continue
            except asyncio.CancelledError:
                raise
            except _SubscriptionHandshakeError as exc:
                if not self._extend_handshake_buffer(
                    pending_handshake_messages, exc.buffered_messages
                ):
                    return
                logger.exception("Solana stream subscription handshake failed")
                self.metrics.websocket_reconnects.inc()
                self.entry_gate.block("stream_disconnected")
                delay = self.BACKOFF_SECONDS[min(attempt, len(self.BACKOFF_SECONDS) - 1)]
                attempt += 1
                await asyncio.sleep(delay)
            except Exception:
                logger.exception("Helius stream connection failed")
                self.metrics.websocket_reconnects.inc()
                self.entry_gate.block("stream_disconnected")
                delay = self.BACKOFF_SECONDS[min(attempt, len(self.BACKOFF_SECONDS) - 1)]
                attempt += 1
                await asyncio.sleep(delay)

    def _extend_handshake_buffer(
        self,
        pending: list[dict[str, Any]],
        incoming: tuple[dict[str, Any], ...],
    ) -> bool:
        if len(pending) + len(incoming) > self.SUBSCRIPTION_MESSAGE_BUFFER_LIMIT:
            logger.error("subscription handshake message buffer overflow")
            self.entry_gate.block("stream_disconnected")
            return False
        pending.extend(incoming)
        return True

    async def _subscribe(
        self, websocket: Any, *, logs_only: bool = False
    ) -> list[dict[str, Any]]:
        if not logs_only:
            request_ids: set[int] = set()
            for request_id, program_id in enumerate(
                (PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID), start=1
            ):
                request_ids.add(request_id)
                await websocket.send(
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "id": request_id,
                            "method": "transactionSubscribe",
                            "params": [
                                {
                                    "accountInclude": [program_id],
                                    "failed": False,
                                    "vote": False,
                                },
                                {
                                    "commitment": "confirmed",
                                    "encoding": "jsonParsed",
                                    "transactionDetails": "full",
                                    "maxSupportedTransactionVersion": 0,
                                },
                            ],
                        }
                    )
                )
            try:
                acknowledgements, buffered = await self._collect_acknowledgements(
                    websocket, request_ids
                )
            except _AcknowledgementCollectionError as exc:
                raise _UseLogsFallback(
                    str(exc), list(exc.buffered_messages)
                ) from exc
            if not all(
                self._valid_subscription_ack(acknowledgements[request_id])
                for request_id in request_ids
            ):
                raise _UseLogsFallback(
                    "transaction subscriptions are unavailable", buffered
                )
            return buffered

        fallback_ids: set[int] = set()
        for offset, program_id in enumerate((PUMP_PROGRAM_ID, PUMPSWAP_PROGRAM_ID), start=101):
            fallback_ids.add(offset)
            await websocket.send(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": offset,
                        "method": "logsSubscribe",
                        "params": [
                            {"mentions": [program_id]},
                            {"commitment": "confirmed"},
                        ],
                    }
                )
            )
        fallback_acks: dict[int, dict[str, Any]] = {}
        try:
            fallback_acks, buffered = await self._collect_acknowledgements(
                websocket, fallback_ids
            )
        except _AcknowledgementCollectionError as exc:
            raise _SubscriptionHandshakeError(
                "Solana logs subscription acknowledgement failed",
                list(exc.buffered_messages),
            ) from exc
        if not all(
            self._valid_subscription_ack(fallback_acks[request_id])
            for request_id in fallback_ids
        ):
            raise _SubscriptionHandshakeError(
                "both Helius transaction and Solana logs subscriptions failed", buffered
            )
        return buffered

    async def _collect_acknowledgements(
        self,
        websocket: Any,
        request_ids: set[int],
    ) -> tuple[dict[int, dict[str, Any]], list[dict[str, Any]]]:
        acknowledgements: dict[int, dict[str, Any]] = {}
        buffered: list[dict[str, Any]] = []
        try:
            async with asyncio.timeout(self.SUBSCRIPTION_ACK_TIMEOUT_SECONDS):
                while request_ids - acknowledgements.keys():
                    message = json.loads(await websocket.recv())
                    if not isinstance(message, dict):
                        raise _AcknowledgementCollectionError(
                            "subscription endpoint returned a non-object message", buffered
                        )
                    if message.get("jsonrpc") != "2.0":
                        raise _AcknowledgementCollectionError(
                            "subscription endpoint returned an invalid JSON-RPC version",
                            buffered,
                        )
                    message_id = message.get("id")
                    if "id" in message:
                        if type(message_id) is not int or message_id not in request_ids:
                            raise _AcknowledgementCollectionError(
                                "subscription endpoint returned an invalid acknowledgement id",
                                buffered,
                            )
                        if message_id in acknowledgements:
                            raise _AcknowledgementCollectionError(
                                "subscription endpoint returned a duplicate acknowledgement",
                                buffered,
                            )
                        acknowledgements[message_id] = message
                        continue
                    if len(buffered) >= self.SUBSCRIPTION_MESSAGE_BUFFER_LIMIT:
                        raise _AcknowledgementCollectionError(
                            "subscription handshake message buffer overflow",
                            [*buffered, message],
                        )
                    buffered.append(message)
        except TimeoutError as exc:
            raise _AcknowledgementCollectionError(
                "subscription acknowledgement timed out", buffered
            ) from exc
        return acknowledgements, buffered

    @staticmethod
    def _valid_subscription_ack(message: dict[str, Any]) -> bool:
        if message.get("jsonrpc") != "2.0" or "error" in message:
            return False
        result = message.get("result")
        return type(result) is int and result >= 0

    async def _receive_or_reconnect(self, websocket: Any) -> Any:
        receive_task = asyncio.create_task(
            websocket.recv(), name="solana-websocket-receive"
        )
        reconnect_task = asyncio.create_task(
            self._reconnect_requested.wait(), name="solana-reconnect-request"
        )
        waiters: set[asyncio.Task[Any]] = {receive_task, reconnect_task}
        try:
            await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in waiters:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)
        if self._reconnect_requested.is_set():
            raise RuntimeError("ordered Solana transaction dispatch requested reconnect")
        return receive_task.result()

    async def handle_message(self, message: dict[str, Any]) -> None:
        if message.get("error"):
            raise RuntimeError("Helius subscription returned an error")
        method = str(message.get("method") or "")
        if not method.endswith("Notification"):
            return
        received_at = datetime.now(tz=timezone.utc)
        if self.last_observed_at is None or received_at > self.last_observed_at:
            self.last_observed_at = received_at
        result = ((message.get("params") or {}).get("result") or {})
        transaction = result.get("transaction") if isinstance(result, dict) else None
        context = result.get("context") if isinstance(result, dict) else None
        if isinstance(transaction, dict):
            if "slot" not in transaction and isinstance(context, dict):
                transaction["slot"] = context.get("slot", 0)
            self._enqueue_notification_dispatch(
                _TransactionNotification(
                    transaction=transaction,
                    received_at=received_at,
                    generation=self._stream_generation,
                )
            )
            return
        if method != "logsNotification":
            return
        value = result.get("value") if isinstance(result, dict) else None
        if not isinstance(value, dict):
            raise RuntimeError("Solana logs notification is missing a valid value")
        signature = value.get("signature")
        if (
            not isinstance(signature, str)
            or not signature
            or signature != signature.strip()
        ):
            raise RuntimeError("Solana logs notification is missing a valid signature")
        if "err" not in value:
            raise RuntimeError("Solana logs notification is missing the required err field")
        if value["err"] is not None:
            return
        raw_logs = value.get("logs")
        if not isinstance(raw_logs, list) or not all(
            isinstance(line, str) for line in raw_logs
        ):
            raise RuntimeError("Solana logs notification is missing a valid logs array")
        slot = context.get("slot") if isinstance(context, dict) else None
        if type(slot) is not int or slot <= 0:
            raise RuntimeError("Solana logs notification is missing a valid slot")
        self._enqueue_notification_dispatch(
            _LogNotification(
                signature=signature,
                slot=slot,
                logs=raw_logs,
                received_at=received_at,
                generation=self._stream_generation,
            )
        )

    def _enqueue_notification_dispatch(
        self,
        item: NotificationDispatch,
    ) -> None:
        if self._reconnect_requested.is_set():
            raise RuntimeError(
                "ordered Solana transaction dispatch requested reconnect"
            )
        dispatch_task = self._notification_dispatch_task
        if dispatch_task is None or dispatch_task.done():
            if dispatch_task is not None:
                try:
                    dispatch_task.exception()
                except asyncio.CancelledError:
                    pass
            self._notification_dispatch_task = asyncio.create_task(
                self._dispatch_notifications(),
                name="solana-notification-dispatch",
            )
        try:
            self._notification_queue.put_nowait(item)
        except asyncio.QueueFull as exc:
            error = RuntimeError(
                "Solana notification ingress queue overflow"
            )
            self._dispatch_recovery_pending = True
            self.metrics.ingestion_events_dropped.labels(
                stage="notification"
            ).inc()
            self.entry_gate.block("stream_fetch_error")
            self._reconnect_requested.set()
            if self.fatal_handler is not None:
                self.fatal_handler(error)
            raise error from exc
        self._sync_queue_depth()

    async def _dispatch_notifications(self) -> None:
        while True:
            item = await self._notification_queue.get()
            try:
                if isinstance(item, _TransactionNotification):
                    await self._queue_transaction(
                        item.transaction,
                        EventSource.HELIUS_WSS,
                        received_at=item.received_at,
                        generation=item.generation,
                    )
                else:
                    await self._spawn_ordered_log_dispatch(
                        signature=item.signature,
                        slot=item.slot,
                        logs=item.logs,
                        received_at=item.received_at,
                        generation=item.generation,
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                self._dispatch_recovery_pending = True
                self.entry_gate.block("stream_fetch_error")
                self._reconnect_requested.set()
                logger.exception(
                    "Solana notification dispatch failed; reconnect requested"
                )
                raise
            finally:
                self._notification_queue.task_done()
                self._sync_queue_depth()

    async def _spawn_ordered_log_dispatch(
        self,
        *,
        signature: str,
        slot: int,
        logs: list[str],
        received_at: datetime,
        generation: int,
    ) -> None:
        await self._acquire_log_fetch_permit()
        sequence = self._log_fetch_sequence
        self._log_fetch_sequence += 1
        previous = self._log_fetch_tail
        task = asyncio.create_task(
            self._fetch_block_time_and_dispatch_logs(
                sequence=sequence,
                signature=signature,
                slot=slot,
                logs=logs,
                received_at=received_at,
                generation=generation,
                previous=previous,
            ),
            name=f"solana-log-dispatch-{sequence}",
        )
        self._log_fetch_tail = task
        self._log_fetch_tasks.add(task)
        task.add_done_callback(self._forget_log_fetch_task)
        self._sync_queue_depth()

    async def _fetch_block_time_and_dispatch_logs(
        self,
        *,
        sequence: int,
        signature: str,
        slot: int,
        logs: list[str],
        received_at: datetime,
        generation: int,
        previous: asyncio.Task[None] | None,
    ) -> None:
        dispatched = False
        try:
            block_time = await self._get_slot_block_time(slot)
            if previous is not None:
                await previous
            if any(failed < sequence for failed in self._fetch_error_sequences):
                raise RuntimeError("an earlier ordered transaction dispatch failed")
            await self._queue_transaction(
                {
                    "slot": slot,
                    "blockTime": block_time,
                    "signature": signature,
                    "meta": {
                        "err": None,
                        "logMessages": logs,
                    },
                },
                EventSource.SOLANA_WSS,
                received_at=received_at,
                generation=generation,
            )
            dispatched = True
        except asyncio.CancelledError:
            raise
        except Exception:
            self._fetch_error_sequences.add(sequence)
            self._dispatch_recovery_pending = True
            self.entry_gate.block("stream_fetch_error")
            self._reconnect_requested.set()
            logger.exception(
                "Solana ordered logs dispatch failed; reconnect requested",
                extra={"sequence": sequence},
            )
            raise
        finally:
            if dispatched:
                self._fetch_error_sequences.discard(sequence)
                if (
                    not self._fetch_error_sequences
                    and not self._dispatch_recovery_pending
                ):
                    self.entry_gate.unblock("stream_fetch_error")
            self._log_fetch_semaphore.release()

    async def _fetch_slot_block_time(self, slot: int) -> int | None:
        for attempt in range(len(self.BLOCK_TIME_RETRY_DELAYS) + 1):
            try:
                return await self.rpc.get_block_time(slot)
            except SolanaRpcError as error:
                if (
                    error.code != -32004
                    or attempt >= len(self.BLOCK_TIME_RETRY_DELAYS)
                ):
                    raise
                await asyncio.sleep(self.BLOCK_TIME_RETRY_DELAYS[attempt])
        raise RuntimeError("unreachable block-time retry state")

    async def _get_slot_block_time(self, slot: int) -> int:
        if slot <= 0:
            raise RuntimeError("Solana logs notification is missing a valid slot")
        cached = self._slot_block_times.get(slot)
        if cached is not None:
            return cached
        task = self._slot_block_time_tasks.get(slot)
        if task is None:
            task = asyncio.create_task(
                self._fetch_slot_block_time(slot),
                name=f"solana-block-time-{slot}",
            )
            self._slot_block_time_tasks[slot] = task
        try:
            block_time = await task
        finally:
            if self._slot_block_time_tasks.get(slot) is task:
                del self._slot_block_time_tasks[slot]
        if block_time is None:
            raise RuntimeError("confirmed Solana slot block time is unavailable")
        self._slot_block_times[slot] = block_time
        while len(self._slot_block_times) > self.SLOT_BLOCK_TIME_CACHE_SIZE:
            oldest_slot = next(iter(self._slot_block_times))
            del self._slot_block_times[oldest_slot]
        return block_time

    async def _acquire_log_fetch_permit(self) -> None:
        while True:
            if self._reconnect_requested.is_set():
                raise RuntimeError(
                    "ordered Solana transaction dispatch requested reconnect"
                )
            try:
                await asyncio.wait_for(
                    self._log_fetch_semaphore.acquire(),
                    timeout=0.25,
                )
            except TimeoutError:
                continue
            if self._reconnect_requested.is_set():
                self._log_fetch_semaphore.release()
                raise RuntimeError(
                    "ordered Solana transaction dispatch requested reconnect"
                )
            return

    async def _spawn_ordered_log_fetch(
        self,
        *,
        signature: str,
        slot: int,
        received_at: datetime,
        generation: int,
    ) -> None:
        await self._acquire_log_fetch_permit()
        sequence = self._log_fetch_sequence
        self._log_fetch_sequence += 1
        previous = self._log_fetch_tail
        task = asyncio.create_task(
            self._fetch_and_dispatch_log_transaction(
                sequence=sequence,
                signature=signature,
                slot=slot,
                received_at=received_at,
                generation=generation,
                previous=previous,
            ),
            name=f"solana-log-fetch-{sequence}",
        )
        self._log_fetch_tail = task
        self._log_fetch_tasks.add(task)
        task.add_done_callback(self._forget_log_fetch_task)
        self._sync_queue_depth()

    async def _fetch_and_dispatch_log_transaction(
        self,
        *,
        sequence: int,
        signature: str,
        slot: int,
        received_at: datetime,
        generation: int,
        previous: asyncio.Task[None] | None,
    ) -> None:
        attempt = 0
        dispatched = False
        try:
            while True:
                try:
                    transaction = await self.rpc.get_transaction(signature)
                    if transaction is None:
                        raise RuntimeError(
                            "confirmed transaction is temporarily unavailable"
                        )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    attempt += 1
                    self._fetch_error_sequences.add(sequence)
                    self.entry_gate.block("stream_fetch_error")
                    logger.warning(
                        "Solana transaction fetch failed; retrying in receive order",
                        extra={"attempt": attempt},
                    )
                    await asyncio.sleep(min(5, 0.25 * (2 ** min(attempt - 1, 5))))
                    continue
                break

            if previous is not None:
                await previous
            if any(failed < sequence for failed in self._fetch_error_sequences):
                raise RuntimeError("an earlier ordered transaction dispatch failed")
            transaction.setdefault("slot", slot)
            await self._queue_transaction(
                transaction,
                EventSource.SOLANA_WSS,
                received_at=received_at,
                generation=generation,
            )
            dispatched = True
        except asyncio.CancelledError:
            raise
        except Exception:
            self._fetch_error_sequences.add(sequence)
            self._dispatch_recovery_pending = True
            self.entry_gate.block("stream_fetch_error")
            self._reconnect_requested.set()
            logger.exception(
                "Solana ordered transaction dispatch failed; reconnect requested",
                extra={"sequence": sequence},
            )
            raise
        finally:
            if dispatched:
                self._fetch_error_sequences.discard(sequence)
                if (
                    not self._fetch_error_sequences
                    and not self._dispatch_recovery_pending
                ):
                    self.entry_gate.unblock("stream_fetch_error")
            self._log_fetch_semaphore.release()

    def _forget_log_fetch_task(self, task: asyncio.Task[None]) -> None:
        try:
            task.exception()
        except asyncio.CancelledError:
            pass
        self._log_fetch_tasks.discard(task)
        if self._log_fetch_tail is task:
            self._log_fetch_tail = None
        self._sync_queue_depth()

    async def _cancel_notification_dispatcher(self) -> None:
        dispatch_task = self._notification_dispatch_task
        if dispatch_task is not None:
            dispatch_task.cancel()
            await asyncio.gather(dispatch_task, return_exceptions=True)
            self._notification_dispatch_task = None
    async def _cancel_log_fetch_tasks(self) -> None:
        await self._cancel_notification_dispatcher()
        fetch_tasks = tuple(self._log_fetch_tasks)
        if fetch_tasks or not self._notification_queue.empty():
            self._dispatch_recovery_pending = True
            self.entry_gate.block("stream_recovery_gap")
            self.metrics.stream_recovery_gap_active.set(1)
        for task in fetch_tasks:
            task.cancel()
        if fetch_tasks:
            await asyncio.gather(*fetch_tasks, return_exceptions=True)
        self._log_fetch_tasks.clear()
        block_time_tasks = tuple(set(self._slot_block_time_tasks.values()))
        for block_time_task in block_time_tasks:
            block_time_task.cancel()
        if block_time_tasks:
            await asyncio.gather(*block_time_tasks, return_exceptions=True)
        self._slot_block_time_tasks.clear()
        self._log_fetch_tail = None
        self._fetch_error_sequences.clear()
        self._sync_queue_depth()

    async def _queue_transaction(
        self,
        transaction: dict[str, Any],
        source: EventSource,
        *,
        received_at: datetime | None = None,
        generation: int | None = None,
    ) -> None:
        received = received_at or datetime.now(tz=timezone.utc)
        slot = int(transaction.get("slot", 0))
        signature = _transaction_signature(transaction)
        self.last_slot = max(self.last_slot, slot)
        self.last_signature = signature or self.last_signature
        if self.last_observed_at is None or received > self.last_observed_at:
            self.last_observed_at = received
        block_time = _transaction_block_time(transaction)
        if block_time is not None and (
            self.last_chain_block_time is None
            or block_time > self.last_chain_block_time
        ):
            self.last_chain_block_time = block_time
        queued_source = source
        if source in {EventSource.HELIUS_WSS, EventSource.SOLANA_WSS}:
            if self._baseline_started_at is None:
                self._baseline_started_at = received
            if (
                received - self._baseline_started_at < self.LIVE_BASELINE_WARMUP
                or (generation is not None and generation != self._stream_generation)
            ):
                queued_source = EventSource.BASELINE_WSS
        protocols = _transaction_protocols(transaction)
        for protocol in protocols:
            if signature:
                self._last_signatures[protocol] = signature
            await self._queue.put((protocol, transaction, queued_source))
        self._sync_queue_depth()

    def _checkpoint_is_recent(self, now: datetime | None = None) -> bool:
        if self.last_slot <= 0 or self.last_observed_at is None:
            return False
        current = now or datetime.now(tz=timezone.utc)
        age = current - self.last_observed_at
        return timedelta(0) <= age <= self.MAX_GAP_RECOVERY_AGE

    def _begin_live_baseline(self) -> None:
        self._stream_generation += 1
        self._baseline_started_at = None
        self.entry_gate.block("stream_baseline")
        self.entry_gate.block("stream_stale")

    def _discard_in_memory_checkpoint(self) -> None:
        self.last_slot = 0
        self.last_signature = None
        self.last_observed_at = None
        self.last_chain_block_time = None
        self.last_processed_block_time = None
        self.last_processing_lag_seconds = None
        self._last_signatures.clear()
        self._begin_live_baseline()

    async def _commit_recovered_events(
        self,
        recovered: list[tuple[Protocol, dict[str, Any], EventSource]],
    ) -> None:
        for protocol, transaction, source in recovered:
            signature = _transaction_signature(transaction)
            slot = int(transaction.get("slot", 0))
            self.last_slot = max(self.last_slot, slot)
            self.last_signature = signature or self.last_signature
            if signature:
                self._last_signatures[protocol] = signature
            await self._queue.put((protocol, transaction, source))
        self._sync_queue_depth()

    async def _recover_gap_with_live_buffer(
        self,
        websocket: Any,
        initial_messages: list[dict[str, Any]],
    ) -> tuple[
        list[tuple[Protocol, dict[str, Any], EventSource]],
        list[dict[str, Any]],
    ]:
        buffered = list(initial_messages)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.GAP_RECOVERY_TIMEOUT_SECONDS
        recovery_task = asyncio.create_task(
            self._recover_gap(),
            name="solana-gap-recovery",
        )
        receive_task = asyncio.create_task(
            websocket.recv(),
            name="solana-gap-live-buffer",
        )
        try:
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise _GapRecoveryTimeout(
                        "Solana gap recovery timed out; "
                        "checkpoint retained; retrying fail-closed",
                        buffered,
                    )
                done, _ = await asyncio.wait(
                    {recovery_task, receive_task},
                    timeout=remaining,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    raise _GapRecoveryTimeout(
                        "Solana gap recovery timed out; "
                        "checkpoint retained; retrying fail-closed",
                        buffered,
                    )
                if receive_task in done:
                    buffered.append(json.loads(receive_task.result()))
                    if len(buffered) > self.SUBSCRIPTION_MESSAGE_BUFFER_LIMIT:
                        raise _GapRecoveryTimeout(
                            "Solana live buffer overflow during gap recovery; "
                            "checkpoint retained; retrying fail-closed",
                            buffered,
                        )
                    receive_task = asyncio.create_task(
                        websocket.recv(),
                        name="solana-gap-live-buffer",
                    )
                if recovery_task in done:
                    return recovery_task.result(), buffered
        finally:
            for task in (recovery_task, receive_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(
                recovery_task,
                receive_task,
                return_exceptions=True,
            )

    async def _recover_gap(
        self,
    ) -> list[tuple[Protocol, dict[str, Any], EventSource]]:
        recovered: dict[str, tuple[Protocol, dict[str, Any]]] = {}
        for protocol, program_id in (
            (Protocol.PUMP, PUMP_PROGRAM_ID),
            (Protocol.PUMPSWAP, PUMPSWAP_PROGRAM_ID),
        ):
            checkpoint = self._last_signatures.get(protocol)
            before: str | None = None
            while True:
                signatures = await self.rpc.get_signatures_for_address(
                    program_id,
                    until=checkpoint,
                    before=before,
                    limit=1000,
                )
                async def fetch_transaction(
                    item: dict[str, Any],
                ) -> tuple[str, dict[str, Any]] | None:
                    if item.get("err") is not None:
                        return None
                    signature = str(item.get("signature") or "")
                    if not signature:
                        return None
                    async with self._gap_recovery_semaphore:
                        transaction = await self.rpc.get_transaction(signature)
                    if transaction is None:
                        return None
                    transaction.setdefault("slot", item.get("slot", 0))
                    return signature, transaction

                fetched = await asyncio.gather(
                    *(fetch_transaction(item) for item in signatures)
                )
                for result in fetched:
                    if result is None:
                        continue
                    signature, transaction = result
                    recovered[f"{protocol.value}:{signature}"] = (
                        protocol,
                        transaction,
                    )
                if checkpoint is None or len(signatures) < 1000:
                    break
                next_before = str(signatures[-1].get("signature") or "")
                if not next_before or next_before == before:
                    raise RuntimeError("gap recovery pagination did not advance")
                before = next_before
        return [
            (protocol, transaction, EventSource.RPC_RECOVERY)
            for protocol, transaction in sorted(
                recovered.values(),
                key=lambda item: int(item[1].get("slot", 0)),
            )
        ]

    async def _worker(self) -> None:
        while True:
            item = await self._queue.get()
            self._sync_queue_depth()
            if item is None:
                self._queue.task_done()
                return
            batch = [item]
            stop_after_batch = False
            if self.batch_handler is not None:
                loop = asyncio.get_running_loop()
                deadline = loop.time() + self.PROCESSING_BATCH_WINDOW_SECONDS
                while len(batch) < self.PROCESSING_BATCH_SIZE:
                    timeout = deadline - loop.time()
                    if timeout <= 0:
                        break
                    try:
                        next_item = await asyncio.wait_for(self._queue.get(), timeout=timeout)
                    except TimeoutError:
                        break
                    if next_item is None:
                        stop_after_batch = True
                        break
                    batch.append(next_item)
                self._sync_queue_depth()
            attempt = 0
            while True:
                try:
                    if self.batch_handler is not None:
                        await self.batch_handler(batch)
                    else:
                        protocol, transaction, source = batch[0]
                        await self.handler(protocol, transaction, source)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    attempt += 1
                    logger.exception(
                        "chain event batch processing failed; retrying in order",
                        extra={"batch_size": len(batch), "attempt": attempt},
                    )
                    self.entry_gate.block("event_processing_error")
                    if attempt >= self.PROCESSING_RETRY_LIMIT:
                        if self.fatal_handler is not None:
                            self.fatal_handler(error)
                        for _ in batch:
                            self._queue.task_done()
                        if stop_after_batch:
                            self._queue.task_done()
                        raise
                    await asyncio.sleep(self.PROCESSING_RETRY_DELAYS[attempt - 1])
                    continue
                self.entry_gate.unblock("event_processing_error")
                processed_at = datetime.now(tz=timezone.utc)
                block_times = [
                    block_time
                    for _, transaction, _ in batch
                    if (block_time := _transaction_block_time(transaction)) is not None
                ]
                if block_times:
                    latest_block_time = max(block_times)
                    if (
                        self.last_processed_block_time is None
                        or latest_block_time > self.last_processed_block_time
                    ):
                        self.last_processed_block_time = latest_block_time
                    self.last_processing_lag_seconds = max(
                        0.0, (processed_at - latest_block_time).total_seconds()
                    )
                break
            for _ in batch:
                self._queue.task_done()
            if stop_after_batch:
                self._queue.task_done()
                return

    def refresh_freshness(self, now: datetime | None = None) -> bool:
        now = now or datetime.now(tz=timezone.utc)
        transport_stale = (
            self.last_observed_at is None
            or (now - self.last_observed_at).total_seconds()
            > self.max_processing_lag_seconds
        )
        processing_lag = self.last_processing_lag_seconds
        if self.last_processed_block_time is not None:
            advancing_lag = max(
                0.0,
                (now - self.last_processed_block_time).total_seconds(),
            )
            processing_lag = max(processing_lag or 0.0, advancing_lag)
        processing_stale = (
            processing_lag is None
            or processing_lag > self.max_processing_lag_seconds
        )
        stale = transport_stale or processing_stale
        if stale:
            self.entry_gate.block("stream_stale")
        else:
            self.entry_gate.unblock("stream_stale")

        baseline_ready = (
            self._baseline_started_at is not None
            and now - self._baseline_started_at >= self.LIVE_BASELINE_WARMUP
        )
        if baseline_ready:
            self.entry_gate.unblock("stream_baseline")
            if (
                self._dispatch_recovery_pending
                and not stale
                and not self._reconnect_requested.is_set()
                and not self._fetch_error_sequences
            ):
                self._dispatch_recovery_pending = False
                self.entry_gate.unblock("stream_fetch_error")
        else:
            self.entry_gate.block("stream_baseline")
        return not stale and baseline_ready

    def _sync_queue_depth(self) -> None:
        processing_depth = self._queue.qsize()
        notification_depth = self._notification_queue.qsize()
        dispatch_depth = len(self._log_fetch_tasks)
        self.metrics.event_processing_queue_depth.set(processing_depth)
        self.metrics.event_notification_queue_depth.set(notification_depth)
        self.metrics.event_log_dispatch_tasks.set(dispatch_depth)
        self.metrics.event_queue_depth.set(
            processing_depth + notification_depth + dispatch_depth
        )


def _transaction_protocols(transaction: dict[str, Any]) -> list[Protocol]:
    meta = transaction.get("meta") or transaction.get("transaction", {}).get("meta") or {}
    logs = meta.get("logMessages") or transaction.get("logs") or []
    text = "\n".join(str(line) for line in logs)
    result: list[Protocol] = []
    if PUMP_PROGRAM_ID in text:
        result.append(Protocol.PUMP)
    if PUMPSWAP_PROGRAM_ID in text:
        result.append(Protocol.PUMPSWAP)
    return result


def _transaction_signature(transaction: dict[str, Any]) -> str | None:
    if transaction.get("signature"):
        return str(transaction["signature"])
    signatures = transaction.get("transaction", {}).get("signatures") or []
    return str(signatures[0]) if signatures else None


def _transaction_block_time(transaction: dict[str, Any]) -> datetime | None:
    raw = transaction.get("blockTime")
    if raw is None:
        raw = (transaction.get("transaction") or {}).get("blockTime")
    if raw is None:
        return None
    try:
        return datetime.fromtimestamp(int(raw), tz=timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return None
