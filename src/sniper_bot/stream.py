"""Helius stream gateway with fail-closed reconnect and slot-gap recovery."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

import websockets

from .events import EventSource, Protocol
from .metrics import BotMetrics
from .protocols.pump import PUMP_PROGRAM_ID
from .protocols.pumpswap import PUMPSWAP_PROGRAM_ID
from .solana_rpc import SolanaRpcClient

logger = logging.getLogger(__name__)

TransactionHandler = Callable[[Protocol, dict[str, Any], EventSource], Awaitable[None]]


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


class HeliusStreamGateway:
    BACKOFF_SECONDS = (1, 2, 4, 8, 15)
    SUBSCRIPTION_ACK_TIMEOUT_SECONDS = 10.0
    SUBSCRIPTION_MESSAGE_BUFFER_LIMIT = 1024

    def __init__(
        self,
        *,
        websocket_url: str,
        rpc: SolanaRpcClient,
        handler: TransactionHandler,
        entry_gate: EntryGate,
        metrics: BotMetrics,
        queue_size: int = 2000,
    ) -> None:
        self.websocket_url = websocket_url
        self.rpc = rpc
        self.handler = handler
        self.entry_gate = entry_gate
        self.metrics = metrics
        self.last_slot = 0
        self.last_signature: str | None = None
        self._last_signatures: dict[Protocol, str] = {}
        self.last_observed_at: datetime | None = None
        self._queue: asyncio.Queue[tuple[Protocol, dict[str, Any], EventSource] | None] = asyncio.Queue(maxsize=queue_size)
        self._run_task: asyncio.Task[None] | None = None
        self._worker_task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()

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
        self._stopping.set()
        if self._run_task is not None:
            self._run_task.cancel()
            try:
                await self._run_task
            except asyncio.CancelledError:
                pass
            self._run_task = None
        if self._worker_task is not None:
            await self._queue.put(None)
            await self._worker_task
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
                    ping_interval=30,
                    ping_timeout=15,
                    close_timeout=5,
                    max_queue=1024,
                ) as websocket:
                    try:
                        buffered = await self._subscribe(websocket, logs_only=logs_only)
                        if (
                            len(pending_handshake_messages) + len(buffered)
                            > self.SUBSCRIPTION_MESSAGE_BUFFER_LIMIT
                        ):
                            logger.error("subscription handshake message buffer overflow")
                            return
                        pending_handshake_messages.extend(buffered)
                        while pending_handshake_messages:
                            await self.handle_message(pending_handshake_messages[0])
                            del pending_handshake_messages[0]
                        if self.last_slot:
                            await self._recover_gap()
                        self.entry_gate.unblock("startup")
                        self.entry_gate.unblock("stream_disconnected")
                        self.entry_gate.unblock("stream_stale")
                        attempt = 0
                        async for raw_message in websocket:
                            await self.handle_message(json.loads(raw_message))
                    finally:
                        self.entry_gate.block("stream_disconnected")
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

    async def handle_message(self, message: dict[str, Any]) -> None:
        if message.get("error"):
            raise RuntimeError("Helius subscription returned an error")
        method = str(message.get("method") or "")
        if not method.endswith("Notification"):
            return
        result = ((message.get("params") or {}).get("result") or {})
        transaction = result.get("transaction") if isinstance(result, dict) else None
        context = result.get("context") if isinstance(result, dict) else None
        if isinstance(transaction, dict):
            if "slot" not in transaction and isinstance(context, dict):
                transaction["slot"] = context.get("slot", 0)
            await self._queue_transaction(transaction, EventSource.HELIUS_WSS)
            return
        value = result.get("value") if isinstance(result, dict) else None
        if isinstance(value, dict) and value.get("signature"):
            transaction = await self.rpc.get_transaction(str(value["signature"]))
            if transaction is not None:
                if isinstance(context, dict):
                    transaction["slot"] = context.get("slot", transaction.get("slot", 0))
                await self._queue_transaction(transaction, EventSource.SOLANA_WSS)

    async def _queue_transaction(self, transaction: dict[str, Any], source: EventSource) -> None:
        slot = int(transaction.get("slot", 0))
        signature = _transaction_signature(transaction)
        self.last_slot = max(self.last_slot, slot)
        self.last_signature = signature or self.last_signature
        self.last_observed_at = datetime.now(tz=timezone.utc)
        protocols = _transaction_protocols(transaction)
        for protocol in protocols:
            if signature:
                self._last_signatures[protocol] = signature
            await self._queue.put((protocol, transaction, source))
        self.metrics.event_queue_depth.set(self._queue.qsize())

    async def _recover_gap(self) -> None:
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
                for item in signatures:
                    if item.get("err") is not None:
                        continue
                    signature = str(item.get("signature") or "")
                    if not signature:
                        continue
                    transaction = await self.rpc.get_transaction(signature)
                    if transaction is None:
                        continue
                    transaction.setdefault("slot", item.get("slot", 0))
                    recovered[f"{protocol.value}:{signature}"] = (protocol, transaction)
                if checkpoint is None or len(signatures) < 1000:
                    break
                next_before = str(signatures[-1].get("signature") or "")
                if not next_before or next_before == before:
                    raise RuntimeError("gap recovery pagination did not advance")
                before = next_before
        for protocol, transaction in sorted(
            recovered.values(), key=lambda item: int(item[1].get("slot", 0))
        ):
            await self._queue.put((protocol, transaction, EventSource.RPC_RECOVERY))
        if recovered:
            self.metrics.websocket_gap_recoveries.inc()

    async def _worker(self) -> None:
        while True:
            item = await self._queue.get()
            self.metrics.event_queue_depth.set(self._queue.qsize())
            if item is None:
                return
            protocol, transaction, source = item
            attempt = 0
            while True:
                try:
                    await self.handler(protocol, transaction, source)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    attempt += 1
                    logger.exception(
                        "chain event processing failed; retrying in order",
                        extra={"protocol": protocol.value, "attempt": attempt},
                    )
                    self.entry_gate.block("event_processing_error")
                    await asyncio.sleep(min(30, 2 ** min(attempt - 1, 5)))
                    continue
                self.entry_gate.unblock("event_processing_error")
                break

    def refresh_freshness(self, now: datetime | None = None) -> bool:
        now = now or datetime.now(tz=timezone.utc)
        stale = self.last_observed_at is None or (now - self.last_observed_at).total_seconds() > 3
        if stale:
            self.entry_gate.block("stream_stale")
        else:
            self.entry_gate.unblock("stream_stale")
        return not stale


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
