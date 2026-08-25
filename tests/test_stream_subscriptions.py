from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from sniper_bot.events import EventSource, Protocol
from sniper_bot.metrics import BotMetrics
from sniper_bot.solana_rpc import SolanaRpcClient
from sniper_bot.stream import (
    EntryGate,
    HeliusStreamGateway,
    _SubscriptionHandshakeError,
    _UseLogsFallback,
)


async def _handler(*_args: Any) -> None:
    return None


def _gateway() -> HeliusStreamGateway:
    gateway = HeliusStreamGateway(
        websocket_url="wss://example.invalid",
        rpc=SolanaRpcClient("https://example.invalid"),
        handler=_handler,
        entry_gate=EntryGate(BotMetrics()),
        metrics=BotMetrics(),
    )
    gateway.SUBSCRIPTION_ACK_TIMEOUT_SECONDS = 0.01
    return gateway


class FakeWebSocket:
    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self.responses = list(responses)
        self.sent: list[dict[str, Any]] = []

    async def send(self, value: str) -> None:
        self.sent.append(json.loads(value))

    async def recv(self) -> str:
        if self.responses:
            return json.dumps(self.responses.pop(0))
        await asyncio.sleep(60)
        raise AssertionError("unreachable")


@pytest.mark.asyncio
async def test_transaction_subscription_timeout_requires_new_fallback_connection() -> None:
    websocket = FakeWebSocket([])

    with pytest.raises(_UseLogsFallback, match="timed out"):
        await _gateway()._subscribe(websocket)

    assert [item["method"] for item in websocket.sent] == [
        "transactionSubscribe",
        "transactionSubscribe",
    ]


@pytest.mark.asyncio
async def test_partial_transaction_ack_requires_new_fallback_connection() -> None:
    websocket = FakeWebSocket([{"jsonrpc": "2.0", "id": 1, "result": 11}])

    with pytest.raises(_UseLogsFallback, match="timed out"):
        await _gateway()._subscribe(websocket)


@pytest.mark.asyncio
async def test_invalid_transaction_ack_requires_fallback() -> None:
    websocket = FakeWebSocket(
        [
            {"jsonrpc": "2.0", "id": 1, "result": 11},
            {"jsonrpc": "2.0", "id": 2, "error": {}},
        ]
    )

    with pytest.raises(_UseLogsFallback, match="unavailable"):
        await _gateway()._subscribe(websocket)


@pytest.mark.asyncio
async def test_transaction_subscriptions_accept_only_valid_numeric_results() -> None:
    websocket = FakeWebSocket(
        [
            {"jsonrpc": "2.0", "id": 1, "result": 11},
            {"jsonrpc": "2.0", "id": 2, "result": 12},
        ]
    )

    await _gateway()._subscribe(websocket)

    assert [item["method"] for item in websocket.sent] == [
        "transactionSubscribe",
        "transactionSubscribe",
    ]


@pytest.mark.asyncio
async def test_logs_only_connection_uses_separate_program_subscriptions() -> None:
    websocket = FakeWebSocket(
        [
            {"jsonrpc": "2.0", "id": 101, "result": 201},
            {"jsonrpc": "2.0", "id": 102, "result": 202},
        ]
    )

    await _gateway()._subscribe(websocket, logs_only=True)

    assert [item["method"] for item in websocket.sent] == [
        "logsSubscribe",
        "logsSubscribe",
    ]
    mentions = [item["params"][0]["mentions"] for item in websocket.sent]
    assert all(len(addresses) == 1 for addresses in mentions)
    assert mentions[0] != mentions[1]


@pytest.mark.asyncio
async def test_logs_subscription_timeout_fails_closed() -> None:
    websocket = FakeWebSocket([])

    with pytest.raises(_SubscriptionHandshakeError, match="acknowledgement failed"):
        await _gateway()._subscribe(websocket, logs_only=True)


@pytest.mark.asyncio
async def test_interleaved_notification_is_buffered_until_handshake_completes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    notification = {
        "jsonrpc": "2.0",
        "method": "transactionNotification",
        "params": {},
    }
    websocket = FakeWebSocket(
        [
            notification,
            {"jsonrpc": "2.0", "id": 1, "result": 11},
            {"jsonrpc": "2.0", "id": 2, "result": 12},
        ]
    )
    gateway = _gateway()
    monkeypatch.setattr(gateway, "handle_message", pytest.fail)

    buffered = await gateway._subscribe(websocket)

    assert websocket.responses == []
    assert buffered == [notification]


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_id", [True, 1.0, "1"])
async def test_acknowledgement_id_requires_exact_integer_type(invalid_id: object) -> None:
    websocket = FakeWebSocket(
        [
            {"jsonrpc": "2.0", "id": invalid_id, "result": 11},
            {"jsonrpc": "2.0", "id": 2, "result": 12},
        ]
    )

    with pytest.raises(_UseLogsFallback, match="invalid acknowledgement id"):
        await _gateway()._subscribe(websocket)


@pytest.mark.asyncio
async def test_acknowledgement_requires_jsonrpc_version() -> None:
    websocket = FakeWebSocket(
        [
            {"id": 1, "result": 11},
            {"jsonrpc": "2.0", "id": 2, "result": 12},
        ]
    )

    with pytest.raises(_UseLogsFallback, match="JSON-RPC version"):
        await _gateway()._subscribe(websocket)


class FakeConnection(FakeWebSocket):
    def __init__(
        self,
        responses: list[dict[str, Any]],
        *,
        finish_iteration: bool,
        ready_event: asyncio.Event | None = None,
    ) -> None:
        super().__init__(responses)
        self.finish_iteration = finish_iteration
        self.ready_event = ready_event
        self._handshake_responses_remaining = len(responses)

    async def recv(self) -> str:
        if self._handshake_responses_remaining > 0:
            self._handshake_responses_remaining -= 1
            return await super().recv()
        return await self.__anext__()

    async def send(self, value: str) -> None:
        await super().send(value)
        if self.ready_event is not None and len(self.sent) >= 2:
            self.ready_event.set()

    def __aiter__(self) -> FakeConnection:
        return self

    async def __anext__(self) -> str:
        if self.finish_iteration:
            raise StopAsyncIteration
        await asyncio.sleep(60)
        raise StopAsyncIteration


class FakeConnectionContext:
    def __init__(
        self,
        connection: FakeConnection,
        events: list[str],
        name: str,
        *,
        exit_started: asyncio.Event | None = None,
        release_exit: asyncio.Event | None = None,
    ) -> None:
        self.connection = connection
        self.events = events
        self.name = name
        self.exit_started = exit_started
        self.release_exit = release_exit

    async def __aenter__(self) -> FakeConnection:
        self.events.append(f"open:{self.name}")
        return self.connection

    async def __aexit__(self, *_args: object) -> None:
        self.events.append(f"close:{self.name}")
        if self.exit_started is not None:
            self.exit_started.set()
        if self.release_exit is not None:
            await self.release_exit.wait()


@pytest.mark.asyncio
async def test_run_closes_transaction_socket_and_persists_logs_only_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = FakeConnection([], finish_iteration=False)
    fallback_acks = [
        {"jsonrpc": "2.0", "id": 101, "result": 201},
        {"jsonrpc": "2.0", "id": 102, "result": 202},
    ]
    second = FakeConnection(fallback_acks, finish_iteration=True)
    third_ready = asyncio.Event()
    third = FakeConnection(
        fallback_acks,
        finish_iteration=False,
        ready_event=third_ready,
    )
    connections = [first, second, third]
    events: list[str] = []

    def connect(*_args: object, **_kwargs: object) -> FakeConnectionContext:
        index = 3 - len(connections)
        connection = connections.pop(0)
        return FakeConnectionContext(connection, events, str(index + 1))

    monkeypatch.setattr("sniper_bot.stream.websockets.connect", connect)
    gateway = _gateway()
    run_task = asyncio.create_task(gateway._run())
    try:
        async with asyncio.timeout(1):
            await third_ready.wait()
        assert events[:5] == ["open:1", "close:1", "open:2", "close:2", "open:3"]
        assert [item["method"] for item in second.sent] == [
            "logsSubscribe",
            "logsSubscribe",
        ]
        assert [item["method"] for item in third.sent] == [
            "logsSubscribe",
            "logsSubscribe",
        ]
        assert gateway.entry_gate.enabled is False
        assert "startup" not in gateway.entry_gate.reasons
        assert "stream_disconnected" not in gateway.entry_gate.reasons
        assert "stream_stale" in gateway.entry_gate.reasons
    finally:
        run_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run_task


@pytest.mark.asyncio
async def test_live_baseline_tags_transactions_non_tradable_until_warmup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = _gateway()
    gateway._begin_live_baseline()
    monkeypatch.setattr(
        "sniper_bot.stream._transaction_protocols",
        lambda _transaction: [Protocol.PUMP],
    )
    transaction = {"slot": 1, "signature": "baseline-signature"}

    await gateway._queue_transaction(transaction, EventSource.SOLANA_WSS)
    _, _, baseline_source = gateway._queue.get_nowait()
    assert baseline_source == EventSource.BASELINE_WSS

    gateway._baseline_started_at = (
        datetime.now(tz=timezone.utc) - gateway.LIVE_BASELINE_WARMUP
    )
    transaction = {"slot": 2, "signature": "live-signature"}
    await gateway._queue_transaction(transaction, EventSource.SOLANA_WSS)
    _, _, live_source = gateway._queue.get_nowait()
    assert live_source == EventSource.SOLANA_WSS


@pytest.mark.asyncio
async def test_gap_recovery_timeout_buffers_live_event_without_partial_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    notification = {
        "jsonrpc": "2.0",
        "method": "transactionNotification",
        "params": {},
    }
    connection = FakeConnection(
        [
            {"jsonrpc": "2.0", "id": 1, "result": 11},
            {"jsonrpc": "2.0", "id": 2, "result": 12},
            notification,
        ],
        finish_iteration=False,
    )
    events: list[str] = []

    def connect(*_args: object, **_kwargs: object) -> FakeConnectionContext:
        return FakeConnectionContext(connection, events, "timeout")

    gateway = _gateway()
    gateway.GAP_RECOVERY_TIMEOUT_SECONDS = 0.01
    gateway.restore_checkpoint(
        123,
        "recent-signature",
        datetime.now(tz=timezone.utc),
    )
    gateway.restore_protocol_checkpoint(Protocol.PUMP, "recent-signature")
    recovery_cancelled = asyncio.Event()
    live_processed = asyncio.Event()
    processed: list[dict[str, Any]] = []

    async def slow_recovery() -> list[tuple[Protocol, dict[str, Any], object]]:
        try:
            await asyncio.sleep(60)
        finally:
            recovery_cancelled.set()
        return []

    async def handle_message(message: dict[str, Any]) -> None:
        processed.append(message)
        live_processed.set()

    monkeypatch.setattr("sniper_bot.stream.websockets.connect", connect)
    monkeypatch.setattr(gateway, "_recover_gap", slow_recovery)
    monkeypatch.setattr(gateway, "handle_message", handle_message)
    run_task = asyncio.create_task(gateway._run())
    try:
        async with asyncio.timeout(1):
            await live_processed.wait()
            await recovery_cancelled.wait()

        assert processed == [notification]
        assert gateway._queue.empty()
        assert gateway.last_slot == 0
        assert gateway.last_signature is None
        assert gateway._last_signatures == {}
        assert "startup" not in gateway.entry_gate.reasons
        assert "stream_disconnected" not in gateway.entry_gate.reasons
        assert "stream_baseline" in gateway.entry_gate.reasons
        assert "stream_stale" in gateway.entry_gate.reasons
    finally:
        run_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run_task


@pytest.mark.asyncio
async def test_stale_checkpoint_skips_gap_recovery_and_waits_for_live_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = FakeConnection(
        [
            {"jsonrpc": "2.0", "id": 1, "result": 11},
            {"jsonrpc": "2.0", "id": 2, "result": 12},
        ],
        finish_iteration=False,
    )
    events: list[str] = []

    def connect(*_args: object, **_kwargs: object) -> FakeConnectionContext:
        return FakeConnectionContext(connection, events, "stale")

    gateway = _gateway()
    gateway.restore_checkpoint(
        123,
        "old-signature",
        datetime.now(tz=timezone.utc) - timedelta(minutes=5),
    )
    gateway.restore_protocol_checkpoint(Protocol.PUMP, "old-signature")

    async def unexpected_recovery() -> None:
        pytest.fail("stale checkpoint must not trigger historical gap recovery")

    checkpoint_discarded = asyncio.Event()
    discard_checkpoint = gateway._discard_in_memory_checkpoint

    def discard_and_notify() -> None:
        discard_checkpoint()
        checkpoint_discarded.set()

    monkeypatch.setattr("sniper_bot.stream.websockets.connect", connect)
    monkeypatch.setattr(gateway, "_recover_gap", unexpected_recovery)
    monkeypatch.setattr(
        gateway,
        "_discard_in_memory_checkpoint",
        discard_and_notify,
    )
    run_task = asyncio.create_task(gateway._run())
    try:
        async with asyncio.timeout(1):
            await checkpoint_discarded.wait()

        assert gateway.last_slot == 0
        assert gateway.last_signature is None
        assert gateway.last_observed_at is None
        assert gateway._last_signatures == {}
        assert "stream_disconnected" not in gateway.entry_gate.reasons
        assert "stream_stale" in gateway.entry_gate.reasons
    finally:
        run_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run_task


@pytest.mark.asyncio
async def test_triggering_overflow_message_forces_fail_closed_buffer() -> None:
    gateway = _gateway()
    gateway.SUBSCRIPTION_MESSAGE_BUFFER_LIMIT = 1
    notification = {"jsonrpc": "2.0", "method": "transactionNotification"}
    websocket = FakeWebSocket([notification, notification])

    with pytest.raises(_UseLogsFallback) as caught:
        await gateway._subscribe(websocket)

    assert len(caught.value.buffered_messages) == 2
    pending: list[dict[str, Any]] = []
    assert gateway._extend_handshake_buffer(pending, caught.value.buffered_messages) is False
    assert "stream_disconnected" in gateway.entry_gate.reasons


@pytest.mark.asyncio
async def test_gate_blocks_before_connection_context_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = FakeConnection(
        [
            {"jsonrpc": "2.0", "id": 1, "result": 11},
            {"jsonrpc": "2.0", "id": 2, "result": 12},
        ],
        finish_iteration=True,
    )
    exit_started = asyncio.Event()
    release_exit = asyncio.Event()
    events: list[str] = []

    def connect(*_args: object, **_kwargs: object) -> FakeConnectionContext:
        return FakeConnectionContext(
            connection,
            events,
            "only",
            exit_started=exit_started,
            release_exit=release_exit,
        )

    monkeypatch.setattr("sniper_bot.stream.websockets.connect", connect)
    gateway = _gateway()
    run_task = asyncio.create_task(gateway._run())
    try:
        async with asyncio.timeout(1):
            await exit_started.wait()
        assert gateway.entry_gate.enabled is False
        assert "stream_disconnected" in gateway.entry_gate.reasons
        gateway._stopping.set()
        release_exit.set()
        await run_task
    finally:
        if not run_task.done():
            run_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await run_task


@pytest.mark.asyncio
async def test_failed_buffer_handler_retries_only_unconfirmed_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    notification = {"jsonrpc": "2.0", "method": "transactionNotification"}
    first = FakeConnection(
        [
            notification,
            {"jsonrpc": "2.0", "id": 1, "result": 11},
            {"jsonrpc": "2.0", "id": 2, "result": 12},
        ],
        finish_iteration=True,
    )
    second = FakeConnection(
        [
            {"jsonrpc": "2.0", "id": 1, "result": 21},
            {"jsonrpc": "2.0", "id": 2, "result": 22},
        ],
        finish_iteration=False,
    )
    connections = [first, second]
    events: list[str] = []

    def connect(*_args: object, **_kwargs: object) -> FakeConnectionContext:
        connection = connections.pop(0)
        return FakeConnectionContext(connection, events, str(len(events)))

    monkeypatch.setattr("sniper_bot.stream.websockets.connect", connect)
    gateway = _gateway()
    gateway.BACKOFF_SECONDS = (0,)
    calls: list[dict[str, Any]] = []
    handled = asyncio.Event()

    async def handle(message: dict[str, Any]) -> None:
        calls.append(message)
        if len(calls) == 1:
            raise RuntimeError("transient")
        handled.set()

    monkeypatch.setattr(gateway, "handle_message", handle)
    run_task = asyncio.create_task(gateway._run())
    try:
        async with asyncio.timeout(1):
            await handled.wait()
        assert calls == [notification, notification]
    finally:
        run_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run_task


@pytest.mark.asyncio
async def test_logs_notification_uses_cached_block_time_without_transaction_fetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = _gateway()
    gateway.SLOT_BLOCK_TIME_CACHE_SIZE = 2
    block_time_calls: list[int] = []
    queued: list[tuple[dict[str, Any], EventSource, dict[str, Any]]] = []

    async def get_block_time(slot: int) -> int:
        block_time_calls.append(slot)
        return 1_787_646_900 + slot

    async def unexpected_get_transaction(_signature: str) -> dict[str, Any]:
        pytest.fail("logsNotification must not fetch the full transaction")

    async def queue_transaction(
        transaction: dict[str, Any],
        source: EventSource,
        **kwargs: Any,
    ) -> None:
        queued.append((transaction, source, kwargs))

    monkeypatch.setattr(gateway.rpc, "get_block_time", get_block_time)
    monkeypatch.setattr(gateway.rpc, "get_transaction", unexpected_get_transaction)
    monkeypatch.setattr(gateway, "_queue_transaction", queue_transaction)

    def notification(signature: str, slot: int) -> dict[str, Any]:
        return {
            "jsonrpc": "2.0",
            "method": "logsNotification",
            "params": {
                "result": {
                    "context": {"slot": slot},
                    "value": {
                        "signature": signature,
                        "err": None,
                        "logs": ["Program data: example"],
                    },
                }
            },
        }

    await gateway.handle_message(notification("first", 1))
    await gateway.handle_message(notification("second", 1))
    await gateway.handle_message(notification("third", 2))
    await gateway.handle_message(notification("fourth", 3))
    tasks = tuple(gateway._log_fetch_tasks)
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)
    await asyncio.sleep(0)

    assert block_time_calls == [1, 2, 3]
    assert [row[0]["signature"] for row in queued] == [
        "first",
        "second",
        "third",
        "fourth",
    ]
    assert queued[0][0]["blockTime"] == 1_787_646_901
    assert queued[0][0]["meta"]["logMessages"] == ["Program data: example"]
    assert all(row[1] == EventSource.SOLANA_WSS for row in queued)
    assert list(gateway._slot_block_times) == [2, 3]
    assert gateway._log_fetch_tasks == set()


@pytest.mark.asyncio
async def test_logs_notification_block_time_failure_is_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = _gateway()

    async def missing_block_time(_slot: int) -> None:
        return None

    async def queue_transaction(
        _transaction: dict[str, Any],
        _source: EventSource,
        **_kwargs: Any,
    ) -> None:
        pytest.fail("unavailable block time must not dispatch a transaction")

    monkeypatch.setattr(gateway.rpc, "get_block_time", missing_block_time)
    monkeypatch.setattr(gateway, "_queue_transaction", queue_transaction)
    notification = {
        "jsonrpc": "2.0",
        "method": "logsNotification",
        "params": {
            "result": {
                "context": {"slot": 123},
                "value": {
                    "signature": "missing-time",
                    "err": None,
                    "logs": ["Program data: example"],
                },
            }
        },
    }

    await gateway.handle_message(notification)
    tasks = tuple(gateway._log_fetch_tasks)
    results = await asyncio.wait_for(
        asyncio.gather(*tasks, return_exceptions=True),
        timeout=1,
    )

    assert len(results) == 1
    assert isinstance(results[0], RuntimeError)
    assert "block time is unavailable" in str(results[0])
    assert gateway._reconnect_requested.is_set()
    assert gateway._dispatch_recovery_pending is True
    assert "stream_fetch_error" in gateway.entry_gate.reasons


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value, slot, message",
    [
        (
            {
                "signature": "missing-err",
                "logs": ["Program data: example"],
            },
            123,
            "required err field",
        ),
        (
            {
                "signature": "missing-logs",
                "err": None,
            },
            123,
            "valid logs array",
        ),
        (
            {
                "signature": "invalid-logs",
                "err": None,
                "logs": "not-a-list",
            },
            123,
            "valid logs array",
        ),
        (
            {
                "err": None,
                "logs": ["Program data: example"],
            },
            123,
            "valid signature",
        ),
        (
            {
                "signature": True,
                "err": None,
                "logs": ["Program data: example"],
            },
            123,
            "valid signature",
        ),
        (
            {
                "signature": " padded ",
                "err": None,
                "logs": ["Program data: example"],
            },
            123,
            "valid signature",
        ),
        (
            {
                "signature": "invalid-slot",
                "err": None,
                "logs": ["Program data: example"],
            },
            True,
            "valid slot",
        ),
        (
            {
                "signature": "invalid-slot",
                "err": None,
                "logs": ["Program data: example"],
            },
            123.5,
            "valid slot",
        ),
    ],
)
async def test_malformed_logs_notification_is_rejected_before_dispatch(
    value: dict[str, Any],
    slot: object,
    message: str,
) -> None:
    gateway = _gateway()
    notification = {
        "jsonrpc": "2.0",
        "method": "logsNotification",
        "params": {
            "result": {
                "context": {"slot": slot},
                "value": value,
            }
        },
    }

    with pytest.raises(RuntimeError, match=message):
        await gateway.handle_message(notification)

    assert gateway._log_fetch_tasks == set()


@pytest.mark.asyncio
async def test_log_block_times_are_concurrent_but_dispatched_in_receive_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = HeliusStreamGateway(
        websocket_url="wss://example.invalid",
        rpc=SolanaRpcClient("https://example.invalid"),
        handler=_handler,
        entry_gate=EntryGate(BotMetrics()),
        metrics=BotMetrics(),
        log_fetch_concurrency=2,
    )
    first_release = asyncio.Event()
    second_fetched = asyncio.Event()
    dispatched: list[str] = []

    async def get_block_time(slot: int) -> int:
        if slot == 1:
            await first_release.wait()
        else:
            second_fetched.set()
        return 1_787_646_900 + slot

    async def dispatch(
        transaction: dict[str, Any],
        _source: EventSource,
        **_kwargs: Any,
    ) -> None:
        dispatched.append(str(transaction["signature"]))

    monkeypatch.setattr(gateway.rpc, "get_block_time", get_block_time)
    monkeypatch.setattr(gateway, "_queue_transaction", dispatch)
    def notification(signature: str, slot: int) -> dict[str, Any]:
        return {
            "jsonrpc": "2.0",
            "method": "logsNotification",
            "params": {
                "result": {
                    "context": {"slot": slot},
                    "value": {
                        "signature": signature,
                        "err": None,
                        "logs": [f"Program data: {signature}"],
                    },
                }
            },
        }

    await gateway.handle_message(notification("first", 1))
    await gateway.handle_message(notification("second", 2))
    await asyncio.wait_for(second_fetched.wait(), timeout=1)
    assert dispatched == []

    first_release.set()
    tasks = tuple(gateway._log_fetch_tasks)
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)
    assert dispatched == ["first", "second"]


@pytest.mark.asyncio
async def test_saturated_log_fetch_wait_aborts_for_reconnect() -> None:
    gateway = HeliusStreamGateway(
        websocket_url="wss://example.invalid",
        rpc=SolanaRpcClient("https://example.invalid"),
        handler=_handler,
        entry_gate=EntryGate(BotMetrics()),
        metrics=BotMetrics(),
        log_fetch_concurrency=1,
    )
    await gateway._log_fetch_semaphore.acquire()
    blocked = asyncio.create_task(
        gateway._spawn_ordered_log_dispatch(
            signature="blocked",
            slot=1,
            logs=["Program data: blocked"],
            received_at=datetime.now(tz=timezone.utc),
            generation=0,
        )
    )
    await asyncio.sleep(0)
    gateway._reconnect_requested.set()

    try:
        with pytest.raises(
            RuntimeError,
            match="ordered Solana transaction dispatch requested reconnect",
        ):
            await asyncio.wait_for(blocked, timeout=1)
    finally:
        gateway._log_fetch_semaphore.release()

    assert gateway._log_fetch_tasks == set()


@pytest.mark.asyncio
async def test_dispatch_failure_is_fail_closed_and_requests_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = HeliusStreamGateway(
        websocket_url="wss://example.invalid",
        rpc=SolanaRpcClient("https://example.invalid"),
        handler=_handler,
        entry_gate=EntryGate(BotMetrics()),
        metrics=BotMetrics(),
        log_fetch_concurrency=2,
    )
    first_dispatch_started = asyncio.Event()
    release_first_dispatch = asyncio.Event()
    dispatched: list[str] = []

    async def get_block_time(slot: int) -> int:
        return 1_787_646_900 + slot

    async def dispatch(
        transaction: dict[str, Any],
        _source: EventSource,
        **_kwargs: Any,
    ) -> None:
        signature = str(transaction["signature"])
        if signature == "first":
            first_dispatch_started.set()
            await release_first_dispatch.wait()
            raise RuntimeError("dispatch failed")
        dispatched.append(signature)

    def notification(signature: str, slot: int) -> dict[str, Any]:
        return {
            "jsonrpc": "2.0",
            "method": "logsNotification",
            "params": {
                "result": {
                    "context": {"slot": slot},
                    "value": {
                        "signature": signature,
                        "err": None,
                        "logs": [f"Program data: {signature}"],
                    },
                }
            },
        }

    monkeypatch.setattr(gateway.rpc, "get_block_time", get_block_time)
    monkeypatch.setattr(gateway, "_queue_transaction", dispatch)

    await gateway.handle_message(notification("first", 1))
    await gateway.handle_message(notification("second", 2))
    await asyncio.wait_for(first_dispatch_started.wait(), timeout=1)
    tasks = tuple(gateway._log_fetch_tasks)
    release_first_dispatch.set()
    results = await asyncio.wait_for(
        asyncio.gather(*tasks, return_exceptions=True),
        timeout=1,
    )

    assert all(isinstance(result, RuntimeError) for result in results)
    assert dispatched == []
    assert gateway._fetch_error_sequences
    assert gateway._reconnect_requested.is_set()
    assert gateway._dispatch_recovery_pending is True
    assert "stream_fetch_error" in gateway.entry_gate.reasons


def test_dispatch_recovery_gate_clears_only_after_new_baseline() -> None:
    gateway = _gateway()
    now = datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc)
    gateway._dispatch_recovery_pending = True
    gateway.entry_gate.block("stream_fetch_error")
    gateway._reconnect_requested.clear()
    gateway._baseline_started_at = now
    gateway.last_observed_at = now
    gateway.last_processed_block_time = now
    gateway.last_processing_lag_seconds = 0

    assert gateway.refresh_freshness(now) is False
    assert "stream_fetch_error" in gateway.entry_gate.reasons

    ready_at = now + gateway.LIVE_BASELINE_WARMUP
    gateway.last_observed_at = ready_at - timedelta(
        seconds=gateway.max_processing_lag_seconds + 1
    )
    gateway.last_processed_block_time = ready_at
    assert gateway.refresh_freshness(ready_at) is False
    assert gateway._dispatch_recovery_pending is True
    assert "stream_fetch_error" in gateway.entry_gate.reasons

    gateway.last_observed_at = ready_at
    assert gateway.refresh_freshness(ready_at) is True
    assert gateway._dispatch_recovery_pending is False
    assert "stream_fetch_error" not in gateway.entry_gate.reasons


def test_processing_lag_blocks_freshness_even_with_live_notifications() -> None:
    gateway = _gateway()
    now = datetime.now(tz=timezone.utc)
    gateway.last_observed_at = now
    gateway._baseline_started_at = now - gateway.LIVE_BASELINE_WARMUP
    gateway.last_processing_lag_seconds = gateway.max_processing_lag_seconds + 1

    assert gateway.refresh_freshness(now) is False
    assert "stream_stale" in gateway.entry_gate.reasons

    gateway.last_processing_lag_seconds = gateway.max_processing_lag_seconds - 1
    assert gateway.refresh_freshness(now) is True
    assert "stream_stale" not in gateway.entry_gate.reasons


def test_processing_freshness_age_advances_with_injected_clock() -> None:
    gateway = _gateway()
    now = datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc)
    gateway._baseline_started_at = now - gateway.LIVE_BASELINE_WARMUP
    gateway.last_observed_at = now
    gateway.last_processed_block_time = now - timedelta(seconds=1)
    gateway.last_processing_lag_seconds = 1

    assert gateway.refresh_freshness(now) is True

    advanced = now + timedelta(
        seconds=gateway.max_processing_lag_seconds + 1
    )
    gateway.last_observed_at = advanced

    assert gateway.refresh_freshness(advanced) is False
    assert "stream_stale" in gateway.entry_gate.reasons
