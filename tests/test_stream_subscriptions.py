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
    assert all(item["params"][1]["maxSupportedTransactionVersion"] == 1 for item in websocket.sent)


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
    connect_options: list[dict[str, object]] = []

    def connect(*_args: object, **kwargs: object) -> FakeConnectionContext:
        connect_options.append(dict(kwargs))
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
        assert connect_options
        assert all(
            options["ping_timeout"]
            == gateway.WEBSOCKET_PING_TIMEOUT_SECONDS
            for options in connect_options
        )
        assert all(
            options["ping_interval"]
            == gateway.WEBSOCKET_PING_INTERVAL_SECONDS
            for options in connect_options
        )
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
async def test_gap_recovery_timeout_reconnects_without_partial_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    abandoned_notification = {
        "jsonrpc": "2.0",
        "method": "transactionNotification",
        "params": {"generation": "abandoned"},
    }
    fresh_notification = {
        "jsonrpc": "2.0",
        "method": "transactionNotification",
        "params": {"generation": "fresh"},
    }
    recovery_connection = FakeConnection(
        [
            {"jsonrpc": "2.0", "id": 1, "result": 11},
            {"jsonrpc": "2.0", "id": 2, "result": 12},
            abandoned_notification,
        ],
        finish_iteration=False,
    )
    fresh_connection = FakeConnection(
        [
            {"jsonrpc": "2.0", "id": 1, "result": 21},
            {"jsonrpc": "2.0", "id": 2, "result": 22},
            fresh_notification,
        ],
        finish_iteration=False,
    )
    connections = [recovery_connection, fresh_connection]
    events: list[str] = []

    def connect(*_args: object, **_kwargs: object) -> FakeConnectionContext:
        connection = connections.pop(0)
        name = "recovery" if connection is recovery_connection else "fresh"
        return FakeConnectionContext(connection, events, name)

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
    recovery_calls = 0

    async def slow_recovery() -> list[tuple[Protocol, dict[str, Any], object]]:
        nonlocal recovery_calls
        recovery_calls += 1
        if recovery_calls == 1:
            try:
                await asyncio.sleep(60)
            finally:
                recovery_cancelled.set()
        return []

    async def handle_message(message: dict[str, Any]) -> None:
        processed.append(message)
        if message == fresh_notification:
            live_processed.set()

    monkeypatch.setattr("sniper_bot.stream.websockets.connect", connect)
    monkeypatch.setattr(gateway, "_recover_gap", slow_recovery)
    monkeypatch.setattr(gateway, "handle_message", handle_message)
    run_task = asyncio.create_task(gateway._run())
    try:
        async with asyncio.timeout(1):
            await live_processed.wait()
            await recovery_cancelled.wait()

        assert processed == [fresh_notification]
        assert events[:3] == ["open:recovery", "close:recovery", "open:fresh"]
        assert gateway._queue.empty()
        assert gateway.last_slot == 123
        assert gateway.last_signature == "recent-signature"
        assert gateway._last_signatures == {
            Protocol.PUMP: "recent-signature"
        }
        assert recovery_calls >= 2
        assert "startup" not in gateway.entry_gate.reasons
        assert "stream_disconnected" not in gateway.entry_gate.reasons
        assert "stream_recovery_gap" not in gateway.entry_gate.reasons
        assert "stream_baseline" in gateway.entry_gate.reasons
        assert "stream_stale" in gateway.entry_gate.reasons
    finally:
        run_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run_task


@pytest.mark.asyncio
async def test_unrecoverable_checkpoint_holds_without_touching_the_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = _gateway()
    gateway.UNRECOVERABLE_GAP_HOLD_SECONDS = 0.05
    gateway.halt_on_unrecoverable_gap = True
    gateway.restore_checkpoint(
        123,
        "old-signature",
        datetime.now(tz=timezone.utc) - timedelta(minutes=5),
    )
    gateway.restore_protocol_checkpoint(Protocol.PUMP, "old-signature")
    gap_reasons: list[str] = []
    recorded = asyncio.Event()

    def unexpected_connect(*_args: object, **_kwargs: object) -> object:
        pytest.fail("an unrecoverable gap must not open a provider socket")

    async def unexpected_recovery() -> None:
        pytest.fail("an unrecoverable gap must not paginate the provider")

    async def record_gap(reason: str) -> None:
        gap_reasons.append(reason)
        recorded.set()

    def unexpected_discard() -> None:
        pytest.fail("an unrecoverable gap must not discard the checkpoint")

    monkeypatch.setattr("sniper_bot.stream.websockets.connect", unexpected_connect)
    monkeypatch.setattr(gateway, "_recover_gap", unexpected_recovery)
    monkeypatch.setattr(gateway, "_discard_in_memory_checkpoint", unexpected_discard)
    gateway.gap_handler = record_gap

    run_task = asyncio.create_task(gateway._run())
    try:
        async with asyncio.timeout(1):
            await recorded.wait()
            await asyncio.sleep(0.2)

        # The reason is recorded once, not on every hold cycle.
        assert gap_reasons == ["checkpoint_unrecoverable"]
        assert gateway.last_slot == 123
        assert gateway.last_signature == "old-signature"
        assert "stream_recovery_gap" in gateway.entry_gate.reasons
    finally:
        run_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run_task


@pytest.mark.asyncio
async def test_unrecoverable_gap_is_recorded_then_baseline_restarts(
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
    gateway.restore_protocol_checkpoint(
        Protocol.PUMP,
        "old-signature",
    )
    gap_reasons: list[str] = []

    async def unexpected_recovery() -> None:
        pytest.fail("an unrecoverable gap must not trigger historical gap recovery")

    async def record_gap(reason: str) -> None:
        gap_reasons.append(reason)

    async def unexpected_resolve() -> None:
        pytest.fail("an accepted archive hole must stay recorded")

    checkpoint_discarded = asyncio.Event()
    discard_checkpoint = gateway._discard_in_memory_checkpoint

    def discard_and_notify() -> None:
        discard_checkpoint()
        checkpoint_discarded.set()

    monkeypatch.setattr(
        "sniper_bot.stream.websockets.connect",
        connect,
    )
    monkeypatch.setattr(
        gateway,
        "_recover_gap",
        unexpected_recovery,
    )
    monkeypatch.setattr(
        gateway,
        "_discard_in_memory_checkpoint",
        discard_and_notify,
    )
    gateway.gap_handler = record_gap
    gateway.gap_resolved_handler = unexpected_resolve
    recovery_gap_unblocked = asyncio.Event()
    unblock = gateway.entry_gate.unblock

    def unblock_and_notify(reason: str) -> None:
        unblock(reason)
        if reason == "stream_recovery_gap":
            recovery_gap_unblocked.set()

    monkeypatch.setattr(gateway.entry_gate, "unblock", unblock_and_notify)
    run_task = asyncio.create_task(gateway._run())
    try:
        async with asyncio.timeout(1):
            await checkpoint_discarded.wait()
            await recovery_gap_unblocked.wait()

        assert gap_reasons == ["unrecoverable_gap_accepted"]
        assert gateway.last_slot == 0
        assert gateway.last_signature is None
        assert gateway.last_observed_at is None
        assert gateway._last_signatures == {}
        assert "stream_recovery_gap" not in gateway.entry_gate.reasons
        assert "stream_baseline" in gateway.entry_gate.reasons
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


def _logs_notification(signature: str, slot: int) -> dict[str, Any]:
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


@pytest.mark.asyncio
async def test_logs_notification_dispatches_in_order_without_rpc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = _gateway()
    queued: list[tuple[dict[str, Any], EventSource, dict[str, Any]]] = []

    async def unexpected_rpc(*_args: Any) -> Any:
        pytest.fail("logsNotification dispatch must not call Solana RPC")

    async def queue_transaction(
        transaction: dict[str, Any],
        source: EventSource,
        **kwargs: Any,
    ) -> None:
        queued.append((transaction, source, kwargs))

    monkeypatch.setattr(gateway.rpc, "get_block_time", unexpected_rpc)
    monkeypatch.setattr(gateway.rpc, "get_transaction", unexpected_rpc)
    monkeypatch.setattr(gateway, "_queue_transaction", queue_transaction)

    for signature, slot in (("first", 1), ("second", 1), ("third", 2), ("fourth", 3)):
        await gateway.handle_message(_logs_notification(signature, slot))
    await asyncio.wait_for(gateway._notification_queue.join(), timeout=1)

    assert [row[0]["signature"] for row in queued] == [
        "first",
        "second",
        "third",
        "fourth",
    ]
    assert [row[0]["slot"] for row in queued] == [1, 1, 2, 3]
    assert "blockTime" not in queued[0][0]
    assert queued[0][0]["meta"] == {"err": None, "logMessages": ["Program data: first"]}
    assert all(row[1] == EventSource.SOLANA_WSS for row in queued)
    assert all(row[2]["generation"] == gateway._stream_generation for row in queued)


@pytest.mark.asyncio
async def test_helius_transaction_notification_takes_signature_and_slot_from_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = _gateway()
    queued: list[tuple[dict[str, Any], EventSource]] = []

    async def queue_transaction(
        transaction: dict[str, Any],
        source: EventSource,
        **_kwargs: Any,
    ) -> None:
        queued.append((transaction, source))

    monkeypatch.setattr(gateway, "_queue_transaction", queue_transaction)
    logs = ["Program pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA invoke [1]"]
    await gateway.handle_message(
        {
            "jsonrpc": "2.0",
            "method": "transactionNotification",
            "params": {
                "subscription": 11,
                "result": {
                    "transaction": {
                        "transaction": ["AQID", "base64"],
                        "meta": {"err": None, "logMessages": logs},
                    },
                    "signature": "helius-signature",
                    "slot": 412_345_678,
                    "transactionIndex": 7,
                },
            },
        }
    )
    await asyncio.wait_for(gateway._notification_queue.join(), timeout=1)

    assert len(queued) == 1
    transaction, source = queued[0]
    assert source == EventSource.HELIUS_WSS
    assert transaction["signature"] == "helius-signature"
    assert transaction["slot"] == 412_345_678
    assert transaction["meta"]["logMessages"] == logs


@pytest.mark.asyncio
async def test_transaction_subscription_requests_compact_encoding() -> None:
    websocket = FakeWebSocket(
        [
            {"jsonrpc": "2.0", "id": 1, "result": 11},
            {"jsonrpc": "2.0", "id": 2, "result": 12},
        ]
    )

    await _gateway()._subscribe(websocket)

    assert [item["params"][1]["encoding"] for item in websocket.sent] == [
        "base64",
        "base64",
    ]
    assert all(
        item["params"][1]["transactionDetails"] == "full" for item in websocket.sent
    )


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

    assert gateway._notification_queue.empty()


@pytest.mark.asyncio
async def test_notification_ingress_is_bounded_and_overflow_is_fatal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fatal_errors: list[BaseException] = []
    gateway = HeliusStreamGateway(
        websocket_url="wss://example.invalid",
        rpc=SolanaRpcClient("https://example.invalid"),
        handler=_handler,
        entry_gate=EntryGate(BotMetrics()),
        metrics=BotMetrics(),
        fatal_handler=fatal_errors.append,
        notification_queue_size=2,
    )
    dispatch_started = asyncio.Event()
    release_dispatch = asyncio.Event()

    async def blocked_queue_transaction(
        _transaction: dict[str, Any],
        _source: EventSource,
        **_kwargs: Any,
    ) -> None:
        dispatch_started.set()
        await release_dispatch.wait()

    monkeypatch.setattr(gateway, "_queue_transaction", blocked_queue_transaction)
    try:
        await gateway.handle_message(_logs_notification("active", 1))
        await asyncio.wait_for(dispatch_started.wait(), timeout=1)
        assert gateway._notification_queue.empty()

        await gateway.handle_message(_logs_notification("queued-1", 1))
        await gateway.handle_message(_logs_notification("queued-2", 1))
        assert gateway._notification_queue.full()

        with pytest.raises(
            RuntimeError,
            match="notification ingress queue overflow",
        ):
            await gateway.handle_message(_logs_notification("overflow", 1))

        assert len(fatal_errors) == 1
        assert gateway._reconnect_requested.is_set()
        assert "stream_fetch_error" in gateway.entry_gate.reasons
    finally:
        await gateway._cancel_ingress_dispatch()

    assert gateway._dispatch_recovery_pending is True
    assert "stream_recovery_gap" in gateway.entry_gate.reasons


@pytest.mark.asyncio
async def test_dispatch_failure_is_fail_closed_and_requests_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = _gateway()
    dispatched: list[str] = []

    async def dispatch(
        transaction: dict[str, Any],
        _source: EventSource,
        **_kwargs: Any,
    ) -> None:
        signature = str(transaction["signature"])
        if signature == "first":
            raise RuntimeError("dispatch failed")
        dispatched.append(signature)

    monkeypatch.setattr(gateway, "_queue_transaction", dispatch)

    await gateway.handle_message(_logs_notification("first", 1))
    dispatcher = gateway._notification_dispatch_task
    assert dispatcher is not None
    with pytest.raises(RuntimeError, match="dispatch failed"):
        await asyncio.wait_for(dispatcher, timeout=1)
    with pytest.raises(RuntimeError, match="requested reconnect"):
        await gateway.handle_message(_logs_notification("second", 2))

    assert dispatched == []
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
