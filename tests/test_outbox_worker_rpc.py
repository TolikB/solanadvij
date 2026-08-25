from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from sniper_bot.database import Database
from sniper_bot.db_models import OutboxEventRow
from sniper_bot.metrics import BotMetrics
from sniper_bot.outbox import TelegramOutboxWorker
from sniper_bot.solana_rpc import SolanaRpcClient, SolanaRpcError


class _BatchResponse:
    status_code = 200

    def __init__(self, payload: list[dict[str, Any]]) -> None:
        self._payload = payload

    def json(self) -> list[dict[str, Any]]:
        return self._payload


class _BatchHttpClient:
    posts: list[list[dict[str, Any]]] = []

    def __init__(self, *, timeout: float) -> None:
        self.timeout = timeout

    async def __aenter__(self) -> _BatchHttpClient:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def post(
        self,
        _endpoint: str,
        *,
        json: list[dict[str, Any]],
    ) -> _BatchResponse:
        self.posts.append(json)
        rows = [
            {
                "jsonrpc": "2.0",
                "id": item["id"],
                "result": {"signature": item["params"][0]},
            }
            for item in reversed(json)
        ]
        return _BatchResponse(rows)


class _NonJsonResponse:
    status_code = 429

    def json(self) -> object:
        raise AssertionError("429 response body must not be decoded")


class _RetryBatchHttpClient(_BatchHttpClient):
    calls = 0

    async def post(
        self,
        endpoint: str,
        *,
        json: list[dict[str, Any]],
    ) -> _BatchResponse | _NonJsonResponse:
        type(self).calls += 1
        if type(self).calls == 1:
            return _NonJsonResponse()
        return await super().post(endpoint, json=json)


class _ArbitraryBatchResponse:
    status_code = 200

    def __init__(self, payload: object) -> None:
        self._payload = payload

    def json(self) -> object:
        return self._payload


class _ArbitraryBatchHttpClient(_BatchHttpClient):
    payload: object = None

    async def post(
        self,
        _endpoint: str,
        *,
        json: list[dict[str, Any]],
    ) -> _ArbitraryBatchResponse:
        self.posts.append(json)
        return _ArbitraryBatchResponse(self.payload)


class _BlockingEnterBatchHttpClient(_BatchHttpClient):
    enter_started = asyncio.Event()
    release_enter = asyncio.Event()

    async def __aenter__(self) -> _BlockingEnterBatchHttpClient:
        self.enter_started.set()
        await self.release_enter.wait()
        return self


class _ClassifyingNotifier:
    async def send_immediate(self, text: str, *, chat_id: int | None = None) -> str:
        if text == "retry":
            raise RuntimeError("telegram rejected request")
        if text == "permanent":
            raise PermissionError("chat is not allowlisted")
        if text == "uncertain":
            raise OSError("connection closed after write")
        return "message-42"


@pytest.mark.asyncio
async def test_outbox_worker_classifies_delivery_outcomes(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'outbox-worker.db'}")
    await database.create_schema_for_tests()
    payloads = {
        "delivered": {"text": "ok"},
        "retry": {"text": "retry"},
        "permanent": {"text": "permanent"},
        "uncertain": {"text": "uncertain"},
        "invalid": {"text": "  "},
    }
    for key, payload in payloads.items():
        assert await database.enqueue_outbox(
            idempotency_key=key,
            event_type="test",
            payload=payload,
        )

    worker = TelegramOutboxWorker(
        database,
        _ClassifyingNotifier(),
        metrics=BotMetrics(),
        poll_seconds=0.01,
        allowed_event_types=frozenset({"test"}),
    )
    assert await worker.deliver_once() == 1

    async with database.sessions() as session:
        rows = list((await session.scalars(select(OutboxEventRow))).all())
    states = {row.idempotency_key: row.delivery_state for row in rows}
    assert states == {
        "delivered": "DELIVERED",
        "retry": "FAILED",
        "permanent": "DEAD",
        "uncertain": "UNCERTAIN",
        "invalid": "DEAD",
    }
    delivered = next(row for row in rows if row.idempotency_key == "delivered")
    assert delivered.telegram_message_id == "message-42"
    await database.close()


@pytest.mark.asyncio
async def test_outbox_worker_suppresses_disallowed_event_types(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'outbox-policy.db'}")
    await database.create_schema_for_tests()
    assert await database.enqueue_outbox(
        idempotency_key="blocked-risk",
        event_type="risk_alert",
        payload={"text": "risk"},
    )
    assert await database.enqueue_outbox(
        idempotency_key="allowed-daily",
        event_type="daily_report",
        payload={"text": "daily"},
    )
    notifier = AsyncMock()
    notifier.send_immediate.return_value = "daily-message"
    worker = TelegramOutboxWorker(
        database,
        notifier,
        metrics=BotMetrics(),
        allowed_event_types=frozenset({"daily_report"}),
    )

    assert await worker.deliver_once() == 1
    notifier.send_immediate.assert_awaited_once_with("daily", chat_id=None)
    async with database.sessions() as session:
        rows = list((await session.scalars(select(OutboxEventRow))).all())
    states = {row.idempotency_key: row.delivery_state for row in rows}
    assert states == {
        "blocked-risk": "DEAD",
        "allowed-daily": "DELIVERED",
    }
    await database.close()


@pytest.mark.asyncio
async def test_outbox_worker_drain_delivers_before_shutdown(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'outbox-drain.db'}")
    await database.create_schema_for_tests()
    assert await database.enqueue_outbox(
        idempotency_key="shutdown-alert",
        event_type="system_stop",
        payload={"text": "sniper bot stopped"},
    )
    worker = TelegramOutboxWorker(
        database,
        _ClassifyingNotifier(),
        metrics=BotMetrics(),
        poll_seconds=60.0,
    )

    await worker.start()
    assert await worker.drain(timeout_seconds=1.0) is True
    assert await database.deliverable_outbox_count() == 0
    async with database.sessions() as session:
        event = await session.scalar(
            select(OutboxEventRow).where(
                OutboxEventRow.idempotency_key == "shutdown-alert"
            )
        )
    assert event is not None
    assert event.delivery_state == "DELIVERED"
    assert event.telegram_message_id == "message-42"
    await worker.stop()
    await database.close()


@pytest.mark.asyncio
async def test_outbox_worker_drain_fails_safe_on_database_error(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'outbox-drain-error.db'}")
    await database.create_schema_for_tests()
    database.deliverable_outbox_count = AsyncMock(
        side_effect=OSError("database unavailable")
    )
    worker = TelegramOutboxWorker(
        database,
        _ClassifyingNotifier(),
        metrics=BotMetrics(),
    )

    assert await worker.drain(timeout_seconds=1.0) is False
    await worker.stop()
    await database.close()


@pytest.mark.asyncio
async def test_solana_rpc_batches_concurrent_transaction_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _BatchHttpClient.posts = []
    monkeypatch.setattr(
        "sniper_bot.solana_rpc.httpx.AsyncClient",
        _BatchHttpClient,
    )
    client = SolanaRpcClient(
        "https://rpc.invalid",
        transaction_batch_size=20,
        transaction_batch_window_seconds=0.001,
    )
    pacing = AsyncMock(wraps=client._acquire_transaction_batch_capacity)
    monkeypatch.setattr(client, "_acquire_transaction_batch_capacity", pacing)

    signatures = [f"SIGNATURE_{index}" for index in range(20)]
    results = await asyncio.gather(
        *(client.get_transaction(signature) for signature in signatures)
    )

    assert len(_BatchHttpClient.posts) == 1
    assert len(_BatchHttpClient.posts[0]) == 20
    assert [result["signature"] if result else None for result in results] == signatures
    pacing.assert_awaited_once()
    assert len(pacing.await_args.args[0]) == 20


@pytest.mark.asyncio
async def test_solana_rpc_paces_requests_by_batch_weight() -> None:
    client = SolanaRpcClient(
        "https://rpc.invalid",
        rpc_requests_per_second=8,
    )
    clock = [100.0]
    delays: list[float] = []

    async def advance(delay: float) -> None:
        delays.append(delay)
        clock[0] += delay

    client._monotonic = lambda: clock[0]
    client._sleep = advance

    await client._acquire_rpc_capacity(20)
    await client._acquire_rpc_capacity(1)

    assert delays == [pytest.approx(2.5)]
    assert client._next_rpc_send_at == pytest.approx(102.625)


@pytest.mark.asyncio
async def test_solana_rpc_does_not_reserve_cancelled_batch_capacity() -> None:
    client = SolanaRpcClient(
        "https://rpc.invalid",
        rpc_requests_per_second=8,
    )
    clock = [100.0]
    pacing_started = asyncio.Event()
    release_pacing = asyncio.Event()
    client._next_rpc_send_at = 101.0

    async def advance(delay: float) -> None:
        pacing_started.set()
        await release_pacing.wait()
        clock[0] += delay

    client._monotonic = lambda: clock[0]
    client._sleep = advance
    loop = asyncio.get_running_loop()
    cancelled = loop.create_future()
    retained = loop.create_future()
    batch = [({"id": 1}, cancelled), ({"id": 2}, retained)]
    pacing = asyncio.create_task(
        client._acquire_transaction_batch_capacity(batch)
    )
    await pacing_started.wait()
    cancelled.cancel()
    release_pacing.set()

    active = await pacing

    assert active == [({"id": 2}, retained)]
    assert client._next_rpc_send_at == pytest.approx(101.125)
    retained.cancel()


@pytest.mark.parametrize("rate", [0.0, -1.0, float("nan"), float("inf")])
def test_solana_rpc_rejects_invalid_request_rate(rate: float) -> None:
    with pytest.raises(
        ValueError,
        match="rpc_requests_per_second must be greater than zero",
    ):
        SolanaRpcClient(
            "https://rpc.invalid",
            rpc_requests_per_second=rate,
        )


@pytest.mark.asyncio
async def test_solana_rpc_retries_non_json_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _BatchHttpClient.posts = []
    _RetryBatchHttpClient.calls = 0
    monkeypatch.setattr(
        "sniper_bot.solana_rpc.httpx.AsyncClient",
        _RetryBatchHttpClient,
    )
    client = SolanaRpcClient(
        "https://rpc.invalid",
        max_retries=1,
        transaction_batch_window_seconds=0.001,
    )

    result = await client.get_transaction("SIGNATURE")

    assert result == {"signature": "SIGNATURE"}
    assert _RetryBatchHttpClient.calls == 2


@pytest.mark.asyncio
async def test_solana_rpc_get_block_time_validates_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = SolanaRpcClient("https://rpc.invalid")
    calls: list[tuple[str, list[object]]] = []

    async def call(method: str, params: list[object]) -> object:
        calls.append((method, params))
        return 1_787_646_900

    monkeypatch.setattr(client, "_call", call)

    assert await client.get_block_time(123) == 1_787_646_900
    assert calls == [("getBlockTime", [123])]
    with pytest.raises(ValueError, match="slot must not be negative"):
        await client.get_block_time(-1)

    async def invalid_call(_method: str, _params: list[object]) -> object:
        return "invalid"

    monkeypatch.setattr(client, "_call", invalid_call)
    with pytest.raises(SolanaRpcError, match="invalid result"):
        await client.get_block_time(123)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "result": {"signature": "WRONG_SHARED_RESULT"},
        },
        [
            {"jsonrpc": "2.0", "id": 1, "result": {"signature": "A"}},
            {"jsonrpc": "2.0", "id": 1, "result": {"signature": "B"}},
        ],
    ],
)
async def test_solana_rpc_rejects_invalid_batch_response_ids(
    monkeypatch: pytest.MonkeyPatch,
    payload: object,
) -> None:
    _ArbitraryBatchHttpClient.posts = []
    _ArbitraryBatchHttpClient.payload = payload
    monkeypatch.setattr(
        "sniper_bot.solana_rpc.httpx.AsyncClient",
        _ArbitraryBatchHttpClient,
    )
    client = SolanaRpcClient(
        "https://rpc.invalid",
        transaction_batch_window_seconds=0.001,
    )

    results = await asyncio.gather(
        client.get_transaction("A"),
        client.get_transaction("B"),
        return_exceptions=True,
    )

    assert all(isinstance(result, SolanaRpcError) for result in results)
    assert len(_ArbitraryBatchHttpClient.posts) == 1


@pytest.mark.asyncio
async def test_solana_rpc_does_not_send_cancelled_batch_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _BatchHttpClient.posts = []
    monkeypatch.setattr(
        "sniper_bot.solana_rpc.httpx.AsyncClient",
        _BatchHttpClient,
    )
    client = SolanaRpcClient(
        "https://rpc.invalid",
        transaction_batch_window_seconds=0.01,
    )
    request = asyncio.create_task(client.get_transaction("CANCELLED"))
    await asyncio.sleep(0)
    request.cancel()

    with pytest.raises(asyncio.CancelledError):
        await request
    await asyncio.sleep(0.02)

    assert _BatchHttpClient.posts == []
    assert client._transaction_batch_flush_task is None


@pytest.mark.asyncio
async def test_solana_rpc_filters_cancellation_during_http_enter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _BlockingEnterBatchHttpClient.posts = []
    _BlockingEnterBatchHttpClient.enter_started = asyncio.Event()
    _BlockingEnterBatchHttpClient.release_enter = asyncio.Event()
    monkeypatch.setattr(
        "sniper_bot.solana_rpc.httpx.AsyncClient",
        _BlockingEnterBatchHttpClient,
    )
    client = SolanaRpcClient(
        "https://rpc.invalid",
        transaction_batch_window_seconds=0.001,
    )
    client._monotonic = lambda: 100.0
    cancelled = asyncio.create_task(client.get_transaction("CANCELLED"))
    retained = asyncio.create_task(client.get_transaction("RETAINED"))
    await asyncio.wait_for(
        _BlockingEnterBatchHttpClient.enter_started.wait(),
        timeout=1,
    )
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    _BlockingEnterBatchHttpClient.release_enter.set()

    result = await asyncio.wait_for(retained, timeout=1)

    assert result == {"signature": "RETAINED"}
    sent = _BlockingEnterBatchHttpClient.posts[0]
    assert [item["params"][0] for item in sent] == ["RETAINED"]
    assert client._next_rpc_send_at == pytest.approx(100.25)


@pytest.mark.asyncio
async def test_solana_rpc_worker_cancellation_settles_pending_and_allows_reuse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _BatchHttpClient.posts = []
    monkeypatch.setattr(
        "sniper_bot.solana_rpc.httpx.AsyncClient",
        _BatchHttpClient,
    )
    client = SolanaRpcClient(
        "https://rpc.invalid",
        transaction_batch_window_seconds=60,
    )
    pending = asyncio.create_task(client.get_transaction("PENDING"))
    await asyncio.sleep(0)
    worker = client._transaction_batch_flush_task
    assert worker is not None
    worker.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(pending, timeout=1)
    with pytest.raises(asyncio.CancelledError):
        await worker
    assert client._transaction_batch_flush_task is None

    client.transaction_batch_window_seconds = 0.001
    result = await asyncio.wait_for(
        client.get_transaction("AFTER_CANCEL"),
        timeout=1,
    )
    assert result == {"signature": "AFTER_CANCEL"}


@pytest.mark.asyncio
async def test_solana_rpc_paginates_complete_fresh_holder_universe() -> None:
    client = SolanaRpcClient("https://rpc.invalid")
    client._call = AsyncMock(
        side_effect=[
            {
                "last_indexed_slot": 100,
                "cursor": "page-2",
                "token_accounts": [
                    {"address": "TOKEN_ACCOUNT_A", "owner": "OWNER_A", "amount": 50},
                ],
            },
            {
                "last_indexed_slot": 101,
                "cursor": None,
                "token_accounts": [
                    {"address": "TOKEN_ACCOUNT_B", "owner": "OWNER_B", "amount": "25"},
                ],
            },
            110,
            {
                "value": [
                    {
                        "account": {
                            "data": {
                                "parsed": {
                                    "info": {"tokenAmount": {"amount": "70"}}
                                }
                            }
                        }
                    },
                    {
                        "account": {
                            "data": {
                                "parsed": {
                                    "info": {"tokenAmount": {"amount": "5"}}
                                }
                            }
                        }
                    },
                ]
            },
        ]
    )

    holders = await client.get_all_holders(
        "MINT", expected_supply_raw=Decimal("75")
    )
    dev_balance = await client.get_owner_token_balance("DEV", "MINT")

    assert [(row.token_account, row.owner, row.amount_raw) for row in holders] == [
        ("TOKEN_ACCOUNT_A", "OWNER_A", Decimal("50")),
        ("TOKEN_ACCOUNT_B", "OWNER_B", Decimal("25")),
    ]
    assert dev_balance == Decimal("75")


@pytest.mark.asyncio
async def test_solana_rpc_rejects_stale_holder_index() -> None:
    client = SolanaRpcClient("https://rpc.invalid")
    client._call = AsyncMock(
        side_effect=[
            {"last_indexed_slot": 100, "cursor": None, "token_accounts": []},
            121,
        ]
    )
    with pytest.raises(RuntimeError, match="too stale"):
        await client.get_all_holders("MINT", expected_supply_raw=Decimal("0"))


@pytest.mark.asyncio
async def test_solana_rpc_rejects_incomplete_holder_supply() -> None:
    client = SolanaRpcClient("https://rpc.invalid")
    client._call = AsyncMock(
        side_effect=[
            {
                "last_indexed_slot": 100,
                "cursor": None,
                "token_accounts": [
                    {"address": "A", "owner": "OWNER", "amount": "99"}
                ],
            },
            100,
        ]
    )
    with pytest.raises(RuntimeError, match="does not match"):
        await client.get_all_holders("MINT", expected_supply_raw=Decimal("100"))
