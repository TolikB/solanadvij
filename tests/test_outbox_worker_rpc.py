from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from sniper_bot.database import Database
from sniper_bot.db_models import OutboxEventRow
from sniper_bot.metrics import BotMetrics
from sniper_bot.outbox import TelegramOutboxWorker
from sniper_bot.solana_rpc import SolanaRpcClient


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
async def test_outbox_worker_drain_delivers_before_shutdown(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'outbox-drain.db'}")
    await database.create_schema_for_tests()
    assert await database.enqueue_outbox(
        idempotency_key="shutdown-alert",
        event_type="system_alert",
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
