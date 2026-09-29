from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from sniper_bot.config import AppConfig
from sniper_bot.database import Database
from sniper_bot.db_models import ExternalApiCallRow, PaperOrderRow
from sniper_bot.events import ChainEventType, EventEnvelope, EventSource, Protocol
from sniper_bot.metrics import BotMetrics
from sniper_bot.models import QuoteResponse
from sniper_bot.pipeline import CANDIDATE_PERSIST_INTERVAL, ConfirmationPipeline
from sniper_bot.registry import WSOL_MINT
from sniper_bot.runtime import EQUITY_MARK_OPEN_INTERVAL, SniperRuntime
from sniper_bot.stream import EntryGate

NOW = datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)


def _config() -> AppConfig:
    return AppConfig(
        APP_MODE="paper",
        APP_REVISION="",
        HELIUS_API_KEY="helius-key",
        JUPITER_API_KEY="jupiter-key",
        POSTGRES_DSN="postgresql://user:pass@localhost:5432/db",
        TELEGRAM_BOT_TOKEN="telegram-token",
        TELEGRAM_ADMIN_CHAT_ID=123456,
        STARTING_EQUITY_USD=Decimal("500"),
    )


def _pool_created(at: datetime) -> EventEnvelope:
    return EventEnvelope(
        source=EventSource.HELIUS_WSS,
        protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.POOL_CREATED,
        slot=1,
        signature="create-pool",
        instruction_index=1,
        block_time=at,
        observed_at=at,
        mint="MINT",
        pool_address="POOL",
        payload={
            "base_mint": "MINT",
            "quote_mint": WSOL_MINT,
            "base_mint_decimals": 6,
            "quote_mint_decimals": 9,
            "pool_base_amount": 1_000_000_000_000,
            "pool_quote_amount": 300_000_000_000,
        },
    )


@pytest.mark.asyncio
async def test_candidate_rows_are_rewritten_on_change_not_every_second(
    tmp_path: Path,
) -> None:
    metrics = BotMetrics()
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="strategy-v1",
        config_hash="config-hash",
        entry_gate=EntryGate(metrics),
        metrics=metrics,
        record_raw=False,
        config=_config(),
    )
    await pipeline.process_event(_pool_created(NOW))
    database = AsyncMock()
    pipeline.database = database

    for second in range(1, 41):
        await pipeline.evaluate_candidates(NOW + timedelta(seconds=second))

    # DISCOVERED -> COLLECTING at 1 s, then the per-second price memory only
    # reaches the database every CANDIDATE_PERSIST_INTERVAL.
    writes = database.upsert_candidate.await_count
    interval = int(CANDIDATE_PERSIST_INTERVAL.total_seconds())
    assert writes == 1 + 40 // interval
    assert database.record_snapshot.await_count == 40


class _FlatQuotes:
    async def get_buy_quote(
        self, quote_token: str, token: str, usdc_amount: Decimal
    ) -> QuoteResponse:
        return self._quote(usdc_amount)

    async def get_sell_quote(
        self, token: str, quote_token: str, token_amount: Decimal
    ) -> QuoteResponse:
        return self._quote(token_amount)

    async def get_sell_quote_mark_to_market(
        self, token: str, quote_token: str, token_amount: Decimal
    ) -> QuoteResponse:
        return self._quote(token_amount)

    @staticmethod
    def _quote(amount: Decimal) -> QuoteResponse:
        return QuoteResponse(
            token_in="A",
            token_out="B",
            in_amount=amount,
            out_amount=amount,
            in_amount_usd=amount,
            out_amount_usd=amount,
            route={},
            price_impact_pct=Decimal("0"),
        )


@pytest.mark.asyncio
async def test_open_positions_mark_the_durable_path_every_few_seconds(
    tmp_path: Path,
) -> None:
    runtime = SniperRuntime(_config(), data_dir=tmp_path)
    provider = _FlatQuotes()
    runtime.quote_provider = provider  # type: ignore[assignment]
    assert runtime.broker is not None
    runtime.broker._quote_provider = provider  # type: ignore[assignment]
    runtime.broker._execution_delay_seconds = 0
    await runtime.broker.open("TOKEN", Decimal("10"))
    database = AsyncMock()
    runtime.database = database
    runtime.database_available = True

    started = datetime.now(tz=timezone.utc)
    for second in range(13):
        await runtime.evaluate_and_close_exits(now=started + timedelta(seconds=second))

    marked = [
        call.kwargs["observed_at"] for call in database.update_paper_marks.await_args_list
    ]
    step = EQUITY_MARK_OPEN_INTERVAL
    assert marked == [started, started + step, started + 2 * step]


@pytest.mark.asyncio
async def test_retention_drops_old_provider_calls_but_keeps_fill_provenance(
    tmp_path: Path,
) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'retention.db'}")
    await database.create_schema_for_tests()
    now = datetime.now(tz=timezone.utc)
    async with database.sessions.begin() as session:
        for row_id, age in (("old", 10), ("old-fill", 10), ("recent", 1)):
            session.add(
                ExternalApiCallRow(
                    id=row_id,
                    provider="jupiter",
                    endpoint="/order",
                    request_hash=row_id,
                    requested_at=now - timedelta(days=age),
                    request_json={},
                    response_json={"big": "payload"},
                )
            )
        session.add(
            PaperOrderRow(
                id="order",
                idempotency_key="order",
                side="BUY",
                status="filled",
                quote_request_id="old-fill",
                created_at=now - timedelta(days=10),
            )
        )

    await database.run_retention(raw_retention_days=90, api_call_retention_days=3)

    async with database.sessions() as session:
        remaining = set((await session.scalars(select(ExternalApiCallRow.id))).all())
    assert remaining == {"old-fill", "recent"}
    await database.close()
