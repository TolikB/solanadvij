"""Security check, entry and exit through the real provider clients.

Everything below the HTTP boundary is the production code path: the Jupiter
quote client and the Solana RPC client talk to a local fake over HTTP, the
runtime builds the security context from their answers, sizes and fills the
entry, marks the position, takes profit and persists fills with evidence.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import select

from sniper_bot.candidates import CandidateState
from sniper_bot.config import AppConfig
from sniper_bot.db_models import PaperFillRow
from sniper_bot.events import ChainEventType, EventEnvelope, EventSource, Protocol
from sniper_bot.registry import USDC_MINT
from sniper_bot.runtime import MARK_COMPARISON_LOG, SniperRuntime
from sniper_bot.scoring import ScoreBreakdown
from tests.fake_providers import FakeProviders

TOKEN = "FakeToken1111111111111111111111111111111111"


def _event(event_type: ChainEventType, at: datetime, **payload: object) -> EventEnvelope:
    return EventEnvelope(
        source=EventSource.HELIUS_WSS,
        protocol=Protocol.PUMP if event_type == ChainEventType.TOKEN_CREATED else Protocol.PUMPSWAP,
        event_type=event_type,
        slot=1,
        signature=f"{event_type.value}-signature",
        instruction_index=1,
        block_time=at,
        observed_at=at,
        mint=TOKEN,
        pool_address=None if event_type == ChainEventType.TOKEN_CREATED else "POOL",
        payload=payload,
    )


def _score() -> ScoreBreakdown:
    zero = Decimal("0")
    return ScoreBreakdown(
        total_score=Decimal("95"),
        organic_score=zero,
        distribution_score=zero,
        execution_score=zero,
        liquidity_score=zero,
        developer_score=zero,
        price_structure_score=zero,
        explanations={},
    )


@pytest.mark.asyncio
async def test_entry_and_take_profit_through_real_clients_and_fake_providers(
    tmp_path: Path,
) -> None:
    with FakeProviders(quote_mint=USDC_MINT, token_mint=TOKEN) as providers:
        config = AppConfig(
            APP_MODE="paper",
            APP_REVISION="",
            HELIUS_API_KEY="helius-key",
            HELIUS_RPC_URL=f"{providers.base_url}/rpc",
            JUPITER_API_KEY="jupiter-key",
            POSTGRES_DSN=f"sqlite+aiosqlite:///{tmp_path / 'e2e.db'}",
            TELEGRAM_BOT_TOKEN="telegram-token",
            TELEGRAM_ADMIN_CHAT_ID=123456,
            STARTING_EQUITY_USD=Decimal("500"),
            providers={"jupiter_base_url": providers.base_url},
            paper={"execution_delay_ms": 0},
        )
        runtime = SniperRuntime(config, data_dir=tmp_path)
        database = runtime.database
        assert database is not None and runtime.broker is not None
        await database.create_schema_for_tests()
        runtime.database_available = True
        runtime.broker._database = database
        now = datetime.now(tz=timezone.utc)
        await database.register_strategy(
            strategy_id=config.strategy_version,
            version=config.strategy_version,
            config_hash=config.config_hash,
            config_json={},
            now=now,
        )
        await database.initialize_paper_account(
            account_id="paper-main", starting_equity=Decimal("500"), now=now
        )

        created = now - timedelta(seconds=60)
        await runtime.pipeline.process_event(
            _event(ChainEventType.TOKEN_CREATED, created, creator="dev-wallet", user="dev-wallet")
        )
        await runtime.pipeline.process_event(
            _event(
                ChainEventType.POOL_CREATED,
                created,
                base_mint=TOKEN,
                quote_mint=USDC_MINT,
                base_mint_decimals=6,
                quote_mint_decimals=6,
                pool_base_amount=1_000_000_000_000,
                pool_quote_amount=1_000_000_000_000,
            )
        )
        candidate = next(iter(runtime.pipeline.candidates.values())).model_copy(
            update={"state": CandidateState.ENTRY_PENDING}
        )
        runtime.pipeline.candidates[candidate.candidate_id] = candidate
        snapshot = runtime.pipeline.features.snapshot("POOL", now)

        security = await runtime._build_security_context(candidate, snapshot)
        assert security.holders is not None
        assert security.holders.largest_holder_pct == Decimal("0.4")
        assert security.execution.round_trip_loss_pct == Decimal("0")

        reason = await runtime._open_candidate(candidate, snapshot, _score(), security)
        assert reason is None
        [position] = runtime.ledger.open_positions
        assert position.remaining_cost_usd > Decimal("8")

        providers.price_usd = Decimal("0.0000014")  # +40 %: take profit 1
        decisions = await runtime.evaluate_and_close_exits(now=datetime.now(tz=timezone.utc))
        assert [decision.reason.value for decision in decisions] == ["TP1"]

        async with database.sessions() as session:
            fills = {
                fill.side: fill
                for fill in (await session.scalars(select(PaperFillRow))).all()
            }
        assert set(fills) == {"BUY", "SELL"}
        assert fills["BUY"].evidence_json is not None
        assert fills["BUY"].evidence_json["reference"]["tokens_per_usd"]
        assert Decimal(fills["BUY"].evidence_json["slippage_bps"]) == Decimal("0")
        assert fills["SELL"].evidence_json["quote"]["out_amount"]

        sources = [
            json.loads(line)["source"]
            for line in (tmp_path / MARK_COMPARISON_LOG).read_text().splitlines()
        ]
        assert sources == ["security_round_trip", "position"]

        assert providers.requests.count("RPC getAccountInfo") == 1
        assert providers.requests.count("RPC getTokenAccounts") == 1
        # Round trip (2), entry (1), mark (1), exit (1).
        assert providers.requests.count("GET /order") == 5
        await database.close()
