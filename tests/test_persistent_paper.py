from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from sniper_bot.candidates import Candidate
from sniper_bot.database import Database
from sniper_bot.db_models import PaperAccountRow, PaperFillRow, PaperPositionRow
from sniper_bot.models import QuoteResponse
from sniper_bot.registry import USDC_MINT, PoolRecord, TokenRecord


@pytest.mark.asyncio
async def test_paper_entry_commits_atomic_idempotent_row_set(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'paper.db'}")
    await database.create_schema_for_tests()
    now = datetime(2026, 8, 18, tzinfo=timezone.utc)
    await database.register_strategy(
        strategy_id="strategy-v1", version="strategy-v1", config_hash="hash",
        config_json={}, now=now,
    )
    await database.upsert_token(TokenRecord(mint="TOKEN", creation_time=now, updated_at=now))
    await database.upsert_pool(
        PoolRecord(
            pool_address="POOL", base_mint="TOKEN", quote_mint=USDC_MINT,
            creation_signature="pool", creation_slot=1, creation_time=now,
            base_decimals=6, quote_decimals=6, updated_at=now,
        )
    )
    await database.upsert_candidate(
        Candidate(
            candidate_id="candidate", mint="TOKEN", pool_address="POOL",
            detected_at=now, updated_at=now, strategy_version="strategy-v1",
            config_hash="hash",
        ),
        "strategy-v1",
    )
    await database.initialize_paper_account(
        account_id="paper-main", starting_equity=Decimal("500"), now=now
    )
    quote = QuoteResponse(
        request_id="quote", requested_at=now, received_at=now,
        token_in=USDC_MINT, token_out="TOKEN", in_amount=Decimal("10000000"),
        out_amount=Decimal("100"), in_amount_usd=Decimal("10"),
        out_amount_usd=Decimal("10"), route={"routePlan": [{}]}, raw={"outAmount": "100"},
    )
    values = dict(
        account_id="paper-main", strategy_version_id="strategy-v1", config_hash="hash",
        candidate_id="candidate", pool_address="POOL", mint="TOKEN", order_id="order",
        position_id="position", fill_id="fill", quote=quote, requested_usd=Decimal("10"),
        filled_token_amount=Decimal("99.5"), adverse_fill_bps=50, filled_at=now,
        outbox_text="entry",
    )

    first = await database.commit_paper_entry(**values)
    second = await database.commit_paper_entry(**values)

    assert first["fill_id"] == second["fill_id"] == "fill"
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(PaperFillRow)) == 1
        assert await session.scalar(select(func.count()).select_from(PaperPositionRow)) == 1
        account = await session.get(PaperAccountRow, "paper-main")
        assert account is not None
        assert account.cash_balance == Decimal("490")
        assert account.locked_capital == Decimal("10")
    blocked_values = {
        **values,
        "order_id": "order-over-limit",
        "position_id": "position-over-limit",
        "fill_id": "fill-over-limit",
        "requested_usd": Decimal("21"),
        "filled_token_amount": Decimal("200"),
        "max_position_usdc": Decimal("20"),
    }
    with pytest.raises(RuntimeError, match="maximum position"):
        await database.commit_paper_entry(**blocked_values)
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(PaperFillRow)) == 1
        assert await session.scalar(select(func.count()).select_from(PaperPositionRow)) == 1
    await database.close()
