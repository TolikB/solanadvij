from __future__ import annotations

from decimal import Decimal

import pytest

from sniper_bot.broker import PaperBroker
from sniper_bot.config import AppConfig
from sniper_bot.id_utils import DeterministicIdFactory
from sniper_bot.ledger import PaperLedger
from sniper_bot.models import QuoteResponse
from sniper_bot.risk import RiskManager


class _StaticQuoteProvider:
    async def get_buy_quote(self, quote_token: str, token: str, usdc_amount: Decimal) -> QuoteResponse:
        return QuoteResponse(
            token_in=quote_token,
            token_out=token,
            in_amount=Decimal("100"),
            out_amount=Decimal("200"),
            in_amount_usd=Decimal("100"),
            out_amount_usd=Decimal("200"),
            route={},
            price_impact_pct=Decimal("0"),
        )

    async def get_sell_quote(self, token: str, quote_token: str, token_amount: Decimal) -> QuoteResponse:
        return QuoteResponse(
            token_in=token,
            token_out=quote_token,
            in_amount=Decimal("50"),
            out_amount=Decimal("75"),
            in_amount_usd=Decimal("50"),
            out_amount_usd=Decimal("75"),
            route={},
            price_impact_pct=Decimal("0"),
        )

    async def get_sell_quote_mark_to_market(self, token: str, quote_token: str, token_amount: Decimal) -> QuoteResponse:
        return await self.get_sell_quote(token, quote_token, token_amount)


def _build_runtime_parts(tmp_path, seed: int):
    ledger = PaperLedger(
        storage_path=tmp_path / "paper_ledger.json",
        starting_equity_usd=Decimal("500"),
        strategy_version="v",
        config_hash="h",
        id_factory=DeterministicIdFactory(seed),
    )
    risk = RiskManager(
        AppConfig(
            APP_MODE="paper",
            HELIUS_API_KEY="helius",
            JUPITER_API_KEY="jupiter",
            POSTGRES_DSN="postgresql://user:pass@localhost:5432/db",
            TELEGRAM_BOT_TOKEN="tg",
            TELEGRAM_ADMIN_CHAT_ID=123456,
            STARTING_EQUITY_USD=Decimal("500"),
            REPLAY_SEED=seed,
        ).risk,
        ledger,
    )
    broker = PaperBroker(
        quote_provider=_StaticQuoteProvider(),
        ledger=ledger,
        risk_manager=risk,
        base_quote_mint="SOL",
        id_factory=DeterministicIdFactory(seed),
    )
    return ledger, broker


@pytest.mark.asyncio
async def test_replay_seed_generates_deterministic_broker_and_ledger_ids(tmp_path) -> None:
    _, broker_a = _build_runtime_parts(tmp_path / "run_a", 2026)
    _, broker_b = _build_runtime_parts(tmp_path / "run_b", 2026)

    result_a = await broker_a.open("TOKEN", Decimal("10"))
    result_b = await broker_b.open("TOKEN", Decimal("10"))

    assert result_a.order_id == result_b.order_id
    assert result_a.fill_id == result_b.fill_id
    assert result_a.position_id == result_b.position_id


@pytest.mark.asyncio
async def test_replay_seed_generates_deterministic_close_ids(tmp_path) -> None:
    _, broker_a = _build_runtime_parts(tmp_path / "close_a", 2026)
    _, broker_b = _build_runtime_parts(tmp_path / "close_b", 2026)

    await broker_a.open("TOKEN", Decimal("10"))
    await broker_b.open("TOKEN", Decimal("10"))

    close_a = await broker_a.close("TOKEN", Decimal("2"))
    close_b = await broker_b.close("TOKEN", Decimal("2"))

    assert close_a.order_id == close_b.order_id
    assert close_a.fill_id == close_b.fill_id
