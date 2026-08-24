from __future__ import annotations

from decimal import Decimal

import pytest

from sniper_bot.broker import PaperBroker
from sniper_bot.ledger import PaperLedger
from sniper_bot.models import QuoteResponse


class _Risk:
    def evaluate_entry(self, _notional, _mint=None):
        return type("Decision", (), {"decision": "allow", "reason": None})()


class _Quotes:
    async def get_buy_quote(self, quote_token, token, usdc_amount):
        return QuoteResponse(
            token_in=quote_token, token_out=token, in_amount=usdc_amount,
            out_amount=Decimal("100"), in_amount_usd=usdc_amount,
            out_amount_usd=usdc_amount, route={"routePlan": [{}]},
        )

    async def get_sell_quote(self, token, quote_token, token_amount):
        raise RuntimeError("no route")


@pytest.mark.asyncio
async def test_missing_sell_route_closes_at_zero_as_unrecoverable(tmp_path) -> None:
    ledger = PaperLedger(
        tmp_path / "ledger.json", Decimal("500"), "v1", "hash"
    )
    broker = PaperBroker(
        _Quotes(), ledger, _Risk(), execution_delay_ms=0,
        exit_retry_timeout_seconds=0, exit_retry_interval_ms=0,
    )
    opened = await broker.open("TOKEN", Decimal("10"))
    closed = await broker.close("TOKEN", opened.token_amount)

    assert closed.usd_notional == Decimal("0")
    assert ledger.state.positions[opened.position_id].final_exit_reason == "UNRECOVERABLE"
    assert ledger.state.positions[opened.position_id].realized_pnl_usd == Decimal("-10")
