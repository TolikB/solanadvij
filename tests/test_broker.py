from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from sniper_bot.broker import PaperBroker
from sniper_bot.ledger import PaperLedger
from sniper_bot.models import PositionRecord, PositionStatus, QuoteResponse


class _CountingRisk:
    def __init__(self) -> None:
        self.calls = 0

    def evaluate_entry(self, notional: Decimal, token_mint: str | None = None):
        self.calls += 1
        return type("R", (), {"decision": "allow", "reason": None})()


class _FakeQuoteProvider:
    def __init__(self) -> None:
        self.sell_calls: list[tuple[str, str, Decimal]] = []

    async def get_sell_quote(self, token: str, quote_token: str, token_amount: Decimal) -> QuoteResponse:
        self.sell_calls.append((token, quote_token, token_amount))
        return QuoteResponse(
            token_in=token,
            token_out=quote_token,
            in_amount=Decimal("0"),
            out_amount=Decimal("5"),
            price_impact_pct=Decimal("0"),
            raw={},
        )

    async def get_buy_quote(self, quote_token: str, token: str, usdc_amount: Decimal) -> QuoteResponse:  # pragma: no cover
        raise RuntimeError("not used in this test")


class _BuySellQuoteProvider:
    def __init__(self) -> None:
        self.buy_calls: list[tuple[str, str, Decimal]] = []
        self.sell_calls: list[tuple[str, str, Decimal]] = []

    async def get_buy_quote(self, quote_token: str, token: str, usdc_amount: Decimal) -> QuoteResponse:
        self.buy_calls.append((quote_token, token, usdc_amount))
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
        self.sell_calls.append((token, quote_token, token_amount))
        return QuoteResponse(
            token_in=token,
            token_out=quote_token,
            in_amount=Decimal("50"),
            out_amount=Decimal("75"),
            in_amount_usd=Decimal("50"),
            out_amount_usd=Decimal("75"),
            route={},
            price_impact_pct=Decimal("0"),
            raw={},
        )


class _FakeLedger:
    def __init__(self) -> None:
        self.last_close_args: tuple[str, Decimal, Decimal, str, str] | None = None
        self.positions = []
        self._position = PositionRecord(
            position_id="pos-1",
            token_mint="TOKEN",
            open_fill_id="fill-open",
            entry_token_amount=Decimal("100"),
            entry_cost_usd=Decimal("10"),
            open_ratio=Decimal("0.1"),
            opened_at=datetime(2026, 8, 18, tzinfo=timezone.utc),
            locked_usd=Decimal("10"),
            status=PositionStatus.OPEN,
            remaining_token_amount=Decimal("100"),
            remaining_cost_usd=Decimal("10"),
        )
        self.positions.append(self._position)

    @property
    def open_positions(self) -> list[PositionRecord]:
        return [self._position]

    def close_position(
        self,
        token_mint: str,
        usd_received: Decimal,
        token_closed: Decimal,
        order_id: str,
        quote_id: str,
        exit_reason: str | None = None,
        close_fill_id: str | None = None,
    ) -> PositionRecord:
        self.last_close_args = (token_mint, usd_received, token_closed, order_id, quote_id)
        return self._position


class _FakeRisk:
    def evaluate_entry(self, notional: Decimal, token_mint: str | None = None):  # pragma: no cover
        raise RuntimeError("not used in this test")


@pytest.mark.asyncio
async def test_close_uses_requested_token_amount() -> None:
    quote_provider = _FakeQuoteProvider()
    ledger = _FakeLedger()
    broker = PaperBroker(quote_provider=quote_provider, ledger=ledger, risk_manager=_FakeRisk())

    result = await broker.close("TOKEN", Decimal("10"))

    assert ledger.last_close_args is not None
    assert quote_provider.sell_calls == [("TOKEN", "So11111111111111111111111111111111111111112", Decimal("10"))]
    _, usd_received, token_closed, _, _ = ledger.last_close_args
    assert token_closed == Decimal("10")
    assert usd_received == Decimal("4.975")
    assert result.usd_notional == Decimal("4.975")


@pytest.mark.asyncio
async def test_close_more_than_position_rejected() -> None:
    quote_provider = _FakeQuoteProvider()
    ledger = _FakeLedger()
    broker = PaperBroker(quote_provider=quote_provider, ledger=ledger, risk_manager=_FakeRisk())

    with pytest.raises(ValueError, match="exceeds remaining position"):
        await broker.close("TOKEN", Decimal("200"))


@pytest.mark.asyncio
async def test_open_idempotency_order_id_skips_new_quote_and_risk_calls(tmp_path) -> None:
    provider = _BuySellQuoteProvider()
    ledger = PaperLedger(
        storage_path=tmp_path / "paper_ledger.json",
        starting_equity_usd=Decimal("500"),
        strategy_version="v",
        config_hash="h",
    )
    risk = _CountingRisk()
    broker = PaperBroker(
        quote_provider=provider,
        ledger=ledger,
        risk_manager=risk,
        base_quote_mint="SOL",
    )

    first = await broker.open("TOKEN", Decimal("10"), order_id="order-1")
    second = await broker.open("TOKEN", Decimal("10"), order_id="order-1")

    assert first.fill_id == second.fill_id
    assert first.order_id == second.order_id == "order-1"
    assert len(provider.buy_calls) == 1
    assert risk.calls == 1


@pytest.mark.asyncio
async def test_close_idempotency_order_id_skips_new_quote(tmp_path) -> None:
    provider = _BuySellQuoteProvider()
    ledger = PaperLedger(
        storage_path=tmp_path / "paper_ledger.json",
        starting_equity_usd=Decimal("500"),
        strategy_version="v",
        config_hash="h",
    )
    risk = _CountingRisk()
    broker = PaperBroker(
        quote_provider=provider,
        ledger=ledger,
        risk_manager=risk,
        base_quote_mint="SOL",
    )

    open_result = await broker.open("TOKEN", Decimal("10"))
    first = await broker.close("TOKEN", Decimal("2"), order_id="close-1")
    second = await broker.close("TOKEN", Decimal("2"), order_id="close-1")

    assert first.fill_id == second.fill_id
    assert first.order_id == second.order_id == "close-1"
    assert first.usd_notional == second.usd_notional == Decimal("74.625")
    assert len(provider.sell_calls) == 1
    assert risk.calls == 1
    assert open_result.fill_id is not None


@pytest.mark.asyncio
async def test_close_half_idempotency_order_id_skips_new_quote(tmp_path) -> None:
    provider = _BuySellQuoteProvider()
    ledger = PaperLedger(
        storage_path=tmp_path / "paper_ledger.json",
        starting_equity_usd=Decimal("500"),
        strategy_version="v",
        config_hash="h",
    )
    risk = _CountingRisk()
    broker = PaperBroker(
        quote_provider=provider,
        ledger=ledger,
        risk_manager=risk,
        base_quote_mint="SOL",
    )

    await broker.open("TOKEN", Decimal("10"))
    first = await broker.close_half("TOKEN", order_id="close-half-1")
    second = await broker.close_half("TOKEN", order_id="close-half-1")

    assert first.fill_id == second.fill_id
    assert first.order_id == second.order_id == "close-half-1"
    assert first.usd_notional == second.usd_notional == Decimal("74.625")
    assert len(provider.sell_calls) == 1
