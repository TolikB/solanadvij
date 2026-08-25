from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import pytest

from sniper_bot.errors import ExecutionBlockedError
from sniper_bot.service import PaperService


@dataclass
class _Result:
    fill_id: str


class _BrokerSpy:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple]] = []

    async def open(self, token_mint: str, usdc_amount: Decimal, *, order_id: str | None = None) -> _Result:
        self.calls.append(("open", (token_mint, usdc_amount, order_id)))
        return _Result(f"fill:{order_id or 'none'}")

    async def close(self, token_mint: str, token_amount: Decimal, *, order_id: str | None = None, exit_reason: str | None = None) -> _Result:
        self.calls.append(("close", (token_mint, token_amount, order_id)))
        return _Result(f"fill:{order_id or 'none'}")

    async def close_half(self, token_mint: str, *, order_id: str | None = None) -> _Result:
        self.calls.append(("close_half", (token_mint, order_id)))
        return _Result(f"fill:{order_id or 'none'}")


class _Runtime:
    def __init__(self, broker: object | None, notifier: object | None = None) -> None:
        self.broker = broker
        self.notifier = notifier


class _NotifierSpy:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send(self, text: str) -> None:
        self.sent.append(text)

    async def send_to(self, chat_id: int, text: str) -> None:
        self.sent.append(f"{chat_id}:{text}")


class _RiskBlockingBroker:
    def __init__(self, error: Exception) -> None:
        self.error = error
        self.calls: list[tuple[str, tuple]] = []

    async def open(self, token_mint: str, usdc_amount: Decimal, *, order_id: str | None = None) -> object:
        self.calls.append(("open", (token_mint, usdc_amount, order_id)))
        raise self.error

    async def close(self, token_mint: str, token_amount: Decimal, *, order_id: str | None = None, exit_reason: str | None = None) -> object:
        self.calls.append(("close", (token_mint, token_amount, order_id, exit_reason)))
        raise RuntimeError("not expected")

    async def close_half(self, token_mint: str, *, order_id: str | None = None) -> object:
        self.calls.append(("close_half", (token_mint, order_id)))
        raise RuntimeError("not expected")


@pytest.mark.asyncio
async def test_service_forwards_order_id_to_broker_calls() -> None:
    broker = _BrokerSpy()
    notifier = _NotifierSpy()
    service = PaperService(_Runtime(broker, notifier))

    open_fill = await service.open_position("TOKEN", Decimal("10"), order_id="order-1")
    close_fill = await service.close_position("TOKEN", Decimal("5"), order_id="order-2")
    close_half_fill = await service.close_half("TOKEN", order_id="order-3")

    assert open_fill == "fill:order-1"
    assert close_fill == "fill:order-2"
    assert close_half_fill == "fill:order-3"
    assert broker.calls == [
        ("open", ("TOKEN", Decimal("10"), "order-1")),
        ("close", ("TOKEN", Decimal("5"), "order-2")),
        ("close_half", ("TOKEN", "order-3")),
    ]
    assert notifier.sent == []


@pytest.mark.asyncio
async def test_service_in_record_mode_raises() -> None:
    service = PaperService(_Runtime(None))

    with pytest.raises(RuntimeError, match="broker is disabled in record mode"):
        await service.open_position("TOKEN", Decimal("10"))


@pytest.mark.asyncio
async def test_service_suppresses_risk_alert_when_open_is_blocked() -> None:
    notifier = _NotifierSpy()
    broker = _RiskBlockingBroker(ExecutionBlockedError("MAX_POSITION_LIMIT"))
    service = PaperService(_Runtime(broker, notifier))

    with pytest.raises(ExecutionBlockedError):
        await service.open_position("TOKEN", Decimal("10"))

    assert notifier.sent == []
