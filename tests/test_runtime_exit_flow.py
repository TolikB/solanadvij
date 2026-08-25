from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from sniper_bot.config import AppConfig
from sniper_bot.exit_engine import ExitPolicy
from sniper_bot.models import QuoteResponse
from sniper_bot.runtime import SniperRuntime


class _RecorderNotifier:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send(self, text: str) -> None:
        self.sent.append(text)

    async def send_to(self, chat_id: int, text: str) -> None:
        self.sent.append(f"{chat_id}:{text}")

    async def start(self, *, start_polling: bool = False, command_handler=None) -> None:
        return None

    async def stop(self) -> None:
        return None


class _FakeQuoteProvider:
    def __init__(self, quotes_by_token: dict[str, Decimal]) -> None:
        self.quotes_by_token = quotes_by_token

    async def get_buy_quote(self, quote_token: str, token: str, usdc_amount: Decimal) -> QuoteResponse:
        out_amount = self.quotes_by_token.get(token, usdc_amount)
        return QuoteResponse(
            token_in=quote_token,
            token_out=token,
            in_amount=usdc_amount,
            out_amount=out_amount,
            in_amount_usd=usdc_amount,
            out_amount_usd=out_amount,
            route={},
            price_impact_pct=Decimal("0"),
        )

    async def get_sell_quote(self, token: str, quote_token: str, token_amount: Decimal) -> QuoteResponse:
        out_amount = self.quotes_by_token.get(token, token_amount)
        return QuoteResponse(
            token_in=token,
            token_out=quote_token,
            in_amount=token_amount,
            out_amount=out_amount,
            in_amount_usd=token_amount,
            out_amount_usd=out_amount,
            route={},
            price_impact_pct=Decimal("0"),
        )

    async def get_sell_quote_mark_to_market(self, token: str, quote_token: str, token_amount: Decimal) -> QuoteResponse:
        return await self.get_sell_quote(token, quote_token, token_amount)


class _FailingMarkQuoteProvider(_FakeQuoteProvider):
    async def get_sell_quote_mark_to_market(
        self, token: str, quote_token: str, token_amount: Decimal
    ) -> QuoteResponse:
        raise RuntimeError("temporary quote failure")


def _base_record_config(mode: str, **extra: object) -> dict:
    data: dict[str, object] = {
        "APP_MODE": mode,
        "HELIUS_API_KEY": "helius-key",
        "JUPITER_API_KEY": "jupiter-key",
        "POSTGRES_DSN": "postgresql://user:pass@localhost:5432/db",
        "TELEGRAM_BOT_TOKEN": "telegram-token",
        "TELEGRAM_ADMIN_CHAT_ID": 123456,
        "STARTING_EQUITY_USD": Decimal("500"),
    }
    data.update(extra)
    return data


def _build_runtime(tmp_path: Path, quotes_by_token: dict[str, Decimal] | None = None) -> SniperRuntime:
    runtime = SniperRuntime(AppConfig(**_base_record_config("paper")), data_dir=tmp_path)
    provider = _FakeQuoteProvider(quotes_by_token or {})
    runtime.quote_provider = provider
    if runtime.broker is not None:
        runtime.broker._quote_provider = provider
    runtime.notifier = _RecorderNotifier()
    return runtime


def _set_runtime_quote_provider(runtime: SniperRuntime, quotes_by_token: dict[str, Decimal]) -> None:
    provider = _FakeQuoteProvider(quotes_by_token)
    runtime.quote_provider = provider
    if runtime.broker is not None:
        runtime.broker._quote_provider = provider


async def _open(runtime: SniperRuntime, token: str, usdc: Decimal) -> None:
    if runtime.broker is None:
        raise RuntimeError("paper broker missing")
    await runtime.broker.open(token, usdc)


@pytest.mark.asyncio
async def test_runtime_evaluate_and_close_exits_takes_profit(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path, {"TOKEN": Decimal("10")})
    if runtime.broker is None:
        raise RuntimeError("paper broker disabled")

    await _open(runtime, "TOKEN", Decimal("10"))
    _set_runtime_quote_provider(runtime, {"TOKEN": Decimal("110")})

    decisions = await runtime.evaluate_and_close_exits(policy=ExitPolicy(tp1_return=Decimal("0.03"), tp2_return=Decimal("0.07")))

    assert len(decisions) == 1
    assert decisions[0].should_exit is True
    assert decisions[0].reason.value == "TP1"
    snapshot = runtime.ledger.snapshot()
    assert snapshot["positions"][0]["status"] == "open"
    assert snapshot["positions"][0]["tp1_taken"] is True
    assert runtime.notifier.sent == []


@pytest.mark.asyncio
async def test_runtime_evaluate_and_close_exits_stops_loss(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path, {"TOKEN": Decimal("10")})
    if runtime.broker is None:
        raise RuntimeError("paper broker disabled")

    await _open(runtime, "TOKEN", Decimal("10"))
    _set_runtime_quote_provider(runtime, {"TOKEN": Decimal("9")})

    decisions = await runtime.evaluate_and_close_exits(policy=ExitPolicy(stop_loss_return=Decimal("-0.05")))

    assert len(decisions) == 1
    assert decisions[0].should_exit is True
    assert decisions[0].reason.value == "STOP_LOSS"
    snapshot = runtime.ledger.snapshot()
    assert snapshot["positions"][0]["final_exit_reason"] == "STOP_LOSS"


@pytest.mark.asyncio
async def test_runtime_evaluate_and_close_exits_respects_max_positions_limit(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path, {"TOKEN_A": Decimal("10"), "TOKEN_B": Decimal("10")})
    if runtime.broker is None:
        raise RuntimeError("paper broker disabled")

    await _open(runtime, "TOKEN_A", Decimal("10"))
    await _open(runtime, "TOKEN_B", Decimal("10"))
    _set_runtime_quote_provider(runtime, {"TOKEN_A": Decimal("120"), "TOKEN_B": Decimal("120")})

    decisions = await runtime.evaluate_and_close_exits(
        max_positions=1,
        policy=ExitPolicy(
            tp1_return=Decimal("0.01"), tp1_size=Decimal("1"),
            tp2_return=Decimal("0.50"),
        ),
    )

    assert sum(1 for decision in decisions if decision.should_exit) == 1
    snapshot = runtime.ledger.snapshot()
    closed_count = sum(1 for row in snapshot["positions"] if row["status"] == "closed")
    open_count = sum(1 for row in snapshot["positions"] if row["status"] == "open")
    assert closed_count == 1
    assert open_count == 1
    assert runtime.notifier.sent == []


@pytest.mark.asyncio
async def test_service_process_exits_counts_only_actual_closes(tmp_path: Path) -> None:
    from sniper_bot.service import PaperService

    runtime = _build_runtime(tmp_path, {"TOKEN": Decimal("10")})
    service = PaperService(runtime)
    if runtime.broker is None:
        raise RuntimeError("paper broker disabled")

    await _open(runtime, "TOKEN", Decimal("10"))
    _set_runtime_quote_provider(runtime, {"TOKEN": Decimal("80")})
    closes = await service.process_exits(policy=ExitPolicy(stop_loss_return=Decimal("-0.05")))

    assert closes == 1


@pytest.mark.asyncio
async def test_record_mode_blocks_process_exits(tmp_path: Path) -> None:
    from pytest import raises

    from sniper_bot.service import PaperService

    runtime = SniperRuntime(AppConfig(**_base_record_config("record")), data_dir=tmp_path)
    service = PaperService(runtime)

    with raises(RuntimeError, match="broker is disabled in record mode"):
        await service.process_exits()


@pytest.mark.asyncio
async def test_runtime_evaluate_and_close_exits_no_positions_returns_empty(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path, {"TOKEN": Decimal("10")})

    decisions = await runtime.evaluate_and_close_exits()

    assert decisions == []


@pytest.mark.asyncio
async def test_runtime_evaluate_and_close_exits_keeps_position_when_no_exit_signal(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    if runtime.broker is None:
        raise RuntimeError("paper broker disabled")

    await _open(runtime, "TOKEN", Decimal("10"))
    _set_runtime_quote_provider(runtime, {"TOKEN": Decimal("10.10")})

    decisions = await runtime.evaluate_and_close_exits(policy=ExitPolicy(tp1_return=Decimal("1.0"), tp2_return=Decimal("2.0")))

    assert len(decisions) == 1
    assert decisions[0].should_exit is False
    assert decisions[0].reason.value == "NO_EXIT"
    snapshot = runtime.ledger.snapshot()
    assert snapshot["positions"][0]["status"] == "open"
    assert snapshot["positions"][0]["final_exit_reason"] is None


@pytest.mark.asyncio
async def test_transient_mark_quote_failure_does_not_close_position(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path, {"TOKEN": Decimal("10")})
    await _open(runtime, "TOKEN", Decimal("10"))
    failing_provider = _FailingMarkQuoteProvider({})
    runtime.quote_provider = failing_provider
    if runtime.broker is None:
        raise RuntimeError("paper broker disabled")
    runtime.broker._quote_provider = failing_provider

    decisions = await runtime.evaluate_and_close_exits()

    assert decisions == []
    snapshot = runtime.ledger.snapshot()
    assert snapshot["positions"][0]["status"] == "open"
    assert snapshot["positions"][0]["final_exit_reason"] is None
    position_id = snapshot["positions"][0]["position_id"]
    assert runtime._mark_quote_failures[position_id][1] == 1
