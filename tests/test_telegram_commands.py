from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from sniper_bot.api import build_api


class _FakeLedger:
    def __init__(self) -> None:
        self.state = type("S", (), {"is_halted": False, "positions": {}})()

    def snapshot(self) -> dict[str, object]:
        return {"equity_usd": "500"}


class _FakeNotifier:
    def __init__(self) -> None:
        self.sent: list[tuple[int, str]] = []
        self.command_handler: Any = None

    async def start(
        self,
        *,
        start_polling: bool = False,
        command_handler: Any = None,
    ) -> None:
        self.command_handler = command_handler

    async def stop(self) -> None:
        return None

    async def send(self, text: str) -> None:
        return None

    async def send_to(self, chat_id: int, text: str) -> None:
        self.sent.append((chat_id, text))


class _FakeRuntime:
    def __init__(self) -> None:
        self.config = type(
            "Cfg",
            (),
            {
                "app_mode": "paper",
                "strategy_version": "test",
                "config_hash": "hash",
                "starting_equity_usd": Decimal("500"),
                "replay_mode": False,
                "time_zone": "Europe/Kyiv",
                "telegram": type("Tg", (), {"enabled": True})(),
            },
        )()
        self.ledger = _FakeLedger()
        self.notifier = _FakeNotifier()

    def health_status(self) -> str:
        return "HEALTHY"

    def build_daily_report(self, date: str | None = None) -> dict[str, object]:
        return {
            "period": "daily",
            "date": date or "2026-08-25",
            "capital": {
                "starting_equity_usd": "500",
                "ending_equity_usd": "512.34",
            },
            "trades": {"closed": 2},
            "report_id": "must-not-be-shown",
        }

    def build_all_time_report(self) -> dict[str, object]:
        return {
            "period": "all_time",
            "current_equity_usd": "512.34",
            "net_pnl_usd": "12.34",
            "return_pct": "0.0247",
            "max_drawdown_pct": "0.01",
            "paper_entries": 3,
            "trade_statistics": {"closed": 2, "win_rate": "0.5"},
            "report_id": "must-not-be-shown",
        }


def _command_handler(runtime: _FakeRuntime) -> Any:
    app = build_api(runtime)
    with TestClient(app) as client:
        assert client.get("/health").status_code == 200
    handler = runtime.notifier.command_handler
    assert callable(handler)
    return handler


@pytest.mark.asyncio
async def test_report_commands_answer_with_human_text_not_json() -> None:
    runtime = _FakeRuntime()
    handler = _command_handler(runtime)
    runtime.notifier.sent.clear()

    await handler(42, "today", [])
    await handler(42, "day", ["2026-08-24"])
    await handler(42, "all", [])

    assert [chat_id for chat_id, _ in runtime.notifier.sent] == [42, 42, 42]
    today, day, all_time = (text for _, text in runtime.notifier.sent)

    local_today = datetime.now(ZoneInfo("Europe/Kyiv")).strftime("%Y-%m-%d")
    assert today.startswith("Щоденний звіт про тестову торгівлю")
    assert f"Дата: {local_today}" in today
    assert day.startswith("Щоденний звіт про тестову торгівлю")
    assert "Дата: 2026-08-24" in day
    assert all_time.startswith("Звіт про тестову торгівлю за весь час")
    assert "Чистий результат: +$12.34" in all_time
    for text in (today, day, all_time):
        assert "{" not in text
        assert "must-not-be-shown" not in text


@pytest.mark.asyncio
async def test_day_command_reports_an_invalid_date_in_plain_text() -> None:
    runtime = _FakeRuntime()
    handler = _command_handler(runtime)
    runtime.notifier.sent.clear()

    await handler(7, "day", ["not-a-date"])

    assert runtime.notifier.sent == [
        (7, "invalid date format, expected YYYY-MM-DD")
    ]
