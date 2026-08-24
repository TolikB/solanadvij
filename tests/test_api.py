from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

from fastapi.testclient import TestClient

from sniper_bot.api import build_api


class _FakeLedger:
    def __init__(self) -> None:
        self.state = SimpleNamespace(is_halted=False)

    def daily_pnl(self, date_key: str) -> Decimal:
        return Decimal("12.34")

    def reconcile(self) -> dict[str, object]:
        return {"equity_usd": Decimal("500")}

    def snapshot(self) -> dict[str, object]:
        return {"equity_usd": "500"}


class _FakeNotifier:
    async def start(self, *, start_polling: bool = False, command_handler=None) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send(self, text: str) -> None:  # pragma: no cover
        return None

    async def send_to(self, chat_id: int, text: str) -> None:  # pragma: no cover
        return None


class _FakeRuntime:
    def __init__(self) -> None:
        self.config = SimpleNamespace(
            app_mode="record",
            strategy_version="test",
            config_hash="hash",
            starting_equity_usd=Decimal("500"),
        )
        self.ledger = _FakeLedger()
        self.notifier = _FakeNotifier()

    def report(self) -> dict[str, object]:
        return {
            "today": {"date": datetime.now().strftime("%Y-%m-%d"), "pnl": "12.34", "reconcile": self.ledger.reconcile()},
            "all_time": {"date": "all", "pnl": "34.56"},
        }

    async def daily_report_if_not_sent(self, date: str | None = None, *, send: bool = False) -> dict[str, object] | None:  # noqa: ARG001
        return {"date": date or datetime.now().strftime("%Y-%m-%d"), "sent": False}

    def halt(self, reason: str) -> None:  # pragma: no cover
        self.halt_reason = reason

    def resume(self) -> None:  # pragma: no cover
        return None


def test_day_endpoint_rejects_invalid_date() -> None:
    app = build_api(_FakeRuntime())
    with TestClient(app) as client:
        response = client.get("/day?date=2026/08/18")
    assert response.status_code == 400
    payload = response.json()
    assert payload["detail"] == "invalid date format, expected YYYY-MM-DD"


def test_health_live_alias_present() -> None:
    app = build_api(_FakeRuntime())
    with TestClient(app) as client:
        response = client.get("/health/live")
    assert response.status_code == 200
    assert response.json()["ok"] is True
