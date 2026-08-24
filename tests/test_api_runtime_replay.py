from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from fastapi.testclient import TestClient

from sniper_bot.api import build_api
from sniper_bot.config import AppConfig
from sniper_bot.runtime import SniperRuntime
from sniper_bot.telegram import NoopTelegramNotifier


class _FakeLedger:
    def __init__(self) -> None:
        self.state = type("S", (), {"is_halted": False})()

    def daily_pnl(self, date_key: str) -> Decimal:
        return Decimal("12.34")

    def reconcile(self) -> dict[str, object]:
        return {"equity_usd": Decimal("500")}

    def snapshot(self) -> dict[str, object]:
        return {"equity_usd": "500"}


class _FakeNotifier:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False
        self.started_args: tuple[bool, object | None] | None = None
        self.sent: list[tuple[str, str]] = []

    async def start(self, *, start_polling: bool = False, command_handler=None) -> None:
        self.started = True
        self.started_args = (start_polling, command_handler)

    async def stop(self) -> None:
        self.stopped = True

    async def send(self, text: str) -> None:
        self.sent.append(("send", text))

    async def send_to(self, chat_id: int, text: str) -> None:
        self.sent.append((str(chat_id), text))


class _FakeRuntime:
    def __init__(self, app_mode: str = "record", replay_mode: bool = False) -> None:
        self.config = type("Cfg", (), {
            "app_mode": app_mode,
            "strategy_version": "test",
            "config_hash": "hash",
            "starting_equity_usd": Decimal("500"),
            "replay_mode": replay_mode,
        })()
        self.ledger = _FakeLedger()
        self.notifier = _FakeNotifier()

    def report(self) -> dict[str, object]:
        return {
            "today": {
                "date": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d"),
                "pnl": "12.34",
                "reconcile": self.ledger.reconcile(),
            },
            "all_time": {"date": "all", "pnl": "34.56"},
        }

    async def daily_report_if_not_sent(self, date: str | None = None, *, send: bool = False) -> dict[str, object] | None:  # noqa: ARG001
        return {"date": date or datetime.now(tz=timezone.utc).strftime("%Y-%m-%d"), "sent": False}

    def halt(self, reason: str) -> None:  # pragma: no cover
        self.halt_reason = reason

    def resume(self) -> None:  # pragma: no cover
        return None


def test_api_skips_telegram_in_replay_mode() -> None:
    runtime = _FakeRuntime(replay_mode=True)
    app = build_api(runtime)

    with TestClient(app) as client:
        assert client.get("/health").status_code == 200

    assert runtime.notifier.started is False
    assert runtime.notifier.stopped is False
    assert runtime.notifier.sent == []


def test_api_starts_telegram_not_in_replay_mode() -> None:
    runtime = _FakeRuntime(replay_mode=False)
    app = build_api(runtime)

    with TestClient(app) as client:
        assert client.get("/health").status_code == 200

    assert runtime.notifier.started is True
    assert runtime.notifier.stopped is True
    assert runtime.notifier.started_args is not None
    assert runtime.notifier.started_args[0] is True
    assert callable(runtime.notifier.started_args[1])
    assert len(runtime.notifier.sent) >= 1


def test_runtime_quote_flags_set_from_config(tmp_path) -> None:
    config = AppConfig(
        **{
            "APP_MODE": "paper",
            "HELIUS_API_KEY": "helius",
            "JUPITER_API_KEY": "jupiter",
            "POSTGRES_DSN": "postgresql://user:pass@localhost:5432/db",
            "TELEGRAM_BOT_TOKEN": "tg",
            "TELEGRAM_ADMIN_CHAT_ID": 123456,
            "STARTING_EQUITY_USD": Decimal("500"),
            "JUPITER_REPLAY_MODE": True,
            "JUPITER_QUOTE_JOURNAL_RECORD": True,
            "JUPITER_QUOTE_JOURNAL_PATH": str(tmp_path / "quotes.ndjson"),
        }
    )

    runtime = SniperRuntime(config, data_dir=tmp_path)

    assert runtime.quote_provider._replay_mode is True
    assert runtime.quote_provider._record_quotes is True
    assert isinstance(runtime.notifier, NoopTelegramNotifier)


def test_api_startup_does_not_crash_on_notifier_send_failure() -> None:
    class _FailingNotifier(_FakeNotifier):
        async def start(self, *, start_polling: bool = False, command_handler=None) -> None:
            self.started = True
            self.started_args = (start_polling, command_handler)

        async def send(self, text: str) -> None:
            self.sent.append(("send", text))
            raise RuntimeError("telegram unavailable")

        async def stop(self) -> None:
            self.stopped = True

    runtime = _FakeRuntime(replay_mode=False)
    runtime.notifier = _FailingNotifier()
    app = build_api(runtime)

    with TestClient(app) as client:
        assert client.get("/health").status_code == 200

    assert runtime.notifier.started is True
    assert runtime.notifier.stopped is True
    assert len(runtime.notifier.sent) >= 1
