from types import SimpleNamespace

from fastapi.testclient import TestClient

from sniper_bot.api import build_api


class _Notifier:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def start(self, **_kwargs: object) -> None:
        self.events.append("notifier-start")

    async def stop(self) -> None:
        self.events.append("notifier-stop")


class _Runtime:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.notifier = _Notifier(self.events)
        self.database = None
        self.database_available = False
        self.config = SimpleNamespace(
            replay_mode=False,
            app_mode=SimpleNamespace(value="paper"),
            strategy_version="strategy-v1",
            config_hash="hash",
            starting_equity_usd=500,
            time_zone="Europe/Kyiv",
        )

    async def start(self) -> None:
        self.events.append("runtime-start")

    async def _notify_system_alert(self, message: str) -> None:
        assert "runtime-start" in self.events
        self.events.append("alert-start" if "started" in message else "alert-stop")

    async def queue_all_time_report(self) -> bool:
        self.events.append("all-time-report")
        return True

    async def shutdown(self) -> None:
        self.events.append("runtime-shutdown")

    def build_daily_report(self, date: str | None = None) -> dict[str, str]:
        return {"period": "daily", "date": date or "today"}

    def build_all_time_report(self) -> dict[str, str]:
        return {"period": "all_time"}

    async def build_all_time_report_with_history(self) -> dict[str, str]:
        return {"period": "all_time", "drawdown_source": "equity_marks"}


def test_api_lifespan_starts_notifier_before_runtime_and_queues_durable_alerts() -> None:
    runtime = _Runtime()
    with TestClient(build_api(runtime)) as client:
        assert runtime.events == [
            "notifier-start",
            "runtime-start",
            "alert-start",
        ]
        assert client.get("/api/v1/reports/today").json()["period"] == "daily"
        assert client.get("/api/v1/reports/all-time").json() == {
            "period": "all_time",
            "drawdown_source": "equity_marks",
        }
    assert runtime.events == [
        "notifier-start",
        "runtime-start",
        "alert-start",
        "alert-stop",
        "all-time-report",
        "runtime-shutdown",
    ]
