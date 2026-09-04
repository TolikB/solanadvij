from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from sniper_bot import main as main_module
from sniper_bot.config import AppConfig
from sniper_bot.errors import LiveTradingNotImplementedError
from sniper_bot.maintenance import RawRetentionManager


def _config(*, replay: bool = False) -> AppConfig:
    return AppConfig(
        APP_MODE="paper",
        HELIUS_API_KEY="helius",
        JUPITER_API_KEY="jupiter",
        POSTGRES_DSN="postgresql://user:pass@localhost:5432/db",
        TELEGRAM_BOT_TOKEN="telegram",
        TELEGRAM_ADMIN_CHAT_ID=123,
        STARTING_EQUITY_USD=Decimal("500"),
        JUPITER_REPLAY_MODE=replay,
        JUPITER_QUOTE_JOURNAL_PATH="quotes.ndjson",
    )


def test_main_starts_api_with_requested_bind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config()
    runtime = object()
    app = object()
    calls: list[tuple[object, str, int, object]] = []
    monkeypatch.setattr(
        sys,
        "argv",
        ["sniper", "--host", "0.0.0.0", "--port", "9000"],
    )
    monkeypatch.setattr(main_module.AppConfig, "load", lambda _path: config)
    monkeypatch.setattr(main_module, "SniperRuntime", lambda _config: runtime)
    monkeypatch.setattr(main_module, "build_api", lambda _runtime: app)
    monkeypatch.setattr(
        main_module.uvicorn,
        "run",
        lambda target, *, host, port, log_config: calls.append(
            (target, host, port, log_config)
        ),
    )

    main_module.main()

    assert calls == [(app, "0.0.0.0", 9000, None)]


def test_main_replay_forces_quote_journal_mode_and_prints_result(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = _config()
    seen: list[tuple[AppConfig, str]] = []
    monkeypatch.setattr(
        sys,
        "argv",
        ["sniper", "--replay-data", "archive", "--replay-speed", "max"],
    )
    monkeypatch.setattr(main_module.AppConfig, "load", lambda _path: config)

    def runtime_factory(
        runtime_config: AppConfig,
        *,
        data_dir: str,
    ) -> object:
        seen.append((runtime_config, data_dir))
        return object()

    def run(coroutine: Any) -> dict[str, object]:
        coroutine.close()
        return {"events_executed": 2}

    monkeypatch.setattr(main_module, "SniperRuntime", runtime_factory)
    monkeypatch.setattr(main_module.asyncio, "run", run)

    main_module.main()

    assert seen[0][0].replay_mode is True
    assert json.loads(capsys.readouterr().out) == {"events_executed": 2}


def test_main_fails_closed_for_missing_config_and_live_mode(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    missing = tmp_path / "missing.yaml"
    monkeypatch.setattr(sys, "argv", ["sniper", "--config", str(missing)])
    with pytest.raises(SystemExit, match="Config file not found"):
        main_module.main()

    existing = tmp_path / "config.yaml"
    existing.write_text("app_mode: live", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["sniper", "--config", str(existing)])

    def reject_live(_path: str | None) -> AppConfig:
        raise LiveTradingNotImplementedError("disabled")

    monkeypatch.setattr(main_module.AppConfig, "load", reject_live)
    with pytest.raises(SystemExit) as caught:
        main_module.main()
    assert caught.value.code == 78
    assert capsys.readouterr().out.splitlines()[-2:] == [
        "LIVE_TRADING_NOT_IMPLEMENTED",
        "disabled",
    ]


@pytest.mark.asyncio
async def test_raw_replay_registers_revision_and_closes_database(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[str] = []

    class _Database:
        def __init__(self, dsn: str) -> None:
            assert dsn.endswith("/db")

        async def ping(self) -> bool:
            events.append("ping")
            return True

        async def register_strategy(self, **kwargs: object) -> None:
            assert kwargs["strategy_id"]
            events.append("register")

        async def close(self) -> None:
            events.append("close")

    class _Runner:
        def __init__(
            self,
            runtime: object,
            *,
            store: object,
            database: object,
        ) -> None:
            del runtime, store, database

        async def run(self, source: str, *, speed: object) -> object:
            assert source == "archive"
            del speed
            return SimpleNamespace(
                run_id="run",
                input_hash="input",
                output_hash="output",
                events_executed=3,
                candidate_states={"armed": 1},
                final_reconcile={"is_reconciled": True},
            )

    monkeypatch.setattr(main_module, "Database", _Database)
    monkeypatch.setattr(main_module, "RawEventReplayRunner", _Runner)
    runtime = SimpleNamespace(data_dir=tmp_path)
    result = await main_module._run_raw_replay(
        runtime,
        _config(replay=True),
        "archive",
        "max",
    )

    assert result["events_executed"] == 3
    assert events == ["ping", "register", "close"]


def test_retention_removes_only_expired_archives(tmp_path: Path) -> None:
    manager = RawRetentionManager(tmp_path / "missing", 1)
    assert manager.retention_days == 90
    assert manager.run() == 0

    root = tmp_path / "raw"
    root.mkdir()
    old = root / "old.ndjson.zst"
    fresh = root / "fresh.ndjson.zst"
    old.write_bytes(b"old")
    fresh.write_bytes(b"fresh")
    now = datetime(2026, 9, 1, tzinfo=timezone.utc)
    old_time = (now - timedelta(days=91)).timestamp()
    fresh_time = (now - timedelta(days=1)).timestamp()
    os.utime(old, (old_time, old_time))
    os.utime(fresh, (fresh_time, fresh_time))

    assert RawRetentionManager(root, 90).run(now=now) == 1
    assert not old.exists()
    assert fresh.exists()