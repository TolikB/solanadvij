from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import sniper_bot.runtime as runtime_module
from sniper_bot.config import AppConfig
from sniper_bot.database import ActiveRuntimeError
from sniper_bot.runtime import SniperRuntime


def _config(*, telegram_enabled: bool = False) -> AppConfig:
    return AppConfig(
        APP_MODE="paper",
        HELIUS_API_KEY="helius-key",
        JUPITER_API_KEY="jupiter-key",
        POSTGRES_DSN="postgresql://user:pass@localhost:5432/db",
        TELEGRAM_BOT_TOKEN="telegram-token",
        TELEGRAM_ADMIN_CHAT_ID=123456,
        STARTING_EQUITY_USD=Decimal("500"),
        telegram={"enabled": telegram_enabled},
    )


def _runtime(tmp_path: Path, *, telegram_enabled: bool = False) -> SniperRuntime:
    runtime = SniperRuntime(
        _config(telegram_enabled=telegram_enabled),
        data_dir=tmp_path,
    )
    runtime.notifier = SimpleNamespace(send=AsyncMock(), stop=AsyncMock())
    runtime.outbox_worker = None
    return runtime


def _startup_database() -> SimpleNamespace:
    return SimpleNamespace(
        ping=AsyncMock(return_value=True),
        acquire_runtime_lease=AsyncMock(),
        register_strategy=AsyncMock(),
        initialize_paper_account=AsyncMock(),
        start_system_run=AsyncMock(return_value="system-run"),
        load_paper_ledger=AsyncMock(return_value=None),
        load_daily_equity_bounds=AsyncMock(return_value=None),
        load_wallet_analysis=AsyncMock(return_value=([], [])),
        load_active_candidates=AsyncMock(return_value=[]),
        load_candidate_score_totals=AsyncMock(return_value={}),
        load_processed_pool_creation_events=AsyncMock(return_value=[]),
        load_quarantined_event_protocols=AsyncMock(return_value=[]),
        load_processed_events_since=AsyncMock(return_value=[]),
        load_runtime_checkpoint=AsyncMock(
            return_value={"momentum_windows": {"position": 2}}
        ),
        load_unprocessed_events=AsyncMock(return_value=[]),
        load_protocol_checkpoints=AsyncMock(return_value={}),
        load_stream_checkpoint=AsyncMock(
            return_value=(7, "signature", datetime.now(tz=timezone.utc))
        ),
        stop_system_run=AsyncMock(),
        release_runtime_lease=AsyncMock(),
        persist_risk_state=AsyncMock(),
        run_retention=AsyncMock(),
        heartbeat_system_run=AsyncMock(),
        close=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_runtime_full_start_and_shutdown_lifecycle(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    database = _startup_database()
    runtime.database = database
    runtime.pipeline.start_background_workers = AsyncMock()
    runtime.pipeline.stop_background_workers = AsyncMock()
    runtime.stream_gateway.start = AsyncMock()
    runtime.stream_gateway.stop = AsyncMock()
    runtime._install_signal_controls = MagicMock()

    await runtime.start()

    assert runtime._started is True
    assert runtime.database_available is True
    assert runtime._system_run_id == "system-run"
    assert runtime._momentum_windows == {"position": 2}
    assert (tmp_path / "sniper.pid").exists()

    await runtime.shutdown()

    database.register_strategy.assert_awaited_once()
    database.initialize_paper_account.assert_awaited_once()
    database.stop_system_run.assert_awaited_once()
    database.close.assert_awaited_once()
    runtime.pipeline.start_background_workers.assert_awaited_once()
    runtime.pipeline.stop_background_workers.assert_awaited_once_with(
        timeout_seconds=120.0
    )
    runtime.stream_gateway.start.assert_awaited_once()
    runtime.stream_gateway.stop.assert_awaited_once()
    runtime.notifier.stop.assert_awaited_once()
    assert runtime._started is False
    assert not (tmp_path / "sniper.pid").exists()


@pytest.mark.asyncio
async def test_runtime_start_fails_closed_and_releases_lease(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    database = _startup_database()
    database.ping.side_effect = RuntimeError("database unavailable")
    runtime.database = database

    with pytest.raises(RuntimeError, match="database unavailable"):
        await runtime.start()

    assert runtime.database_available is False
    assert "database_unavailable" in runtime.entry_gate.reasons
    database.release_runtime_lease.assert_awaited_once()
    assert runtime._started is False


@pytest.mark.asyncio
async def test_runtime_health_readiness_and_risk_state(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    assert runtime.health_status() == "HALTED"
    assert await runtime.wait_until_ready(timeout_seconds=0) is False

    runtime.database_available = True
    for reason in tuple(runtime.entry_gate.reasons):
        runtime.entry_gate.unblock(reason)
    assert runtime.health_status() == "HEALTHY"
    assert await runtime.wait_until_ready(timeout_seconds=0.01) is True

    runtime.entry_gate.block("warmup")
    assert runtime.health_status() == "DEGRADED"
    runtime.entry_gate.unblock("warmup")
    runtime.entry_gate.block("protocol:pump")
    assert runtime.health_status() == "HALTED"
    runtime.entry_gate.unblock("protocol:pump")

    database = _startup_database()
    runtime.database = database
    runtime.halt("operator pause")
    await asyncio.gather(*runtime._persistence_tasks)
    assert runtime.resume() is True
    await asyncio.gather(*runtime._persistence_tasks)
    assert database.persist_risk_state.await_count == 2


async def _cancel_on_sleep(_seconds: float) -> None:
    raise asyncio.CancelledError


@pytest.mark.asyncio
async def test_runtime_periodic_loops_success_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    runtime.database = _startup_database()
    runtime.database_available = True
    runtime._system_run_id = "system-run"
    runtime.pipeline.evaluate_candidates = AsyncMock()
    runtime.evaluate_and_close_exits = AsyncMock(return_value=[])
    runtime.stream_gateway.refresh_freshness = MagicMock()
    runtime.quote_provider.get_sol_usd_price = AsyncMock(
        return_value=Decimal("150")
    )
    runtime.pipeline.pools.set_quote_price = MagicMock()
    runtime.retention.run = MagicMock(return_value=0)
    runtime.daily_report_if_not_sent = AsyncMock(return_value={})
    monkeypatch.setattr(runtime_module.asyncio, "sleep", _cancel_on_sleep)
    monkeypatch.setattr(runtime_module.asyncio, "to_thread", AsyncMock(return_value=0))

    loops = (
        runtime._candidate_loop,
        runtime._exit_loop,
        runtime._freshness_loop,
        runtime._quote_asset_loop,
        runtime._maintenance_loop,
        runtime._heartbeat_loop,
        runtime._daily_report_loop,
    )
    for loop in loops:
        with pytest.raises(asyncio.CancelledError):
            await loop()

    runtime.pipeline.evaluate_candidates.assert_awaited_once()
    runtime.evaluate_and_close_exits.assert_awaited_once()
    runtime.stream_gateway.refresh_freshness.assert_called_once()
    runtime.pipeline.pools.set_quote_price.assert_called_once()
    runtime.database.run_retention.assert_awaited_once()
    runtime.database.heartbeat_system_run.assert_awaited_once()
    runtime.daily_report_if_not_sent.assert_awaited_once()


@pytest.mark.asyncio
async def test_runtime_periodic_loops_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    runtime.pipeline.evaluate_candidates = AsyncMock(
        side_effect=RuntimeError("candidate failure")
    )
    runtime.evaluate_and_close_exits = AsyncMock(
        side_effect=RuntimeError("exit failure")
    )
    runtime.stream_gateway.refresh_freshness = MagicMock(
        side_effect=RuntimeError("stream failure")
    )
    runtime.quote_provider.get_sol_usd_price = AsyncMock(
        side_effect=RuntimeError("quote failure")
    )
    monkeypatch.setattr(runtime_module.asyncio, "sleep", _cancel_on_sleep)

    loops = (
        runtime._candidate_loop,
        runtime._exit_loop,
        runtime._freshness_loop,
        runtime._quote_asset_loop,
    )
    for loop in loops:
        with pytest.raises(asyncio.CancelledError):
            await loop()

    assert "security_data_unavailable" in runtime.entry_gate.reasons
    assert "exit_monitor_error" in runtime.entry_gate.reasons
    assert "stream_stale" in runtime.entry_gate.reasons
    assert "sol_price_unavailable" in runtime.entry_gate.reasons


@pytest.mark.asyncio
async def test_runtime_heartbeat_terminates_when_lease_is_lost(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    runtime.database = _startup_database()
    runtime.database.heartbeat_system_run.side_effect = ActiveRuntimeError(
        "lease lost"
    )
    runtime._system_run_id = "system-run"
    kill = MagicMock()
    monkeypatch.setattr(runtime_module.os, "kill", kill)

    await runtime._heartbeat_loop()

    assert "runtime_lease_lost" in runtime.entry_gate.reasons
    kill.assert_called_once()


@pytest.mark.asyncio
async def test_runtime_lifecycle_notifications_use_durable_outbox(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path, telegram_enabled=True)
    runtime.database = SimpleNamespace(enqueue_outbox=AsyncMock(return_value=True))
    runtime.database_available = True
    runtime._system_run_id = "system-run"

    await runtime._notify_lifecycle_alert("system_start", "started")
    await runtime._notify_lifecycle_alert("system_stop", "stopped")

    assert runtime.database.enqueue_outbox.await_count == 2
    assert runtime._lifecycle_start_sent is False
    runtime.notifier.send.assert_not_awaited()
    with pytest.raises(ValueError, match="unsupported"):
        await runtime._notify_lifecycle_alert("trade", "not allowed")


@pytest.mark.asyncio
async def test_runtime_notification_fallback_and_daily_report(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path, telegram_enabled=True)
    runtime.database = SimpleNamespace(
        enqueue_outbox=AsyncMock(side_effect=RuntimeError("outbox unavailable"))
    )
    runtime.database_available = True

    await runtime._notify_lifecycle_alert("system_start", "started")
    assert runtime._lifecycle_start_sent is True
    runtime.notifier.send.assert_awaited_once_with("started")

    runtime.notifier.send.reset_mock()
    report = runtime.build_daily_report("2026-09-03")
    assert await runtime._notify_daily_report(report) is True
    runtime.notifier.send.assert_awaited_once()
    runtime.notifier.send.side_effect = RuntimeError("telegram unavailable")
    assert await runtime._notify_daily_report(report) is False


@pytest.mark.asyncio
async def test_runtime_warmup_and_enrichment_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)

    async def no_delay(_seconds: float) -> None:
        return None

    monkeypatch.setattr(runtime_module.asyncio, "sleep", no_delay)
    runtime.entry_gate.block("warmup")
    await runtime._warmup()
    assert "warmup" not in runtime.entry_gate.reasons

    queue = SimpleNamespace(
        get=AsyncMock(side_effect=["mint", asyncio.CancelledError()]),
        task_done=MagicMock(),
    )
    runtime._enrichment_queue = queue
    runtime.enrichment.get_token = AsyncMock(
        return_value=SimpleNamespace(
            model_dump=MagicMock(return_value={"symbol": "T"}),
            observed_at=datetime.now(tz=timezone.utc),
        )
    )
    runtime.pipeline.tokens.apply_enrichment = MagicMock(return_value=None)

    with pytest.raises(asyncio.CancelledError):
        await runtime._enrichment_loop()

    queue.task_done.assert_called_once()
    runtime.enrichment.get_token.assert_awaited_once_with("mint")
