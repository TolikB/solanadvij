"""Read-only FastAPI surface and allowlisted Telegram command routing."""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, FastAPI, HTTPException, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST


def build_api(runtime: Any) -> FastAPI:
    router = APIRouter()

    def mode_value() -> str:
        return str(getattr(runtime.config.app_mode, "value", runtime.config.app_mode))

    def health_payload() -> dict[str, Any]:
        status = runtime.health_status() if hasattr(runtime, "health_status") else "HEALTHY"
        return {
            "ok": status == "HEALTHY",
            "status": status,
            "mode": mode_value(),
            "strategy_version": runtime.config.strategy_version,
            "config_hash": runtime.config.config_hash,
            "utc": datetime.now(tz=timezone.utc).isoformat(),
            "trade_halted": runtime.ledger.state.is_halted,
        }

    def require_dict(value: Any, source: str) -> dict[str, Any]:
        if not isinstance(value, dict):
            raise RuntimeError(f"{source} must return an object")
        return value

    @router.get("/health")
    @router.get("/health/live")
    async def health_live() -> dict[str, Any]:
        payload = health_payload()
        payload["ok"] = True
        return payload

    @router.get("/health/ready")
    async def health_ready() -> Response:
        payload = health_payload()
        return JSONResponse(payload, status_code=200 if payload["ok"] else 503)

    @router.get("/metrics")
    async def metrics() -> Response:
        if hasattr(runtime, "_sync_paper_metrics"):
            runtime._sync_paper_metrics()
        body = runtime.metrics.render() if hasattr(runtime, "metrics") else b""
        return Response(content=body, media_type=CONTENT_TYPE_LATEST)

    @router.get("/api/v1/status")
    async def status() -> dict[str, Any]:
        if not hasattr(runtime, "system_status"):
            return health_payload()
        return require_dict(runtime.system_status(), "system_status")

    @router.get("/state")
    @router.get("/api/v1/account")
    async def account() -> dict[str, Any]:
        return require_dict(runtime.ledger.snapshot(), "ledger.snapshot")

    def positions(*, open_only: bool) -> list[dict[str, Any]]:
        values = list(runtime.ledger.state.positions.values())
        selected = [item for item in values if (item.status.value == "OPEN") is open_only]
        return [item.model_dump(mode="json") for item in selected]

    @router.get("/api/v1/positions/open")
    async def positions_open() -> list[dict[str, Any]]:
        return positions(open_only=True)

    @router.get("/api/v1/positions/closed")
    async def positions_closed() -> list[dict[str, Any]]:
        return positions(open_only=False)

    @router.get("/api/v1/candidates")
    async def candidates() -> list[dict[str, Any]]:
        pipeline = getattr(runtime, "pipeline", None)
        return [item.model_dump(mode="json") for item in pipeline.list_candidates()] if pipeline else []

    @router.get("/api/v1/rejections")
    async def rejections() -> list[dict[str, Any]]:
        pipeline = getattr(runtime, "pipeline", None)
        return [item.model_dump(mode="json") for item in pipeline.list_rejections()] if pipeline else []

    @router.get("/today")
    @router.get("/api/v1/reports/today")
    async def today() -> dict[str, Any]:
        return await report_for_day(coerce_day(None))

    @router.get("/all")
    @router.get("/api/v1/reports/all-time")
    async def all_time() -> dict[str, Any]:
        return await report_all_time()

    def coerce_day(date: str | None) -> str:
        if not date:
            return datetime.now(ZoneInfo(runtime.config.time_zone)).strftime("%Y-%m-%d")
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail="invalid date format, expected YYYY-MM-DD"
            ) from exc
        return date

    async def report_for_day(target_date: str) -> dict[str, Any]:
        database = getattr(runtime, "database", None)
        if database is not None and getattr(runtime, "database_available", False):
            stored = await database.load_daily_report(
                target_date,
                timezone_name=runtime.config.time_zone,
                strategy_version=runtime.config.strategy_version,
            )
            if stored is not None:
                return require_dict(stored, "load_daily_report")
            bounds = await database.load_daily_equity_bounds(
                account_id="paper-main",
                report_date=target_date,
                timezone_name=runtime.config.time_zone,
            )
            if bounds is not None and hasattr(runtime, "report_builder"):
                return require_dict(
                    runtime.report_builder.daily(
                        target_date, capital_bounds=bounds
                    ),
                    "build_daily_report",
                )
            if target_date < coerce_day(None):
                raise HTTPException(
                    status_code=404,
                    detail="historical daily equity snapshot is unavailable",
                )
        return require_dict(runtime.build_daily_report(target_date), "build_daily_report")

    async def report_all_time() -> dict[str, Any]:
        if hasattr(runtime, "build_all_time_report_with_history"):
            return require_dict(
                await runtime.build_all_time_report_with_history(),
                "build_all_time_report",
            )
        return require_dict(runtime.build_all_time_report(), "build_all_time_report")

    @router.get("/day")
    async def day_alias(date: str | None = None) -> dict[str, Any]:
        target_date = coerce_day(date)
        return await report_for_day(target_date)

    @router.get("/api/v1/reports/day/{date}")
    async def day(date: str) -> dict[str, Any]:
        target_date = coerce_day(date)
        return await report_for_day(target_date)

    @router.get("/mode")
    async def mode() -> dict[str, Any]:
        return {
            "app_mode": mode_value(),
            "strategy_version": runtime.config.strategy_version,
            "config_hash": runtime.config.config_hash,
            "starting_equity_usd": str(runtime.config.starting_equity_usd),
            "now": datetime.now(tz=timezone.utc).isoformat(),
        }

    async def on_startup() -> None:
        if getattr(runtime.config, "replay_mode", False):
            return
        notifier = getattr(runtime, "notifier", None)
        telegram_enabled = getattr(
            getattr(runtime.config, "telegram", None),
            "enabled",
            True,
        )
        if notifier is not None and telegram_enabled:
            try:
                await notifier.start(start_polling=True, command_handler=handle_command)
            except Exception:
                logging.getLogger(__name__).exception("Telegram startup failed")
        if hasattr(runtime, "start"):
            await runtime.start()
        ready = True
        if hasattr(runtime, "wait_until_ready"):
            ready = await runtime.wait_until_ready(
                timeout_seconds=120.0
            )
        if not ready:
            raise RuntimeError(
                "paper runtime did not become ready; "
                "lifecycle start suppressed"
            )
        if hasattr(runtime, "_notify_lifecycle_alert"):
            await runtime._notify_lifecycle_alert(
                "system_start", "Бот запущено."
            )
        elif notifier is not None:
            try:
                await notifier.send("Бот запущено.")
            except Exception:
                logging.getLogger(__name__).exception("Telegram startup alert failed")

    async def on_shutdown() -> None:
        if getattr(runtime.config, "replay_mode", False):
            return
        notifier = getattr(runtime, "notifier", None)
        manages_lifecycle = bool(
            getattr(
                runtime,
                "manages_lifecycle_notifications",
                False,
            )
        )
        if (
            not manages_lifecycle
            and hasattr(runtime, "_notify_lifecycle_alert")
        ):
            await runtime._notify_lifecycle_alert(
                "system_stop", "Бот зупинено."
            )
        elif not manages_lifecycle and notifier is not None:
            try:
                await notifier.send("Бот зупинено.")
            except Exception:
                logging.getLogger(__name__).exception("Telegram shutdown alert failed")

        if hasattr(runtime, "shutdown"):
            await runtime.shutdown()
        elif notifier is not None:
            await notifier.stop()

    async def handle_command(chat_id: int, command: str, args: list[str]) -> None:
        notifier = runtime.notifier
        cmd = command.lower()
        if cmd == "status":
            payload: Any = {
                "today": await report_for_day(coerce_day(None)),
                "all_time": await report_all_time(),
                "system": runtime.system_status(),
            }
        elif cmd == "today":
            payload = await report_for_day(coerce_day(None))
        elif cmd == "all":
            payload = await report_all_time()
        elif cmd == "open":
            payload = positions(open_only=True)
        elif cmd == "last":
            payload = positions(open_only=False)[-5:]
        elif cmd == "health":
            payload = health_payload()
        elif cmd == "config":
            payload = runtime.config.masked_view()
        elif cmd == "rejections":
            payload = await rejections()
        elif cmd == "day" and args:
            try:
                payload = await report_for_day(coerce_day(args[0]))
            except HTTPException as exc:
                payload = {"error": exc.detail}
        elif cmd in {"pause", "halt"}:
            runtime.halt(" ".join(args) if args else "manual pause")
            payload = {"paused": True}
        elif cmd == "resume":
            payload = {"resumed": runtime.resume()}
        else:
            payload = {"error": "unknown command"}
        await notifier.send_to(chat_id, json.dumps(payload, default=str, sort_keys=True))

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        await on_startup()
        try:
            yield
        finally:
            await on_shutdown()

    app = FastAPI(title="Solana Confirmation Sniper Bot", lifespan=lifespan)
    app.include_router(router)
    return app
