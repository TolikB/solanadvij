"""Entry point for the MVP app."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import uvicorn

from .api import build_api
from .config import AppConfig
from .database import Database
from .errors import LiveTradingNotImplementedError
from .logging_setup import configure_logging
from .replay import RawEventReplayRunner, ReplayRunStore, ReplaySpeed
from .runtime import SniperRuntime


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Solana confirmation sniper bot")
    parser.add_argument("--config", default=None, help="Path to yaml config file")
    parser.add_argument("--host", default="127.0.0.1", help="API host")
    parser.add_argument("--port", type=int, default=8080, help="API port")
    parser.add_argument("--replay-data", default=None, help="Raw event archive root")
    parser.add_argument(
        "--replay-speed", choices=[item.value for item in ReplaySpeed],
        default=ReplaySpeed.MAX.value,
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    try:
        if args.config:
            config_path = Path(args.config)
            if not config_path.exists():
                raise FileNotFoundError(f"Config file not found: {args.config}")
        config = AppConfig.load(args.config)
        configure_logging(
            level=config.logging.level,
            json_logs=config.logging.json_logs,
        )
        if args.replay_data and not config.replay_mode:
            replay_payload = config.model_dump(
                by_alias=True,
                exclude={"config_hash", "strategy_version"},
            )
            replay_payload["JUPITER_REPLAY_MODE"] = True
            config = AppConfig.model_validate(replay_payload)
    except LiveTradingNotImplementedError as exc:
        print("LIVE_TRADING_NOT_IMPLEMENTED")
        if str(exc):
            print(str(exc))
        sys.exit(78)
    except Exception as exc:
        raise SystemExit(str(exc)) from exc

    if args.replay_data:
        with TemporaryDirectory(prefix="sniper-replay-") as replay_data_dir:
            runtime = SniperRuntime(config, data_dir=replay_data_dir)
            result = asyncio.run(
                _run_raw_replay(runtime, config, args.replay_data, args.replay_speed)
            )
            print(json.dumps(result, sort_keys=True, default=str))
            return
    runtime = SniperRuntime(config)
    app = build_api(runtime)
    uvicorn.run(app, host=args.host, port=args.port, log_config=None)


async def _run_raw_replay(
    runtime: SniperRuntime, config: AppConfig, source: str, speed: str
) -> dict[str, object]:
    database = Database(config.postgres_dsn)
    try:
        await database.ping()
        await database.register_strategy(
            strategy_id=config.strategy_version,
            version=config.strategy_version,
            config_hash=config.config_hash,
            config_json=config.masked_view(),
            git_commit=config.release_revision or None,
            now=__import__("datetime").datetime.now(tz=__import__("datetime").timezone.utc),
        )
        runner = RawEventReplayRunner(
            runtime,
            store=ReplayRunStore(runtime.data_dir / "replay_runs.json"),
            database=database,
        )
        result = await runner.run(source, speed=ReplaySpeed(speed))
        return {
            "run_id": result.run_id,
            "input_hash": result.input_hash,
            "output_hash": result.output_hash,
            "events_executed": result.events_executed,
            "candidate_states": result.candidate_states,
            "final_reconcile": result.final_reconcile,
        }
    finally:
        await database.close()


if __name__ == "__main__":
    main()
