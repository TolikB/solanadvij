"""Freeze the statistical protocol of one collection window before it starts.

The window is start -> OOS boundary -> end, fixed before collection and never
extended afterwards. The bot has to run with the same end date (``COLLECTION``
in ``.env``) because the date is part of the frozen config hash and closes
entries early enough for every pool and position to finish inside the window.

1. Print the ``COLLECTION`` line for ``.env``::

       python scripts/freeze_statistical_protocol.py collection-env \\
           --collection-start 2026-10-01T00:00:00Z

2. With that line in ``.env``, freeze the protocol inside the release image so
   the config hash is exactly the one the bot registers::

       docker compose -p solanadvij --env-file .env run --rm --no-deps -T \\
           sniper-bot python scripts/freeze_statistical_protocol.py freeze \\
           --collection-start 2026-10-01T00:00:00Z > statistical-protocol.json

3. Publish that exact file immutably before the start, then record where::

       python scripts/freeze_statistical_protocol.py receipt \\
           --protocol statistical-protocol.json \\
           --published-at 2026-09-30T21:10:00Z \\
           --reference https://github.com/OWNER/REPO/issues/1 > protocol-precommit.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from sniper_bot.acceptance import (
    MAX_ARTIFACT_BYTES,
    ProtocolPrecommitReceipt,
    StatisticalProtocol,
)
from sniper_bot.config import AppConfig, AppMode, CollectionConfig

DEFAULT_COLLECTION_DAYS = 30
DEFAULT_OOS_DAY = 15
# Project policy floors for the frozen sample; the gate additionally requires
# 3000 discovered pools and 300 closed trades over the whole window.
DEFAULT_MINIMUM_NEGATIVE_LAUNCHES = 300
DEFAULT_MINIMUM_OOS_TRADES = 100
# The equity path may go unobserved at most as long as a position may live
# (exits.maximum_holding_seconds): a restart or reboot within that bound keeps
# coverage, an outage that leaves a position unmanaged past it does not.
MINIMUM_EQUITY_MARK_GAP_SECONDS = 120  # two flat-account heartbeats
MAXIMUM_EQUITY_MARK_GAP_SECONDS = 3600
COHORT_CONFIG_NAME = "default.yaml"
# Enough time to publish the frozen file before the first counted pool.
MINIMUM_PUBLICATION_LEAD = timedelta(minutes=10)


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("timestamps must include a UTC offset")
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _window(start: datetime, days: int, oos_day: int) -> tuple[datetime, datetime]:
    if days <= 0 or not 0 < oos_day < days:
        raise ValueError("the OOS boundary must fall strictly inside the window")
    return start + timedelta(days=oos_day), start + timedelta(days=days)


def collection_env_line(start: datetime, days: int) -> str:
    ends_at = start + timedelta(days=days)
    cutoff = CollectionConfig().entry_cutoff_seconds
    payload = {"ends_at": _iso(ends_at), "entry_cutoff_seconds": cutoff}
    return "COLLECTION=" + json.dumps(payload, separators=(",", ":"))


def build_protocol(
    config: AppConfig,
    *,
    start: datetime,
    days: int,
    oos_day: int,
    frozen_at: datetime,
    minimum_negative_launches: int = DEFAULT_MINIMUM_NEGATIVE_LAUNCHES,
    minimum_oos_trades: int = DEFAULT_MINIMUM_OOS_TRADES,
    maximum_equity_mark_gap_seconds: int | None = None,
) -> StatisticalProtocol:
    oos_started_at, ends_at = _window(start, days, oos_day)
    gap = (
        config.exits.maximum_holding_seconds
        if maximum_equity_mark_gap_seconds is None
        else maximum_equity_mark_gap_seconds
    )
    if not MINIMUM_EQUITY_MARK_GAP_SECONDS <= gap <= MAXIMUM_EQUITY_MARK_GAP_SECONDS:
        raise ValueError(
            "the equity mark gap must be between "
            f"{MINIMUM_EQUITY_MARK_GAP_SECONDS} and {MAXIMUM_EQUITY_MARK_GAP_SECONDS} seconds"
        )
    if config.app_mode != AppMode.PAPER:
        raise ValueError("the statistical cohort must run in APP_MODE=paper")
    if not config.release_revision:
        raise ValueError("APP_REVISION must name the deployed 40-character commit")
    if config.collection.ends_at != ends_at:
        raise ValueError(
            "the bot's collection window does not end with this protocol; put "
            f"{collection_env_line(start, days)} in .env and rerun"
        )
    if start - frozen_at < MINIMUM_PUBLICATION_LEAD:
        raise ValueError(
            "the protocol must be frozen at least "
            f"{int(MINIMUM_PUBLICATION_LEAD.total_seconds() // 60)} minutes before "
            "collection starts, to publish it first"
        )
    monthly = Decimal(str(config.reporting.monthly_infrastructure_cost_usd))
    if not config.reporting.include_operational_costs or monthly <= 0:
        raise ValueError(
            "reporting.monthly_infrastructure_cost_usd must be a positive "
            "included cost before the config hash is frozen"
        )
    return StatisticalProtocol(
        schema_version=3,
        revision=config.release_revision,
        strategy_version_id=config.strategy_version,
        config_hash=config.config_hash,
        frozen_at=frozen_at,
        collection_started_at=start,
        oos_started_at=oos_started_at,
        collection_ended_at=ends_at,
        minimum_negative_launches=minimum_negative_launches,
        minimum_oos_trades=minimum_oos_trades,
        maximum_equity_mark_gap_seconds=gap,
        # Same daily allocation the reports use for infrastructure costs.
        daily_operational_cost_usd=monthly / Decimal("30"),
        negative_launch_definition="distinct_rejected_pumpswap_pool",
        time_zone=config.time_zone,
    )


def render(model: StatisticalProtocol | ProtocolPrecommitReceipt) -> bytes:
    return (model.model_dump_json(indent=2) + "\n").encode("utf-8")


def build_receipt(
    protocol_bytes: bytes, *, published_at: datetime, reference: str
) -> ProtocolPrecommitReceipt:
    protocol = StatisticalProtocol.model_validate_json(protocol_bytes)
    if not protocol.frozen_at <= published_at < protocol.collection_started_at:
        raise ValueError(
            "the protocol must be published after it was frozen and before "
            "collection starts"
        )
    return ProtocolPrecommitReceipt(
        schema_version=1,
        protocol_sha256=hashlib.sha256(protocol_bytes).hexdigest(),
        published_at=published_at,
        immutable_reference=reference,
    )


def _emit(content: bytes, output: str | None) -> None:
    if output is None:
        sys.stdout.buffer.write(content)
        sys.stdout.flush()
        return
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)

    env = commands.add_parser("collection-env", help="print the COLLECTION .env line")
    env.add_argument("--collection-start", type=_timestamp, required=True)
    env.add_argument("--days", type=int, default=DEFAULT_COLLECTION_DAYS)

    freeze = commands.add_parser("freeze", help="write the frozen protocol JSON")
    freeze.add_argument("--collection-start", type=_timestamp, required=True)
    freeze.add_argument("--days", type=int, default=DEFAULT_COLLECTION_DAYS)
    freeze.add_argument("--oos-day", type=int, default=DEFAULT_OOS_DAY)
    freeze.add_argument(
        "--config", default=os.environ.get("CONFIG_PATH", "configs/default.yaml")
    )
    freeze.add_argument(
        "--max-equity-gap",
        type=int,
        help="seconds; default exits.maximum_holding_seconds",
    )
    freeze.add_argument("--output")

    receipt = commands.add_parser("receipt", help="write the publication receipt JSON")
    receipt.add_argument("--protocol", required=True)
    receipt.add_argument("--published-at", type=_timestamp, required=True)
    receipt.add_argument("--reference", required=True)
    receipt.add_argument("--output")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "collection-env":
        if args.days <= 0:
            raise ValueError("--days must be positive")
        print(collection_env_line(args.collection_start, args.days))
        return 0
    if args.command == "freeze":
        if Path(args.config).name != COHORT_CONFIG_NAME:
            raise ValueError(
                f"the statistical cohort runs on configs/{COHORT_CONFIG_NAME}, not {args.config}"
            )
        config = AppConfig.load(args.config)
        protocol = build_protocol(
            config,
            start=args.collection_start,
            days=args.days,
            oos_day=args.oos_day,
            frozen_at=datetime.now(tz=timezone.utc),
            maximum_equity_mark_gap_seconds=args.max_equity_gap,
        )
        content = render(protocol)
        _emit(content, args.output)
        print(
            f"protocol_sha256={hashlib.sha256(content).hexdigest()} "
            f"strategy_version_id={protocol.strategy_version_id} "
            f"collection={_iso(protocol.collection_started_at)}"
            f"..{_iso(protocol.oos_started_at)}..{_iso(protocol.collection_ended_at)}",
            file=sys.stderr,
        )
        return 0
    protocol_path = Path(args.protocol)
    protocol_bytes = protocol_path.read_bytes()
    if len(protocol_bytes) > MAX_ARTIFACT_BYTES:
        raise ValueError("protocol exceeds the acceptance artifact limit")
    _emit(
        render(
            build_receipt(
                protocol_bytes,
                published_at=args.published_at,
                reference=args.reference,
            )
        ),
        args.output,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
