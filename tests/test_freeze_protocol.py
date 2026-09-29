from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from scripts.freeze_statistical_protocol import (
    build_protocol,
    build_receipt,
    collection_env_line,
    main,
    render,
)
from sniper_bot.acceptance import ProtocolPrecommitReceipt, StatisticalProtocol
from sniper_bot.config import AppConfig

REVISION = "0123456789abcdef0123456789abcdef01234567"
START = datetime(2026, 10, 1, 0, 0, tzinfo=timezone.utc)
FROZEN = START - timedelta(hours=1)


def _config(**overrides: Any) -> AppConfig:
    values: dict[str, Any] = {
        "APP_MODE": "paper",
        "APP_REVISION": REVISION,
        "HELIUS_API_KEY": "helius-key",
        "JUPITER_API_KEY": "jupiter-key",
        "POSTGRES_DSN": "postgresql://user:pass@localhost:5432/db",
        "TELEGRAM_BOT_TOKEN": "telegram-token",
        "TELEGRAM_ADMIN_CHAT_ID": 123456,
        "reporting": {"monthly_infrastructure_cost_usd": "15"},
        "collection": {"ends_at": (START + timedelta(days=30)).isoformat()},
    }
    values.update(overrides)
    return AppConfig(**values)


def test_collection_env_line_matches_the_config_the_bot_loads() -> None:
    line = collection_env_line(START, 30)

    assert line == 'COLLECTION={"ends_at":"2026-10-31T00:00:00Z","entry_cutoff_seconds":1800}'
    loaded = _config(collection=json.loads(line.removeprefix("COLLECTION=")))
    assert loaded.collection.ends_at == START + timedelta(days=30)


def test_protocol_freezes_three_dates_cost_and_the_exact_cohort() -> None:
    config = _config()

    protocol = build_protocol(config, start=START, days=30, oos_day=15, frozen_at=FROZEN)

    assert protocol.revision == REVISION
    assert protocol.strategy_version_id == config.strategy_version
    assert protocol.strategy_version_id.endswith(REVISION[:12])
    assert protocol.config_hash == config.config_hash
    assert protocol.collection_started_at == START
    assert protocol.oos_started_at == START + timedelta(days=15)
    assert protocol.collection_ended_at == START + timedelta(days=30)
    assert protocol.daily_operational_cost_usd == Decimal("0.5")
    assert protocol.minimum_negative_launches == 300
    assert protocol.minimum_oos_trades == 100
    assert protocol.maximum_equity_mark_gap_seconds == 600
    assert protocol.time_zone == "Europe/Kyiv"
    assert StatisticalProtocol.model_validate_json(render(protocol)) == protocol


@pytest.mark.parametrize(
    ("overrides", "kwargs", "message"),
    [
        ({"collection": {"ends_at": None}}, {}, "COLLECTION="),
        ({"APP_MODE": "record"}, {}, "APP_MODE=paper"),
        ({"APP_REVISION": ""}, {}, "APP_REVISION"),
        ({"reporting": {"monthly_infrastructure_cost_usd": "0"}}, {}, "monthly_infrastructure_cost_usd"),
        ({}, {"frozen_at": START - timedelta(minutes=5)}, "before collection starts"),
        ({}, {"oos_day": 30}, "OOS boundary"),
        ({}, {"maximum_equity_mark_gap_seconds": 60}, "equity mark gap"),
        ({}, {"maximum_equity_mark_gap_seconds": 7200}, "equity mark gap"),
    ],
)
def test_protocol_refuses_an_unsound_freeze(
    overrides: dict[str, Any], kwargs: dict[str, Any], message: str
) -> None:
    arguments: dict[str, Any] = {"start": START, "days": 30, "oos_day": 15, "frozen_at": FROZEN}
    arguments.update(kwargs)
    with pytest.raises(ValueError, match=message):
        build_protocol(_config(**overrides), **arguments)


def test_receipt_binds_the_published_bytes_before_collection() -> None:
    content = render(build_protocol(_config(), start=START, days=30, oos_day=15, frozen_at=FROZEN))
    published_at = FROZEN + timedelta(minutes=20)

    receipt = build_receipt(
        content, published_at=published_at, reference="https://github.com/o/r/issues/1"
    )

    assert receipt.protocol_sha256 == hashlib.sha256(content).hexdigest()
    assert ProtocolPrecommitReceipt.model_validate_json(render(receipt)) == receipt
    with pytest.raises(ValueError, match="before collection starts"):
        build_receipt(content, published_at=START, reference="https://github.com/o/r/issues/1")


def test_freeze_command_writes_the_protocol_from_the_deployment_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    start = datetime.now(tz=timezone.utc).replace(microsecond=0) + timedelta(hours=2)
    for name, value in {
        "APP_MODE": "paper",
        "APP_REVISION": REVISION,
        "HELIUS_API_KEY": "helius-key",
        "JUPITER_API_KEY": "jupiter-key",
        "POSTGRES_DSN": "postgresql://user:pass@localhost:5432/db",
        "TELEGRAM_BOT_TOKEN": "telegram-token",
        "TELEGRAM_ADMIN_CHAT_ID": "123456",
        "COLLECTION": collection_env_line(start, 30).removeprefix("COLLECTION="),
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("APP_REVISION_FILE", str(tmp_path / "missing-revision"))
    output = tmp_path / "protocol.json"

    assert main(
        [
            "freeze",
            "--collection-start",
            start.isoformat(),
            "--config",
            "configs/default.yaml",
            "--output",
            str(output),
        ]
    ) == 0

    protocol = StatisticalProtocol.model_validate_json(output.read_bytes())
    assert protocol.collection_ended_at == start + timedelta(days=30)
    assert protocol.daily_operational_cost_usd == Decimal("0.5")
    assert protocol.config_hash == AppConfig.load("configs/default.yaml").config_hash


def test_collection_progress_reports_the_sample_against_its_targets() -> None:
    from scripts.collection_progress import summarize
    from sniper_bot.acceptance import ClosedTrade, EquityPoint, StatisticalInputs

    protocol = build_protocol(_config(), start=START, days=30, oos_day=15, frozen_at=FROZEN)
    trades = [
        ClosedTrade(
            position_id=f"p{index}",
            entry_time=START + timedelta(days=day),
            closed_at=START + timedelta(days=day, minutes=5),
            pnl_usd=Decimal("1"),
            developer_cluster=None,
            cluster_evaluated=False,
        )
        for index, day in enumerate((1, 2, 16))
    ]
    inputs = StatisticalInputs(
        fixed_revision_strategy_cohort=True,
        discovered_pool_count=1200,
        materialized_pool_count=1200,
        missing_materialized_pool_count=0,
        missing_final_pool_outcome_count=2,
        negative_launch_count=1195,
        censored_position_count=1,
        recorded_operational_cost_usd=Decimal("0"),
        starting_equities=[Decimal("500")],
        closed_trades=trades,
        equity_points=[EquityPoint(START + timedelta(days=16), Decimal("501"))],
    )

    summary = summarize(inputs, protocol, START + timedelta(days=16))

    assert summary["phase"] == "out_of_sample"
    assert summary["closed_trades"] == 3
    assert summary["signal_trades"] == 3
    assert summary["in_sample_signal_trades"] == 2
    assert summary["oos_signal_trades"] == 1
    assert summary["discovered_pumpswap_pools_target"] == 3000
    assert summary["signal_trades_target"] == 300
    assert summary["pools_without_outcome_yet"] == 2
    assert summary["oos_equity_marks"] == 1


def test_equity_gap_follows_the_holding_limit_unless_overridden() -> None:
    config = _config()
    assert config.exits.maximum_holding_seconds == 600

    override = build_protocol(
        config,
        start=START,
        days=30,
        oos_day=15,
        frozen_at=FROZEN,
        maximum_equity_mark_gap_seconds=900,
    )

    assert override.maximum_equity_mark_gap_seconds == 900


def test_freeze_refuses_the_calibration_config() -> None:
    with pytest.raises(ValueError, match="configs/default.yaml"):
        main(
            [
                "freeze",
                "--collection-start",
                (datetime.now(tz=timezone.utc) + timedelta(hours=2)).isoformat(),
                "--config",
                "configs/calibration.yaml",
            ]
        )
