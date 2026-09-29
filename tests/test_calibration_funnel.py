from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
import yaml  # type: ignore[import-untyped]

from scripts.calibration_funnel import LADDER, PassedCandidate, Rung, analyze, decide
from sniper_bot.candidates import Candidate, CandidateState
from sniper_bot.database import Database
from sniper_bot.db_models import MarketSnapshotRow
from sniper_bot.events import ChainEventType, EventEnvelope, EventSource, Protocol
from sniper_bot.registry import WSOL_MINT, PoolRecord, TokenRecord
from sniper_bot.security import RejectReason

T0 = datetime(2026, 10, 1, tzinfo=timezone.utc)
CALIBRATION = Rung("calibration", Decimal("20000"), 20)


def _passing(liquidity: int, buyers: int, count: int) -> list[PassedCandidate]:
    return [
        PassedCandidate(str(uuid4()), Decimal(liquidity), buyers) for _ in range(count)
    ]


def test_specified_strategy_is_kept_when_it_already_trades_enough() -> None:
    result = decide(
        _passing(45_000, 30, 13), days=Decimal(1), calibration=CALIBRATION, window_long_enough=True
    )

    assert result["decision"] == "A"
    assert result["config_changes"] == {"risk.max_trades_per_day": 24}


def test_first_rung_reaching_the_target_is_chosen_in_ladder_order() -> None:
    passed = _passing(45_000, 30, 5) + _passing(25_000, 30, 8) + _passing(25_000, 21, 40)

    result = decide(passed, days=Decimal(1), calibration=CALIBRATION, window_long_enough=True)

    assert result["decision"] == "B"
    assert result["rung"] == "R2"
    assert result["config_changes"] == {
        "risk.max_trades_per_day": 24,
        "liquidity.min_quote_liquidity_usd": 20000,
        "flow.min_unique_buyers_60s": 25,
    }
    assert [row["entries"] for row in result["ladder"]] == [5, 5, 13, 53]


def test_no_rung_reaching_the_target_keeps_the_specified_strategy() -> None:
    result = decide(
        _passing(25_000, 21, 12), days=Decimal(1), calibration=CALIBRATION, window_long_enough=True
    )

    assert result["decision"] == "C"
    assert result["config_changes"] == {}


def test_short_window_and_too_strict_calibration_refuse_to_decide() -> None:
    short = decide(
        _passing(45_000, 30, 50), days=Decimal(1), calibration=CALIBRATION, window_long_enough=False
    )
    assert short["decision"] is None

    strict = decide(
        _passing(45_000, 30, 50),
        days=Decimal(1),
        calibration=Rung("strict", Decimal("30000"), 25),
        window_long_enough=True,
    )
    assert [row["measurable"] for row in strict["ladder"]] == [True, True, False, False]
    assert strict["decision"] == "A"


def test_ladder_starts_at_the_specified_strategy_and_ends_at_calibration() -> None:
    root = Path(__file__).resolve().parents[1]
    default = yaml.safe_load((root / "configs/default.yaml").read_text(encoding="utf-8"))
    calibration = yaml.safe_load((root / "configs/calibration.yaml").read_text(encoding="utf-8"))

    assert (
        Decimal(str(default["liquidity"]["min_quote_liquidity_usd"])),
        default["flow"]["min_unique_buyers_60s"],
    ) == (LADDER[0].min_quote_liquidity_usd, LADDER[0].min_unique_buyers_60s)
    assert (
        Decimal(str(calibration["liquidity"]["min_quote_liquidity_usd"])),
        calibration["flow"]["min_unique_buyers_60s"],
    ) == (LADDER[-1].min_quote_liquidity_usd, LADDER[-1].min_unique_buyers_60s)
    differing = {
        (section, key)
        for section, values in default.items()
        if isinstance(values, dict)
        for key, value in values.items()
        if calibration[section][key] != value
    }
    top_level = {key for key, value in default.items() if calibration.get(key) != value}
    assert differing == {
        ("liquidity", "min_quote_liquidity_usd"),
        ("flow", "min_unique_buyers_60s"),
    }
    assert top_level == {"liquidity", "flow"}


async def _candidate(
    database: Database,
    name: str,
    *,
    reason: RejectReason,
    liquidity: list[int],
    buyers: int,
) -> None:
    mint, pool = f"mint-{name}", f"pool-{name}"
    detected = T0 + timedelta(hours=1)
    entry = detected + timedelta(seconds=45 + len(liquidity))
    await database.upsert_token(TokenRecord(mint=mint, updated_at=detected))
    await database.upsert_pool(
        PoolRecord(
            pool_address=pool,
            base_mint=mint,
            quote_mint=WSOL_MINT,
            creation_signature=f"sig-{name}",
            creation_slot=1,
            creation_time=detected,
            base_decimals=6,
            quote_decimals=9,
            updated_at=detected,
        )
    )
    await database.record_events(
        [
            EventEnvelope(
                source=EventSource.HELIUS_WSS,
                protocol=Protocol.PUMPSWAP,
                event_type=ChainEventType.POOL_CREATED,
                slot=1,
                signature=f"create-{name}",
                instruction_index=1,
                block_time=detected,
                observed_at=detected,
                mint=mint,
                pool_address=pool,
                payload={},
            )
        ]
    )
    await database.upsert_candidate(
        Candidate(
            candidate_id=name,
            mint=mint,
            pool_address=pool,
            state=CandidateState.REJECTED,
            detected_at=detected,
            updated_at=entry,
            rejected_at=entry,
            reject_reason=reason,
            strategy_version="calibration-v1",
            config_hash="hash",
        ),
        "calibration-v1",
    )
    snapshots = [(detected + timedelta(seconds=10), 1_000, 0)] + [
        (detected + timedelta(seconds=45 + index), value, buyers)
        for index, value in enumerate(liquidity)
    ]
    async with database.sessions.begin() as session:
        for at, value, unique_buyers in snapshots:
            session.add(
                MarketSnapshotRow(
                    id=str(uuid4()),
                    snapshot_date=at.date(),
                    pool_address=pool,
                    snapshot_time=at,
                    price_usd=Decimal("1"),
                    quote_liquidity_usd=Decimal(value),
                    base_liquidity_usd=Decimal(value),
                    market_cap_estimate=None,
                    volume_15s=Decimal("0"),
                    volume_30s=Decimal("0"),
                    volume_60s=Decimal("0"),
                    unique_buyers_60s=unique_buyers,
                    unique_sellers_60s=0,
                    holder_count=0,
                    features_json={
                        "quote_liquidity_usd": str(value),
                        "unique_buyers_60s": unique_buyers,
                    },
                    data_quality_flags=[],
                )
            )


@pytest.mark.asyncio
async def test_analyze_replays_each_rung_over_the_observed_ticks(tmp_path: Path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'funnel.db'}")
    await database.create_schema_for_tests()
    await database.register_strategy(
        strategy_id="calibration-v1",
        version="calibration-v1",
        config_hash="hash",
        config_json={
            "liquidity": {"min_quote_liquidity_usd": "20000"},
            "flow": {"min_unique_buyers_60s": 20},
            "candidate": {"min_observation_seconds": 45},
        },
        now=T0,
    )
    await _candidate(
        database, "strong", reason=RejectReason.RISK_MANAGER_BLOCKED,
        liquidity=[45_000, 42_000], buyers=30,
    )
    await _candidate(
        database, "mid", reason=RejectReason.RISK_MANAGER_BLOCKED,
        liquidity=[35_000, 31_000], buyers=26,
    )
    await _candidate(
        database, "thin", reason=RejectReason.RISK_MANAGER_BLOCKED,
        liquidity=[25_000], buyers=22,
    )
    await _candidate(
        database, "dip", reason=RejectReason.RISK_MANAGER_BLOCKED,
        liquidity=[45_000, 19_000, 45_000], buyers=40,
    )
    await _candidate(
        database, "rejected", reason=RejectReason.LOW_QUOTE_LIQUIDITY,
        liquidity=[5_000], buyers=3,
    )
    await _candidate(
        database, "baseline", reason=RejectReason.STREAM_NOT_TRADABLE,
        liquidity=[], buyers=0,
    )

    report = await analyze(database, since=T0, until=T0 + timedelta(days=1))

    assert report["strategy_version_id"] == "calibration-v1"
    assert report["discovered_pumpswap_pools"] == 6
    assert report["candidates"] == 6
    assert report["live_candidates"] == 5
    assert report["passed_every_entry_rule"] == 4
    assert [row["entries"] for row in report["ladder"]] == [1, 2, 2, 3]
    assert report["decision"] == "C"
    await database.close()
