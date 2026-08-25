from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
import sys
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from sniper_bot.acceptance import (
    MAX_ARTIFACT_BYTES,
    REQUIRED_CI_GATES,
    REQUIRED_HARD_FILTER_PROOFS,
    REQUIRED_RUNTIME_CLAIMS,
    REQUIRED_TELEGRAM_EVENT_TYPES,
    RUNTIME_CLAIM_SOURCE_TYPES,
    CiAcceptanceArtifact,
    ClosedTrade,
    EquityPoint,
    OperationalCostEvidence,
    ProtocolPrecommitReceipt,
    RuntimeAcceptanceArtifact,
    StatisticalInputs,
    StatisticalProtocol,
    evaluate_statistical_stage,
    load_statistical_stage_data,
    verify_acceptance_evidence,
)
from sniper_bot.database import Database
from sniper_bot.db_models import (
    CandidateRow,
    OperationalCostRow,
    PaperAccountRow,
    PaperPositionRow,
    PoolRow,
    RawChainEventRow,
    StrategyVersionRow,
    SystemRunRow,
    TokenRow,
)

REVISION = "a" * 40
CONFIG_HASH = "b" * 64
OPERATIONAL_COST_RECEIPT = {
    "schema_version": 1,
    "account_id": "paper-main",
    "category": "vps",
    "amount_usd": "0.70",
    "incurred_at": "2026-07-02T00:00:00Z",
}
OPERATIONAL_COST_RECEIPT_BYTES = json.dumps(
    OPERATIONAL_COST_RECEIPT, sort_keys=True, separators=(",", ":")
).encode()
OPERATIONAL_COST_RECEIPT_SHA256 = hashlib.sha256(
    OPERATIONAL_COST_RECEIPT_BYTES
).hexdigest()


def _protocol() -> StatisticalProtocol:
    return StatisticalProtocol(
        schema_version=3,
        revision=REVISION,
        strategy_version_id="strategy-v1",
        config_hash=CONFIG_HASH,
        frozen_at=datetime(2026, 5, 31, tzinfo=timezone.utc),
        collection_started_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
        oos_started_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
        collection_ended_at=datetime(2026, 7, 7, tzinfo=timezone.utc),
        minimum_negative_launches=300,
        minimum_oos_trades=100,
        maximum_equity_mark_gap_seconds=3600,
        daily_operational_cost_usd=Decimal("0.10"),
        negative_launch_definition="distinct_rejected_pumpswap_pool",
    )


def _passing_inputs(protocol: StatisticalProtocol) -> StatisticalInputs:
    trades: list[ClosedTrade] = []
    for index in range(150):
        entry = protocol.oos_started_at - timedelta(hours=150 - index)
        trades.append(
            ClosedTrade(
                position_id=f"in-{index}",
                entry_time=entry,
                closed_at=entry + timedelta(minutes=30),
                pnl_usd=Decimal("0"),
                developer_cluster=f"cluster-{index % 10}",
                cluster_evaluated=True,
            )
        )
    for index in range(150):
        entry = protocol.oos_started_at + timedelta(minutes=index * 30)
        trades.append(
            ClosedTrade(
                position_id=f"oos-{index}",
                entry_time=entry,
                closed_at=entry + timedelta(minutes=30),
                pnl_usd=Decimal("2") if index % 3 else Decimal("-1"),
                developer_cluster=f"cluster-{index % 10}",
                cluster_evaluated=True,
            )
        )
    hours = int((protocol.collection_ended_at - protocol.oos_started_at).total_seconds() // 3600)
    equity_points = [
        EquityPoint(protocol.oos_started_at + timedelta(hours=index), Decimal("500") + Decimal(index % 7))
        for index in range(hours + 1)
    ]
    return StatisticalInputs(
        fixed_revision_strategy_cohort=True,
        discovered_pool_count=3000,
        materialized_pool_count=3000,
        missing_materialized_pool_count=0,
        missing_final_pool_outcome_count=0,
        negative_launch_count=400,
        censored_position_count=0,
        recorded_operational_cost_usd=Decimal("0.70"),
        starting_equities=[Decimal("500")],
        closed_trades=trades,
        equity_points=equity_points,
        operational_costs=[
            OperationalCostEvidence(
                account_id="paper-main",
                category="vps",
                amount_usd=Decimal("0.70"),
                incurred_at=datetime(2026, 7, 2, tzinfo=timezone.utc),
                source_reference_sha256=OPERATIONAL_COST_RECEIPT_SHA256,
            )
        ],
    )


def _passing_report(protocol: StatisticalProtocol, protocol_bytes: bytes):
    return evaluate_statistical_stage(
        inputs=_passing_inputs(protocol),
        protocol=protocol,
        protocol_sha256=hashlib.sha256(protocol_bytes).hexdigest(),
    )


def test_statistical_stage_uses_entry_time_frozen_cohort_marks_and_all_costs() -> None:
    protocol = _protocol()
    protocol_bytes = protocol.model_dump_json(indent=2).encode()
    inputs = _passing_inputs(protocol)
    crossing = ClosedTrade(
        position_id="crossing",
        entry_time=protocol.oos_started_at - timedelta(minutes=1),
        closed_at=protocol.oos_started_at + timedelta(minutes=1),
        pnl_usd=Decimal("100"),
        developer_cluster="cluster-crossing",
        cluster_evaluated=True,
    )
    report = evaluate_statistical_stage(
        inputs=replace(inputs, closed_trades=[*inputs.closed_trades, crossing]),
        protocol=protocol,
        protocol_sha256=hashlib.sha256(protocol_bytes).hexdigest(),
    )

    assert report.passed is True
    assert report.metrics.in_sample_trade_count == 151
    assert report.metrics.oos_trade_count == 150
    assert report.metrics.oos_operational_cost_usd == Decimal("0.70")
    assert report.metrics.oos_equity_mark_coverage is True


def test_statistical_stage_fails_for_missing_cluster_marks_and_retention() -> None:
    protocol = _protocol()
    inputs = _passing_inputs(protocol)
    bad_trade = inputs.closed_trades[-1]
    report = evaluate_statistical_stage(
        inputs=replace(
            inputs,
            fixed_revision_strategy_cohort=False,
            materialized_pool_count=2999,
            missing_materialized_pool_count=1,
            negative_launch_count=1,
            starting_equities=[],
            closed_trades=[
                *inputs.closed_trades[:-1],
                replace(bad_trade, developer_cluster=None, cluster_evaluated=False),
            ],
            equity_points=[],
        ),
        protocol=protocol,
        protocol_sha256="c" * 64,
    )
    failed = {criterion.name for criterion in report.criteria if not criterion.passed}

    assert report.passed is False
    assert {
        "fixed_revision_strategy_cohort",
        "pool_retention_complete",
        "minimum_negative_launches",
        "equity_mark_coverage",
        "cluster_attribution_complete",
    } <= failed


def test_statistical_stage_rejects_naive_database_timestamp() -> None:
    protocol = _protocol()
    inputs = _passing_inputs(protocol)
    first = inputs.closed_trades[0]
    with pytest.raises(ValueError, match="database timestamp must be timezone-aware"):
        evaluate_statistical_stage(
            inputs=replace(
                inputs,
                closed_trades=[
                    replace(first, entry_time=first.entry_time.replace(tzinfo=None)),
                    *inputs.closed_trades[1:],
                ],
            ),
            protocol=protocol,
            protocol_sha256="c" * 64,
        )


def _write_json(path, model) -> dict[str, str]:
    content = model.model_dump_json(indent=2).encode()
    path.write_bytes(content)
    return {"path": path.name, "sha256": hashlib.sha256(content).hexdigest()}


def _runtime_artifact(now: datetime, receipt_root=None) -> RuntimeAcceptanceArtifact:
    receipts = []
    for claim in sorted(REQUIRED_RUNTIME_CLAIMS):
        source_content = json.dumps({"claim": claim, "result": "pass"}, sort_keys=True).encode()
        source_path = f"receipts/{claim}.json"
        if receipt_root is not None:
            target = receipt_root / source_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(source_content)
        receipts.append(
            {
                "claim": claim,
                "source_type": RUNTIME_CLAIM_SOURCE_TYPES[claim],
                "source": {
                    "path": source_path,
                    "sha256": hashlib.sha256(source_content).hexdigest(),
                },
                "observed_at": now - timedelta(minutes=90),
            }
        )
    operational_receipt_path = "receipts/operational-cost-vps.json"
    if receipt_root is not None:
        target = receipt_root / operational_receipt_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(OPERATIONAL_COST_RECEIPT_BYTES)
    return RuntimeAcceptanceArtifact.model_validate(
        {
            "schema_version": 3,
            "revision": REVISION,
            "generated_at": now,
            "observation_started_at": now - timedelta(hours=2),
            "observation_ended_at": now - timedelta(hours=1),
            "claim_receipts": receipts,
            "operational_cost_receipts": [
                {
                    "path": operational_receipt_path,
                    "sha256": OPERATIONAL_COST_RECEIPT_SHA256,
                }
            ],
            "mainnet": {
                "record_mode_started": True,
                "paper_mode_started": True,
                "pump_events": 1,
                "pumpswap_events": 1,
                "analyzed_pools": 1,
                "duplicate_actions": 0,
                "live_transactions": 0,
                "wallet_credential_loads": 0,
                "transaction_submission_surface_absent": True,
                "hard_filter_proofs": sorted(REQUIRED_HARD_FILTER_PROOFS),
                "unsafe_filter_observations": len(REQUIRED_HARD_FILTER_PROOFS),
                "unsafe_filter_observations_blocked": len(REQUIRED_HARD_FILTER_PROOFS),
                "unsafe_filter_observations_allowed": 0,
                "holder_owner_aggregation_samples": 1,
                "onchain_liquidity_samples": 1,
                "jupiter_buy_quotes": 1,
                "jupiter_sell_quotes": 1,
                "exit_fills": 1,
                "exit_fills_with_executable_sell_quote": 1,
                "closed_positions": 1,
                "positions_with_round_trip_costs": 1,
                "positions_with_risk_sizing_evidence": 1,
                "starting_equity_usd": "500",
                "maximum_position_usd": "20",
                "maximum_exposure_usd": "50",
                "daily_loss_gate_observed": True,
                "drawdown_hard_halt_observed": True,
                "telegram_resume_bypass_blocked": True,
                "ledger_reconciliation_difference_usd": "0.01",
                "report_reconciliation_difference_usd": "0.01",
                "daily_report_generated": True,
                "all_time_endpoint_verified": True,
                "restart_duplicate_positions": 0,
                "deterministic_replay_hash_match": True,
                "audited_positions": 1,
                "positions_missing_audit_items": 0,
            },
            "telegram": {
                "delivered_event_types": sorted(REQUIRED_TELEGRAM_EVENT_TYPES),
                "lifecycle_alert_p95_seconds": "4.9",
                "unauthorized_access_blocked": True,
                "lifecycle_alert_after_database_commit": True,
                "outbox_redelivery_duplicates": 0,
                "outage_collection_continued": True,
                "undelivered_messages_retained": True,
            },
            "deployment": {
                "compose_started": True,
                "migrations_applied_automatically": True,
                "non_root_container": True,
                "api_bound_to_localhost": True,
                "secrets_absent_from_image": True,
                "minimum_postgres_role_verified": True,
                "ntp_synchronized": True,
                "post_reboot_started": True,
                "restart_duplicate_positions": 0,
                "daily_backup_created": True,
                "restore_verified_on_separate_database": True,
            },
            "performance": {
                "internal_event_p95_ms": "249.9",
                "feature_update_p95_ms": "99.9",
                "telegram_lifecycle_alert_p95_seconds": "4.9",
                "daily_report_max_seconds": "119.9",
            },
        }
    )


def test_bundle_verifier_parses_distinct_artifacts_expected_revision_and_freshness(tmp_path) -> None:
    now = datetime.now(tz=timezone.utc)
    protocol = _protocol()
    protocol_ref = _write_json(tmp_path / "protocol.json", protocol)
    protocol_bytes = (tmp_path / "protocol.json").read_bytes()
    precommit_ref = _write_json(
        tmp_path / "protocol-precommit.json",
        ProtocolPrecommitReceipt(
            schema_version=1,
            protocol_sha256=hashlib.sha256(protocol_bytes).hexdigest(),
            published_at=protocol.frozen_at,
            immutable_reference="object-lock://acceptance/protocol-v1",
        ),
    )
    report = _passing_report(protocol, protocol_bytes)
    report_ref = _write_json(tmp_path / "statistics.json", report)
    ci = CiAcceptanceArtifact(
        schema_version=2,
        revision=REVISION,
        generated_at=now,
        run_url="https://github.com/example/sniper/actions/runs/1",
        test_count=100,
        skipped_test_count=0,
        coverage_pct=Decimal("71"),
        gate_receipt_sha256={gate: hashlib.sha256(gate.encode()).hexdigest() for gate in REQUIRED_CI_GATES},
        ruff_passed=True,
        strict_mypy_passed=True,
        tests_passed=True,
        deterministic_replay_passed=True,
        postgres_previous_migration_passed=True,
        sqlite_previous_migration_passed=True,
        data_integrity_passed=True,
        no_live_audit_passed=True,
        internal_latency_passed=True,
        compose_config_passed=True,
        docker_startup_smoke_passed=True,
    )
    ci_ref = _write_json(tmp_path / "ci.json", ci)
    runtime = _runtime_artifact(now, tmp_path)
    runtime_ref = _write_json(tmp_path / "runtime.json", runtime)
    document = {
        "schema_version": 3,
        "revision": REVISION,
        "generated_at": now.isoformat(),
        "ci": ci_ref,
        "runtime": runtime_ref,
        "protocol_precommit_receipt": precommit_ref,
        "statistical_protocol": protocol_ref,
        "statistical_report": report_ref,
    }
    trusted = {
        "expected_ci_sha256": ci_ref["sha256"],
        "expected_runtime_sha256": runtime_ref["sha256"],
        "expected_precommit_receipt_sha256": precommit_ref["sha256"],
        "expected_protocol_sha256": protocol_ref["sha256"],
        "expected_report_sha256": report_ref["sha256"],
        "trusted_protocol_published_at": protocol.frozen_at,
    }

    verified = verify_acceptance_evidence(
        document,
        artifact_root=tmp_path,
        expected_revision=REVISION,
        max_evidence_age_hours=24,
        now=now,
        **trusted,
    )
    assert verified.revision == REVISION

    receipt_path = tmp_path / "receipts" / "operational-cost-vps.json"
    receipt_path.write_text("tampered", encoding="utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        verify_acceptance_evidence(
            document,
            artifact_root=tmp_path,
            expected_revision=REVISION,
            max_evidence_age_hours=24,
            now=now,
            **trusted,
        )
    receipt_path.write_bytes(OPERATIONAL_COST_RECEIPT_BYTES)

    mismatched_report = report.model_copy(deep=True)
    mismatched_report.operational_costs[0].amount_usd = Decimal("0.60")
    mismatched_report_ref = _write_json(tmp_path / "statistics.json", mismatched_report)
    with pytest.raises(ValueError, match="receipt total"):
        verify_acceptance_evidence(
            {**document, "statistical_report": mismatched_report_ref},
            artifact_root=tmp_path,
            expected_revision=REVISION,
            max_evidence_age_hours=24,
            now=now,
            **{**trusted, "expected_report_sha256": mismatched_report_ref["sha256"]},
        )
    assert _write_json(tmp_path / "statistics.json", report) == report_ref

    forged_cohort = report.model_copy(deep=True)
    forged_cohort.metrics.closed_trade_count += 1
    forged_cohort_ref = _write_json(tmp_path / "statistics.json", forged_cohort)
    with pytest.raises(ValueError, match="IS and OOS cohort counts"):
        verify_acceptance_evidence(
            {**document, "statistical_report": forged_cohort_ref},
            artifact_root=tmp_path,
            expected_revision=REVISION,
            max_evidence_age_hours=24,
            now=now,
            **{**trusted, "expected_report_sha256": forged_cohort_ref["sha256"]},
        )
    assert _write_json(tmp_path / "statistics.json", report) == report_ref

    impossible_economics = report.model_copy(deep=True)
    impossible_economics.metrics.oos_net_pnl_after_all_costs_usd += Decimal("1")
    impossible_ref = _write_json(tmp_path / "statistics.json", impossible_economics)
    with pytest.raises(ValueError, match="gross profit minus gross loss"):
        verify_acceptance_evidence(
            {**document, "statistical_report": impossible_ref},
            artifact_root=tmp_path,
            expected_revision=REVISION,
            max_evidence_age_hours=24,
            now=now,
            **{**trusted, "expected_report_sha256": impossible_ref["sha256"]},
        )
    assert _write_json(tmp_path / "statistics.json", report) == report_ref

    below_floor_report = report.model_copy(deep=True)
    below_floor_report.operational_costs[0].amount_usd = Decimal("0.60")
    below_floor_report.metrics.recorded_operational_cost_usd = Decimal("0.60")
    below_floor_report_ref = _write_json(tmp_path / "statistics.json", below_floor_report)
    with pytest.raises(ValueError, match="frozen cost floor"):
        verify_acceptance_evidence(
            {**document, "statistical_report": below_floor_report_ref},
            artifact_root=tmp_path,
            expected_revision=REVISION,
            max_evidence_age_hours=24,
            now=now,
            **{**trusted, "expected_report_sha256": below_floor_report_ref["sha256"]},
        )
    assert _write_json(tmp_path / "statistics.json", report) == report_ref

    stale_runtime_data = runtime.model_dump()
    stale_runtime_data["observation_started_at"] = now - timedelta(hours=30)
    stale_runtime_data["claim_receipts"][0]["observed_at"] = now - timedelta(hours=25)
    stale_runtime_ref = _write_json(
        tmp_path / "runtime.json",
        RuntimeAcceptanceArtifact.model_validate(stale_runtime_data),
    )
    with pytest.raises(ValueError, match="runtime receipt .* older"):
        verify_acceptance_evidence(
            {**document, "runtime": stale_runtime_ref},
            artifact_root=tmp_path,
            expected_revision=REVISION,
            max_evidence_age_hours=24,
            now=now,
            **{**trusted, "expected_runtime_sha256": stale_runtime_ref["sha256"]},
        )
    assert _write_json(tmp_path / "runtime.json", runtime) == runtime_ref

    bad_precommit_ref = _write_json(
        tmp_path / "protocol-precommit.json",
        ProtocolPrecommitReceipt(
            schema_version=1,
            protocol_sha256="c" * 64,
            published_at=protocol.frozen_at,
            immutable_reference="object-lock://acceptance/protocol-v1",
        ),
    )
    with pytest.raises(ValueError, match="does not reference"):
        verify_acceptance_evidence(
            {**document, "protocol_precommit_receipt": bad_precommit_ref},
            artifact_root=tmp_path,
            expected_revision=REVISION,
            max_evidence_age_hours=24,
            now=now,
            **{
                **trusted,
                "expected_precommit_receipt_sha256": bad_precommit_ref["sha256"],
            },
        )
    assert _write_json(
        tmp_path / "protocol-precommit.json",
        ProtocolPrecommitReceipt(
            schema_version=1,
            protocol_sha256=hashlib.sha256(protocol_bytes).hexdigest(),
            published_at=protocol.frozen_at,
            immutable_reference="object-lock://acceptance/protocol-v1",
        ),
    ) == precommit_ref

    original_report = report.model_copy(deep=True)
    report.criteria[0].actual = "misleading"
    bad_report_ref = _write_json(tmp_path / "statistics.json", report)
    with pytest.raises(ValueError, match="display values are inconsistent"):
        verify_acceptance_evidence(
            {**document, "statistical_report": bad_report_ref},
            artifact_root=tmp_path,
            expected_revision=REVISION,
            max_evidence_age_hours=24,
            now=now,
            **{**trusted, "expected_report_sha256": bad_report_ref["sha256"]},
        )
    report = original_report
    assert _write_json(tmp_path / "statistics.json", report) == report_ref

    with pytest.raises(ValueError, match="trusted expected revision"):
        verify_acceptance_evidence(
            document,
            artifact_root=tmp_path,
            expected_revision="d" * 40,
            max_evidence_age_hours=24,
            now=now,
            **trusted,
        )

    (tmp_path / "runtime.json").write_text("tampered", encoding="utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        verify_acceptance_evidence(
            document,
            artifact_root=tmp_path,
            expected_revision=REVISION,
            max_evidence_age_hours=24,
            now=now,
            **trusted,
        )


def test_bundle_verifier_rejects_secret_bearing_artifact_and_accepts_uppercase_hash(tmp_path) -> None:
    content = json.dumps({"bot_token": "1234567890:" + ("A" * 31)}).encode()
    path = tmp_path / "ci.json"
    path.write_bytes(content)
    document = {
        "schema_version": 3,
        "revision": REVISION,
        "generated_at": "2026-08-24T12:00:00Z",
        "ci": {"path": path.name, "sha256": hashlib.sha256(content).hexdigest().upper()},
        "runtime": {"path": "runtime.json", "sha256": "0" * 64},
        "protocol_precommit_receipt": {"path": "precommit.json", "sha256": "3" * 64},
        "statistical_protocol": {"path": "protocol.json", "sha256": "1" * 64},
        "statistical_report": {"path": "statistics.json", "sha256": "2" * 64},
    }
    with pytest.raises(ValueError, match="secret or private identifiers"):
        verify_acceptance_evidence(
            document,
            artifact_root=tmp_path,
            expected_revision=REVISION,
            max_evidence_age_hours=24,
            now=datetime(2026, 8, 24, 12, tzinfo=timezone.utc),
            expected_ci_sha256=hashlib.sha256(content).hexdigest(),
            expected_runtime_sha256="0" * 64,
            expected_precommit_receipt_sha256="3" * 64,
            expected_protocol_sha256="1" * 64,
            expected_report_sha256="2" * 64,
            trusted_protocol_published_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
        )


def test_statistical_stage_fails_closed_for_censoring_sparse_marks_and_cost_gap() -> None:
    protocol = _protocol()
    inputs = _passing_inputs(protocol)
    report = evaluate_statistical_stage(
        inputs=replace(
            inputs,
            censored_position_count=1,
            recorded_operational_cost_usd=Decimal("0"),
            equity_points=[inputs.equity_points[0], inputs.equity_points[-1]],
        ),
        protocol=protocol,
        protocol_sha256="c" * 64,
    )
    failed = {criterion.name for criterion in report.criteria if not criterion.passed}
    assert {"no_censored_positions", "equity_mark_coverage", "operational_costs_reconciled"} <= failed


def test_protocol_and_runtime_timestamps_are_fail_closed() -> None:
    protocol = _protocol()
    with pytest.raises(ValueError, match="frozen before collection"):
        StatisticalProtocol.model_validate(
            {**protocol.model_dump(), "frozen_at": protocol.collection_started_at}
        )
    runtime = _runtime_artifact(datetime.now(tz=timezone.utc))
    with pytest.raises(ValueError, match="runtime evidence timestamps"):
        RuntimeAcceptanceArtifact.model_validate(
            {
                **runtime.model_dump(),
                "observation_ended_at": runtime.generated_at + timedelta(hours=1),
            }
        )
    wrong_source = runtime.model_dump()
    wrong_source["claim_receipts"][0]["source_type"] = "ci"
    with pytest.raises(ValueError, match="wrong source type"):
        RuntimeAcceptanceArtifact.model_validate(wrong_source)


def test_statistical_evaluator_rejects_trade_outside_frozen_interval() -> None:
    protocol = _protocol()
    inputs = _passing_inputs(protocol)
    outside = replace(
        inputs.closed_trades[-1],
        entry_time=protocol.collection_ended_at + timedelta(seconds=1),
        closed_at=protocol.collection_ended_at + timedelta(seconds=2),
    )
    with pytest.raises(ValueError, match="entry falls outside"):
        evaluate_statistical_stage(
            inputs=replace(inputs, closed_trades=[*inputs.closed_trades[:-1], outside]),
            protocol=protocol,
            protocol_sha256="c" * 64,
        )


def test_bundle_verifier_rejects_untrusted_hash_and_oversized_artifact(tmp_path) -> None:
    oversized = b"x" * (MAX_ARTIFACT_BYTES + 1)
    path = tmp_path / "ci.json"
    path.write_bytes(oversized)
    digest = hashlib.sha256(oversized).hexdigest()
    document = {
        "schema_version": 3,
        "revision": REVISION,
        "generated_at": "2026-08-24T12:00:00Z",
        "ci": {"path": path.name, "sha256": digest},
        "runtime": {"path": "runtime.json", "sha256": "0" * 64},
        "protocol_precommit_receipt": {"path": "precommit.json", "sha256": "3" * 64},
        "statistical_protocol": {"path": "protocol.json", "sha256": "1" * 64},
        "statistical_report": {"path": "statistics.json", "sha256": "2" * 64},
    }
    common = {
        "artifact_root": tmp_path,
        "expected_revision": REVISION,
        "max_evidence_age_hours": 24,
        "expected_runtime_sha256": "0" * 64,
        "expected_precommit_receipt_sha256": "3" * 64,
        "expected_protocol_sha256": "1" * 64,
        "expected_report_sha256": "2" * 64,
        "trusted_protocol_published_at": datetime(2026, 8, 1, tzinfo=timezone.utc),
        "now": datetime(2026, 8, 24, 12, tzinfo=timezone.utc),
    }
    with pytest.raises(ValueError, match="independently trusted"):
        verify_acceptance_evidence(document, expected_ci_sha256="9" * 64, **common)
    with pytest.raises(ValueError, match="exceeds"):
        verify_acceptance_evidence(document, expected_ci_sha256=digest, **common)


def test_ci_writer_requires_revision_bound_gate_receipts(tmp_path) -> None:
    junit = tmp_path / "junit.xml"
    coverage = tmp_path / "coverage.xml"
    gates = tmp_path / "gates"
    output = tmp_path / "ci.json"
    junit.write_text(
        '<testsuites><testsuite tests="1" failures="0" errors="0" skipped="0" /></testsuites>',
        encoding="utf-8",
    )
    coverage.write_text('<coverage line-rate="0.75" />', encoding="utf-8")
    gates.mkdir()
    for gate in REQUIRED_CI_GATES:
        (gates / f"{gate}.ok").write_text(f"{REVISION} {gate}\n", encoding="utf-8")
    env = {
        **os.environ,
        "GITHUB_ACTIONS": "true",
        "GITHUB_SHA": REVISION,
        "GITHUB_SERVER_URL": "https://github.example.com",
        "GITHUB_REPOSITORY": "example/sniper",
        "GITHUB_RUN_ID": "123",
    }
    command = [
        sys.executable,
        "scripts/write_ci_acceptance.py",
        "--junit",
        str(junit),
        "--coverage",
        str(coverage),
        "--gates-dir",
        str(gates),
        "--output",
        str(output),
    ]
    result = subprocess.run(command, capture_output=True, text=True, env=env, check=False)
    assert result.returncode == 0, result.stderr
    artifact = CiAcceptanceArtifact.model_validate_json(output.read_bytes())
    assert len(set(artifact.gate_receipt_sha256.values())) == len(REQUIRED_CI_GATES)

    (gates / "ruff.ok").write_text(f"{'d' * 40} ruff\n", encoding="utf-8")
    result = subprocess.run(command, capture_output=True, text=True, env=env, check=False)
    assert result.returncode != 0
    assert "does not match the workflow revision" in result.stderr

    (gates / "ruff.ok").write_text(f"{REVISION} ruff\n", encoding="utf-8")
    junit.write_text(
        '<testsuites><testsuite tests="1" failures="0" errors="0" skipped="1" /></testsuites>',
        encoding="utf-8",
    )
    result = subprocess.run(command, capture_output=True, text=True, env=env, check=False)
    assert result.returncode != 0
    assert "does not describe a passing test suite" in result.stderr


@pytest.mark.asyncio
async def test_statistical_loader_fails_closed_on_config_cutoff_censoring_and_period_costs(tmp_path) -> None:
    protocol = _protocol()
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'acceptance-loader.db'}")
    await database.create_schema_for_tests()
    try:
        async with database.sessions.begin() as session:
            session.add(
                StrategyVersionRow(
                    id=protocol.strategy_version_id,
                    version="v1",
                    config_hash=protocol.config_hash,
                    config_json={},
                    git_commit=protocol.revision,
                    created_at=protocol.frozen_at,
                    activated_at=protocol.frozen_at,
                    deactivated_at=None,
                )
            )
            session.add(
                TokenRow(
                    mint="mint-1",
                    token_program="spl",
                    name=None,
                    symbol=None,
                    decimals=6,
                    total_supply_raw=Decimal("1000"),
                    creator_address=None,
                    creation_signature=None,
                    creation_slot=None,
                    creation_time=protocol.collection_started_at,
                    metadata_uri=None,
                    metadata_mutable=None,
                    enrichment_json={},
                    enriched_at=None,
                    first_seen_at=protocol.collection_started_at,
                    updated_at=protocol.collection_started_at,
                )
            )
            session.add(
                PoolRow(
                    pool_address="pool-1",
                    mint="mint-1",
                    protocol="pumpswap",
                    quote_mint="SOL",
                    base_vault=None,
                    quote_vault=None,
                    creation_signature="sig-pool",
                    creation_slot=1,
                    creation_time=protocol.collection_started_at,
                    migration_signature=None,
                    status="ACTIVE",
                    updated_at=protocol.collection_started_at,
                )
            )
            session.add(
                RawChainEventRow(
                    id="raw-1",
                    block_date=date(2026, 6, 1),
                    event_id="event-1",
                    ingest_sequence=1,
                    source="helius",
                    protocol="pumpswap",
                    event_type="pool_created",
                    slot=1,
                    signature="sig-raw",
                    instruction_index=0,
                    inner_instruction_index=0,
                    block_time=protocol.collection_started_at,
                    observed_at=protocol.collection_started_at,
                    commitment="confirmed",
                    mint="mint-1",
                    pool_address="pool-1",
                    payload_json={},
                    created_at=protocol.collection_started_at,
                )
            )
            session.add_all(
                [
                    CandidateRow(
                        id="candidate-wrong-config",
                        mint="mint-1",
                        pool_address="pool-1",
                        state="REJECTED",
                        detected_at=protocol.collection_started_at,
                        eligible_at=None,
                        armed_at=None,
                        expired_at=None,
                        rejected_at=protocol.collection_started_at + timedelta(minutes=1),
                        reject_reason="FILTER",
                        strategy_version_id=protocol.strategy_version_id,
                        config_hash="9" * 64,
                        runtime_state_json=None,
                    ),
                    CandidateRow(
                        id="candidate-after-cutoff",
                        mint="mint-1",
                        pool_address="pool-1",
                        state="REJECTED",
                        detected_at=protocol.collection_started_at,
                        eligible_at=None,
                        armed_at=None,
                        expired_at=None,
                        rejected_at=protocol.collection_ended_at + timedelta(seconds=1),
                        reject_reason="FILTER",
                        strategy_version_id=protocol.strategy_version_id,
                        config_hash=protocol.config_hash,
                        runtime_state_json=None,
                    ),
                ]
            )
            session.add(
                PaperAccountRow(
                    id="paper-main",
                    base_currency="USDC",
                    starting_equity=Decimal("500"),
                    cash_balance=Decimal("500"),
                    locked_capital=Decimal("0"),
                    realized_pnl=Decimal("0"),
                    unrealized_pnl=Decimal("0"),
                    simulated_costs=Decimal("0"),
                    operational_costs=Decimal("200.70"),
                    equity=Decimal("299.30"),
                    peak_equity=Decimal("500"),
                    drawdown_pct=Decimal("40.14"),
                    halt_reason=None,
                    pause_until=None,
                    daily_halt_date=None,
                    updated_at=protocol.collection_ended_at,
                )
            )
            session.add_all(
                [
                    OperationalCostRow(
                        id="cost-before",
                        account_id="paper-main",
                        category="vps",
                        amount_usd=Decimal("100"),
                        incurred_at=protocol.oos_started_at - timedelta(seconds=1),
                        source_reference_sha256="1" * 64,
                        created_at=protocol.collection_ended_at,
                    ),
                    OperationalCostRow(
                        id="cost-oos",
                        account_id="paper-main",
                        category="vps",
                        amount_usd=Decimal("0.70"),
                        incurred_at=protocol.oos_started_at,
                        source_reference_sha256="2" * 64,
                        created_at=protocol.collection_ended_at,
                    ),
                    OperationalCostRow(
                        id="cost-after",
                        account_id="paper-main",
                        category="vps",
                        amount_usd=Decimal("100"),
                        incurred_at=protocol.collection_ended_at + timedelta(seconds=1),
                        source_reference_sha256="3" * 64,
                        created_at=protocol.collection_ended_at,
                    ),
                ]
            )
            session.add(
                PaperPositionRow(
                    id="position-open",
                    mint="mint-1",
                    pool_address="pool-1",
                    status="OPEN",
                    strategy_version_id=protocol.strategy_version_id,
                    entry_time=protocol.oos_started_at,
                    closed_at=None,
                    initial_cost_usd=Decimal("10"),
                    remaining_cost_usd=Decimal("10"),
                    initial_token_amount_raw=Decimal("1"),
                    token_amount_raw=Decimal("1"),
                    open_fill_id="missing-fk-disabled-in-sqlite",
                    realized_pnl=Decimal("0"),
                    unrealized_pnl=Decimal("0"),
                    mfe_pct=Decimal("0"),
                    mae_pct=Decimal("0"),
                    highest_executable_value=Decimal("10"),
                    lowest_executable_value=Decimal("10"),
                    tp1_taken=False,
                    tp2_taken=False,
                    last_new_high_at=None,
                    config_hash=protocol.config_hash,
                    exit_reason=None,
                )
            )
        inputs = await load_statistical_stage_data(database, protocol)
        assert inputs.fixed_revision_strategy_cohort is True
        assert inputs.discovered_pool_count == 1
        assert inputs.materialized_pool_count == 1
        assert inputs.negative_launch_count == 0
        assert inputs.missing_final_pool_outcome_count == 1
        assert inputs.censored_position_count == 1
        assert inputs.recorded_operational_cost_usd == Decimal("0.70")
        assert len(inputs.operational_costs) == 1
        assert inputs.operational_costs[0].source_reference_sha256 == "2" * 64
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_operational_cost_ledger_is_idempotent_and_updates_equity(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'operational-cost.db'}")
    await database.create_schema_for_tests()
    now = datetime.now(tz=timezone.utc)
    try:
        await database.initialize_paper_account(
            account_id="paper-main",
            starting_equity=Decimal("500"),
            now=now,
        )
        with pytest.raises(ValueError, match="SHA-256"):
            await database.record_operational_cost(
                cost_id="invalid-cost",
                account_id="paper-main",
                category="vps",
                amount_usd=Decimal("1"),
                incurred_at=now,
                source_reference_sha256="z" * 64,
                recorded_at=now,
            )
        inserted, duplicate = await asyncio.gather(
            database.record_operational_cost(
                cost_id="cost-1",
                account_id="paper-main",
                category="vps",
                amount_usd=Decimal("12.50"),
                incurred_at=now,
                source_reference_sha256="a" * 64,
                recorded_at=now,
            ),
            database.record_operational_cost(
                cost_id="cost-2",
                account_id="paper-main",
                category="vps",
                amount_usd=Decimal("12.50"),
                incurred_at=now,
                source_reference_sha256="a" * 64,
                recorded_at=now,
            ),
        )
        async with database.sessions() as session:
            account = await session.get(PaperAccountRow, "paper-main")
        assert sorted((inserted, duplicate)) == [False, True]
        assert account is not None
        assert account.operational_costs == Decimal("12.50")
        assert account.cash_balance == Decimal("487.50")
        assert account.equity == Decimal("487.50")
        stale_run_id = "stale-run"
        async with database.sessions.begin() as session:
            session.add(
                SystemRunRow(
                    id=stale_run_id,
                    started_at=now - timedelta(minutes=10),
                    stopped_at=None,
                    mode="paper",
                    strategy_version_id="runtime-strategy",
                    hostname="previous",
                    app_version="test",
                    stop_reason=None,
                    last_heartbeat_at=now - timedelta(minutes=10),
                )
            )
        run_id = await database.start_system_run(
            mode="paper",
            strategy_version_id="runtime-strategy",
            hostname="test",
            app_version="test",
            now=now,
            account_id="paper-main",
        )
        assert run_id != stale_run_id
        with pytest.raises(RuntimeError, match="active system run"):
            await database.start_system_run(
                mode="paper",
                strategy_version_id="runtime-strategy",
                hostname="test",
                app_version="test",
                now=now,
                account_id="paper-main",
            )
        with pytest.raises(RuntimeError, match="stop the bot"):
            await database.record_operational_cost(
                cost_id="cost-3",
                account_id="paper-main",
                category="vps",
                amount_usd=Decimal("1"),
                incurred_at=now,
                source_reference_sha256="b" * 64,
                recorded_at=now,
            )
        non_owner = Database(database.dsn)
        try:
            with pytest.raises(RuntimeError, match="owned by another runtime"):
                await non_owner.persist_risk_state(
                    account_id="paper-main",
                    halt_reason="test",
                    pause_until=None,
                    daily_halt_date=None,
                    updated_at=now,
                )
        finally:
            await non_owner.close()
        await database.stop_system_run(run_id, reason="test", now=now)
        start_result, cost_result = await asyncio.gather(
            database.start_system_run(
                mode="paper",
                strategy_version_id="runtime-strategy",
                hostname="test",
                app_version="test",
                now=now + timedelta(seconds=1),
                account_id="paper-main",
            ),
            database.record_operational_cost(
                cost_id="cost-race",
                account_id="paper-main",
                category="vps",
                amount_usd=Decimal("1"),
                incurred_at=now,
                source_reference_sha256="c" * 64,
                recorded_at=now,
            ),
            return_exceptions=True,
        )
        assert isinstance(start_result, str)
        assert cost_result is True or (
            isinstance(cost_result, RuntimeError) and "stop the bot" in str(cost_result)
        )
        await database.stop_system_run(start_result, reason="test", now=now)
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_strategy_revision_rows_are_immutable(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'strategy-revision.db'}")
    await database.create_schema_for_tests()
    now = datetime.now(tz=timezone.utc)
    try:
        await database.register_strategy(
            strategy_id="strategy-a",
            version="strategy-a",
            config_hash=CONFIG_HASH,
            config_json={},
            git_commit=REVISION,
            now=now,
        )
        with pytest.raises(ValueError, match="immutable"):
            await database.register_strategy(
                strategy_id="strategy-a",
                version="strategy-a",
                config_hash=CONFIG_HASH,
                config_json={},
                git_commit="d" * 40,
                now=now,
            )
    finally:
        await database.close()
