"""Fail-closed statistical evaluation and structural acceptance-bundle verification."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import Any, Literal, Self
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import desc, select

from .database import Database
from .db_models import (
    CandidateRow,
    OperationalCostRow,
    PaperAccountRow,
    PaperEquityMarkRow,
    PaperPositionRow,
    PoolRow,
    RawChainEventRow,
    StrategyVersionRow,
    TokenRow,
    WalletProfileRow,
)

MAX_ARTIFACT_BYTES = 10 * 1024 * 1024
MAX_STATISTICAL_EQUITY_MARKS = 100_000
REQUIRED_CI_GATES = frozenset(
    {
        "compose_config",
        "data_integrity",
        "deterministic_replay",
        "docker_startup",
        "internal_latency",
        "no_live_audit",
        "postgres_previous_migration",
        "postgres_hardening",
        "ruff",
        "sqlite_previous_migration",
        "strict_mypy",
        "tests",
    }
)
REQUIRED_RUNTIME_CLAIMS = frozenset(
    {
        "all_time_drawdown_gate",
        "all_time_report",
        "audit_trail",
        "backup_restore",
        "daily_loss_gate",
        "daily_report",
        "deployment_security",
        "deterministic_replay",
        "duplicate_idempotency",
        "executable_exit_quotes",
        "holder_owner_aggregation",
        "jupiter_buy_sell_quotes",
        "ledger_reconciliation",
        "mainnet_ingestion",
        "no_live_submission",
        "onchain_liquidity",
        "paper_equity_and_limits",
        "record_and_paper_modes",
        "restart_recovery",
        "risk_position_sizing",
        "round_trip_costs",
        "security_filters",
        "telegram_delivery",
        "telegram_outage",
        "timing_nfrs",
    }
)
RUNTIME_CLAIM_SOURCE_TYPES = {
    "all_time_drawdown_gate": "postgres_query",
    "all_time_report": "postgres_query",
    "audit_trail": "postgres_query",
    "backup_restore": "host_command",
    "daily_loss_gate": "postgres_query",
    "daily_report": "postgres_query",
    "deployment_security": "host_command",
    "deterministic_replay": "ci",
    "duplicate_idempotency": "postgres_query",
    "executable_exit_quotes": "postgres_query",
    "holder_owner_aggregation": "rpc_capture",
    "jupiter_buy_sell_quotes": "rpc_capture",
    "ledger_reconciliation": "postgres_query",
    "mainnet_ingestion": "rpc_capture",
    "no_live_submission": "ci",
    "onchain_liquidity": "rpc_capture",
    "paper_equity_and_limits": "postgres_query",
    "record_and_paper_modes": "host_command",
    "restart_recovery": "host_command",
    "risk_position_sizing": "postgres_query",
    "round_trip_costs": "postgres_query",
    "security_filters": "rpc_capture",
    "telegram_delivery": "telegram_receipt",
    "telegram_outage": "telegram_receipt",
    "timing_nfrs": "benchmark",
}
REQUIRED_TELEGRAM_EVENT_TYPES = frozenset(
    {"system_start", "system_stop", "daily_report"}
)
REQUIRED_HARD_FILTER_PROOFS = frozenset(
    {
        "TOKEN_2022_REJECTED",
        "NONSTANDARD_TOKEN_PROGRAM_REJECTED",
        "MINT_AUTHORITY_REJECTED",
        "FREEZE_AUTHORITY_REJECTED",
        "TOTAL_SUPPLY_REJECTED",
        "INVALID_DECIMALS_REJECTED",
        "HOLDER_CONCENTRATION_REJECTED",
        "DEV_HOLDING_REJECTED",
        "DEV_CLUSTER_REJECTED",
        "RELATED_CLUSTER_REJECTED",
        "UNKNOWN_SUPPLY_REJECTED",
        "MINIMUM_LIQUIDITY_REJECTED",
        "BUY_ROUTE_REJECTED",
        "SELL_ROUTE_REJECTED",
        "ROUND_TRIP_LOSS_REJECTED",
        "BUY_PRICE_IMPACT_REJECTED",
        "SELL_PRICE_IMPACT_REJECTED",
        "EXTERNAL_SELLERS_REJECTED",
        "DEV_SELL_REJECTED",
        "STALE_HOLDER_DATA_REJECTED",
        "STALE_STREAM_DATA_REJECTED",
        "STALE_QUOTE_REJECTED",
        "POOL_AGE_REJECTED",
        "LIQUIDITY_DECLINE_REJECTED",
        "UNKNOWN_PROTOCOL_LAYOUT_REJECTED",
        "CRITICAL_API_UNAVAILABLE_REJECTED",
        "UNSUPPORTED_QUOTE_MINT_REJECTED",
    }
)
REQUIRED_STATISTICAL_CRITERIA = frozenset(
    {
        "fixed_revision_strategy_cohort",
        "single_paper_account",
        "starting_equity_500",
        "pool_retention_complete",
        "all_discovered_pools_evaluated",
        "minimum_new_pools",
        "minimum_closed_trades",
        "minimum_negative_launches",
        "minimum_oos_trades",
        "no_censored_positions",
        "separate_oos_period",
        "equity_mark_coverage",
        "operational_costs_reconciled",
        "oos_net_pnl_after_all_costs_positive",
        "profit_factor",
        "max_drawdown",
        "positive_expectancy",
        "single_trade_concentration",
        "day_independence",
        "cluster_attribution_complete",
        "developer_cluster_independence",
    }
)
_SENSITIVE_FIELD_ALTERNATION = "|".join(
    ("api_key", "bot_token", "password", "private" + "_" + "key", "secret", "chat_id", "user_id")
)
_SECRET_PATTERNS = (
    re.compile(r"\b\d{6,}:[A-Za-z0-9_-]{20,}\b"),
    re.compile(r"postgres(?:ql)?(?:\+asyncpg)?://[^\s:/]+:[^\s@]+@", re.IGNORECASE),
    re.compile(r"api-key=(?!REDACTED)[^\s&\"']+", re.IGNORECASE),
    re.compile(r'"(?:' + _SENSITIVE_FIELD_ALTERNATION + r')"\s*:', re.IGNORECASE),
    re.compile(r"\bauthorization\s*:\s*bearer\s+\S+", re.IGNORECASE),
    re.compile(r"\bx-api-key\s*:\s*(?!REDACTED)\S+", re.IGNORECASE),
    re.compile(
        r"\b(?:HELIUS|JUPITER|TELEGRAM|SOLANA)[A-Z0-9_]*(?:KEY|TOKEN|SECRET)\s*=\s*(?!REDACTED)\S+",
        re.IGNORECASE,
    ),
)


def _require_aware(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    return _require_aware(value, "database timestamp")


class ArtifactEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    path: str = Field(min_length=1)
    sha256: str

    @field_validator("path")
    @classmethod
    def require_safe_relative_path(cls, value: str) -> str:
        normalized = value.replace("\\", "/")
        parsed = PurePosixPath(normalized)
        if parsed.is_absolute() or ".." in parsed.parts or re.match(r"^[A-Za-z]:", normalized):
            raise ValueError("artifact path must stay relative to the evidence root")
        return normalized

    @field_validator("sha256")
    @classmethod
    def normalize_sha256(cls, value: str) -> str:
        normalized = value.lower()
        if re.fullmatch(r"[0-9a-f]{64}", normalized) is None:
            raise ValueError("sha256 must contain exactly 64 hexadecimal characters")
        return normalized


class OperationalCostEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account_id: str = Field(min_length=1)
    category: str = Field(min_length=1, max_length=64)
    amount_usd: Decimal = Field(gt=0)
    incurred_at: datetime
    source_reference_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("incurred_at")
    @classmethod
    def require_incurred_at_aware(cls, value: datetime) -> datetime:
        return _require_aware(value, "operational cost incurred_at")


class OperationalCostReceiptArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1]
    account_id: str = Field(min_length=1)
    category: str = Field(min_length=1, max_length=64)
    amount_usd: Decimal = Field(gt=0)
    incurred_at: datetime

    @field_validator("incurred_at")
    @classmethod
    def require_incurred_at_aware(cls, value: datetime) -> datetime:
        return _require_aware(value, "operational cost receipt incurred_at")


class AcceptanceManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[3]
    revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    generated_at: datetime
    ci: ArtifactEvidence
    runtime: ArtifactEvidence
    protocol_precommit_receipt: ArtifactEvidence
    statistical_protocol: ArtifactEvidence
    statistical_report: ArtifactEvidence

    @field_validator("generated_at")
    @classmethod
    def require_generated_at_aware(cls, value: datetime) -> datetime:
        return _require_aware(value, "generated_at")

    @model_validator(mode="after")
    def require_distinct_artifacts(self) -> Self:
        paths = [
            self.ci.path,
            self.runtime.path,
            self.protocol_precommit_receipt.path,
            self.statistical_protocol.path,
            self.statistical_report.path,
        ]
        if len(set(paths)) != len(paths):
            raise ValueError("acceptance sections must use distinct artifacts")
        return self


class CiAcceptanceArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[2]
    revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    generated_at: datetime
    run_url: str = Field(
        pattern=r"^https://[A-Za-z0-9.-]+(?::[0-9]+)?/[^/]+/[^/]+/actions/runs/[0-9]+$"
    )
    test_count: int = Field(ge=1)
    skipped_test_count: Literal[0]
    coverage_pct: Decimal = Field(ge=70, le=100)
    gate_receipt_sha256: dict[str, str]
    ruff_passed: Literal[True]
    strict_mypy_passed: Literal[True]
    tests_passed: Literal[True]
    deterministic_replay_passed: Literal[True]
    postgres_previous_migration_passed: Literal[True]
    sqlite_previous_migration_passed: Literal[True]
    data_integrity_passed: Literal[True]
    no_live_audit_passed: Literal[True]
    internal_latency_passed: Literal[True]
    compose_config_passed: Literal[True]
    docker_startup_smoke_passed: Literal[True]

    @field_validator("generated_at")
    @classmethod
    def require_generated_at_aware(cls, value: datetime) -> datetime:
        return _require_aware(value, "CI generated_at")

    @model_validator(mode="after")
    def require_gate_receipts(self) -> Self:
        if set(self.gate_receipt_sha256) != REQUIRED_CI_GATES:
            raise ValueError("CI gate receipts are missing or unexpected")
        for digest in self.gate_receipt_sha256.values():
            if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                raise ValueError("CI gate receipt hashes must be lowercase SHA-256")
        return self


class MainnetRuntimeEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    record_mode_started: Literal[True]
    paper_mode_started: Literal[True]
    pump_events: int = Field(ge=1)
    pumpswap_events: int = Field(ge=1)
    analyzed_pools: int = Field(ge=1)
    duplicate_actions: int = Field(ge=0, le=0)
    live_transactions: int = Field(ge=0, le=0)
    wallet_credential_loads: int = Field(ge=0, le=0)
    transaction_submission_surface_absent: Literal[True]
    hard_filter_proofs: set[str]
    unsafe_filter_observations: int = Field(ge=1)
    unsafe_filter_observations_blocked: int = Field(ge=1)
    unsafe_filter_observations_allowed: int = Field(ge=0, le=0)
    holder_owner_aggregation_samples: int = Field(ge=1)
    onchain_liquidity_samples: int = Field(ge=1)
    jupiter_buy_quotes: int = Field(ge=1)
    jupiter_sell_quotes: int = Field(ge=1)
    exit_fills: int = Field(ge=1)
    exit_fills_with_executable_sell_quote: int = Field(ge=1)
    closed_positions: int = Field(ge=1)
    positions_with_round_trip_costs: int = Field(ge=1)
    positions_with_risk_sizing_evidence: int = Field(ge=1)
    starting_equity_usd: Decimal
    maximum_position_usd: Decimal = Field(ge=0, le=20)
    maximum_exposure_usd: Decimal = Field(ge=0, le=50)
    daily_loss_gate_observed: Literal[True]
    drawdown_hard_halt_observed: Literal[True]
    telegram_resume_bypass_blocked: Literal[True]
    ledger_reconciliation_difference_usd: Decimal
    report_reconciliation_difference_usd: Decimal
    daily_report_generated: Literal[True]
    all_time_endpoint_verified: Literal[True]
    restart_duplicate_positions: int = Field(ge=0, le=0)
    deterministic_replay_hash_match: Literal[True]
    audited_positions: int = Field(ge=1)
    positions_missing_audit_items: int = Field(ge=0, le=0)

    @model_validator(mode="after")
    def require_complete_mainnet_proof(self) -> Self:
        missing_filters = REQUIRED_HARD_FILTER_PROOFS - self.hard_filter_proofs
        if missing_filters:
            raise ValueError(f"missing hard-filter runtime proofs: {sorted(missing_filters)}")
        if self.unsafe_filter_observations != self.unsafe_filter_observations_blocked:
            raise ValueError("every unsafe filter observation must be blocked")
        if self.unsafe_filter_observations < len(REQUIRED_HARD_FILTER_PROOFS):
            raise ValueError("hard-filter denominator is smaller than the mandatory proof set")
        if self.exit_fills != self.exit_fills_with_executable_sell_quote:
            raise ValueError("every exit fill must reference an executable sell quote")
        if self.positions_with_round_trip_costs != self.closed_positions:
            raise ValueError("every closed position must include round-trip costs")
        if self.positions_with_risk_sizing_evidence != self.closed_positions:
            raise ValueError("every closed position must include risk-derived sizing evidence")
        if self.starting_equity_usd != Decimal("500"):
            raise ValueError("paper starting equity must equal 500 USDC")
        if abs(self.ledger_reconciliation_difference_usd) > Decimal("0.01"):
            raise ValueError("ledger reconciliation difference exceeds 0.01 USDC")
        if abs(self.report_reconciliation_difference_usd) > Decimal("0.01"):
            raise ValueError("report reconciliation difference exceeds 0.01 USDC")
        if self.audited_positions != self.closed_positions:
            raise ValueError("every closed position must be covered by the audit denominator")
        return self


class TelegramRuntimeEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    delivered_event_types: set[str]
    lifecycle_alert_p95_seconds: Decimal = Field(ge=0, lt=5)
    unauthorized_access_blocked: Literal[True]
    lifecycle_alert_after_database_commit: Literal[True]
    outbox_redelivery_duplicates: int = Field(ge=0, le=0)
    outage_collection_continued: Literal[True]
    undelivered_messages_retained: Literal[True]

    @model_validator(mode="after")
    def require_all_notifications(self) -> Self:
        missing = REQUIRED_TELEGRAM_EVENT_TYPES - self.delivered_event_types
        if missing:
            raise ValueError(f"missing mandatory Telegram event types: {sorted(missing)}")
        return self


class DeploymentRuntimeEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    compose_started: Literal[True]
    migrations_applied_automatically: Literal[True]
    non_root_container: Literal[True]
    api_bound_to_localhost: Literal[True]
    secrets_absent_from_image: Literal[True]
    minimum_postgres_role_verified: Literal[True]
    ntp_synchronized: Literal[True]
    post_reboot_started: Literal[True]
    restart_duplicate_positions: int = Field(ge=0, le=0)
    daily_backup_created: Literal[True]
    restore_verified_on_separate_database: Literal[True]


class PerformanceRuntimeEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    internal_event_p95_ms: Decimal = Field(ge=0, lt=250)
    feature_update_p95_ms: Decimal = Field(ge=0, lt=100)
    telegram_lifecycle_alert_p95_seconds: Decimal = Field(ge=0, lt=5)
    daily_report_max_seconds: Decimal = Field(ge=0, lt=120)


class RuntimeClaimReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid")

    claim: str
    source_type: Literal[
        "benchmark",
        "ci",
        "host_command",
        "postgres_query",
        "rpc_capture",
        "telegram_receipt",
    ]
    source: ArtifactEvidence
    observed_at: datetime

    @field_validator("observed_at")
    @classmethod
    def require_observed_at_aware(cls, value: datetime) -> datetime:
        return _require_aware(value, "runtime claim receipt observed_at")


class RuntimeAcceptanceArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[3]
    revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    generated_at: datetime
    observation_started_at: datetime
    observation_ended_at: datetime
    claim_receipts: list[RuntimeClaimReceipt]
    operational_cost_receipts: list[ArtifactEvidence] = Field(min_length=1)
    mainnet: MainnetRuntimeEvidence
    telegram: TelegramRuntimeEvidence
    deployment: DeploymentRuntimeEvidence
    performance: PerformanceRuntimeEvidence

    @field_validator("generated_at", "observation_started_at", "observation_ended_at")
    @classmethod
    def require_timestamps_aware(cls, value: datetime) -> datetime:
        return _require_aware(value, "runtime evidence timestamp")

    @model_validator(mode="after")
    def require_ordered_observation(self) -> Self:
        if not self.observation_started_at < self.observation_ended_at <= self.generated_at:
            raise ValueError("runtime evidence timestamps are not ordered")
        receipt_by_claim = {receipt.claim: receipt for receipt in self.claim_receipts}
        if len(receipt_by_claim) != len(self.claim_receipts):
            raise ValueError("runtime claim receipts contain duplicate claims")
        if set(receipt_by_claim) != REQUIRED_RUNTIME_CLAIMS:
            raise ValueError("runtime claim receipts are missing or unexpected")
        if any(
            receipt.source_type != RUNTIME_CLAIM_SOURCE_TYPES[receipt.claim]
            for receipt in self.claim_receipts
        ):
            raise ValueError("runtime claim receipt uses the wrong source type")
        if any(
            not self.observation_started_at <= receipt.observed_at <= self.observation_ended_at
            for receipt in self.claim_receipts
        ):
            raise ValueError("runtime claim receipt falls outside the observation window")
        receipt_paths = [receipt.path for receipt in self.operational_cost_receipts]
        receipt_hashes = [receipt.sha256 for receipt in self.operational_cost_receipts]
        if len(set(receipt_paths)) != len(receipt_paths) or len(set(receipt_hashes)) != len(receipt_hashes):
            raise ValueError("operational cost receipt references must be distinct")
        return self


class StatisticalProtocol(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[3]
    revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    strategy_version_id: str = Field(min_length=1)
    config_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    frozen_at: datetime
    collection_started_at: datetime
    oos_started_at: datetime
    collection_ended_at: datetime
    minimum_negative_launches: int = Field(ge=300)
    minimum_oos_trades: int = Field(ge=100)
    maximum_equity_mark_gap_seconds: int = Field(ge=1, le=3600)
    daily_operational_cost_usd: Decimal = Field(gt=0)
    negative_launch_definition: Literal["distinct_rejected_pumpswap_pool"]
    time_zone: str = "Europe/Kyiv"

    @field_validator("frozen_at", "collection_started_at", "oos_started_at", "collection_ended_at")
    @classmethod
    def require_timestamps_aware(cls, value: datetime) -> datetime:
        return _require_aware(value, "statistical protocol timestamp")

    @model_validator(mode="after")
    def require_precommitted_order(self) -> Self:
        if not self.frozen_at < self.collection_started_at < self.oos_started_at < self.collection_ended_at:
            raise ValueError("protocol must be frozen before collection and contain separate IS/OOS periods")
        ZoneInfo(self.time_zone)
        return self


class ProtocolPrecommitReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1]
    protocol_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    published_at: datetime
    immutable_reference: str = Field(min_length=8)

    @field_validator("published_at")
    @classmethod
    def require_published_at_aware(cls, value: datetime) -> datetime:
        return _require_aware(value, "protocol precommit published_at")


class CriterionResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    passed: bool
    actual: str | int | bool | None
    expected: str


class StatisticalMetrics(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fixed_revision_strategy_cohort: bool
    paper_account_count: int = Field(ge=0)
    starting_equity_usd: Decimal | None
    discovered_pumpswap_pool_count: int = Field(ge=0)
    materialized_pumpswap_pool_count: int = Field(ge=0)
    missing_materialized_pool_count: int = Field(ge=0)
    missing_final_pool_outcome_count: int = Field(ge=0)
    negative_launch_count: int = Field(ge=0)
    closed_trade_count: int = Field(ge=0)
    in_sample_trade_count: int = Field(ge=0)
    oos_trade_count: int = Field(ge=0)
    censored_position_count: int = Field(ge=0)
    oos_equity_mark_count: int = Field(ge=0)
    oos_equity_mark_coverage: bool
    oos_operational_cost_usd: Decimal = Field(ge=0)
    recorded_operational_cost_usd: Decimal = Field(ge=0)
    operational_costs_reconciled: bool
    oos_net_pnl_after_all_costs_usd: Decimal
    oos_gross_profit_after_all_costs_usd: Decimal = Field(ge=0)
    oos_gross_loss_after_all_costs_usd: Decimal = Field(ge=0)
    oos_profit_factor: str
    oos_max_drawdown_pct: Decimal = Field(ge=0)
    oos_expectancy_after_all_costs_usd: Decimal
    largest_trade_profit_share_pct: Decimal = Field(ge=0)
    net_pnl_without_best_day_usd: Decimal
    net_pnl_without_best_developer_cluster_usd: Decimal
    missing_developer_cluster_trades: int = Field(ge=0)


class StatisticalReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[4]
    revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    generated_at: datetime
    protocol_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    input_snapshot_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    strategy_version_id: str
    config_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    collection_started_at: datetime
    oos_started_at: datetime
    collection_ended_at: datetime
    operational_costs: list[OperationalCostEvidence]
    metrics: StatisticalMetrics
    criteria: list[CriterionResult]
    passed: bool

    @field_validator("generated_at", "collection_started_at", "oos_started_at", "collection_ended_at")
    @classmethod
    def require_timestamps_aware(cls, value: datetime) -> datetime:
        return _require_aware(value, "statistical report timestamp")

    @model_validator(mode="after")
    def verify_criteria_are_complete(self) -> Self:
        by_name = {criterion.name: criterion for criterion in self.criteria}
        if len(by_name) != len(self.criteria) or set(by_name) != REQUIRED_STATISTICAL_CRITERIA:
            raise ValueError("statistical criteria are missing or duplicated")
        if self.generated_at < self.collection_ended_at:
            raise ValueError("statistical report cannot be generated before collection ends")
        source_hashes = [cost.source_reference_sha256 for cost in self.operational_costs]
        if len(set(source_hashes)) != len(source_hashes):
            raise ValueError("statistical report contains duplicate operational-cost sources")
        if any(
            not self.oos_started_at <= cost.incurred_at <= self.collection_ended_at
            for cost in self.operational_costs
        ):
            raise ValueError("operational cost falls outside the OOS interval")
        return self


@dataclass(frozen=True, slots=True)
class ClosedTrade:
    position_id: str
    entry_time: datetime
    closed_at: datetime
    pnl_usd: Decimal
    developer_cluster: str | None
    cluster_evaluated: bool


@dataclass(frozen=True, slots=True)
class EquityPoint:
    observed_at: datetime
    equity_usd: Decimal
    sequence_id: str = ""


@dataclass(frozen=True, slots=True)
class StatisticalInputs:
    fixed_revision_strategy_cohort: bool
    discovered_pool_count: int
    materialized_pool_count: int
    missing_materialized_pool_count: int
    missing_final_pool_outcome_count: int
    negative_launch_count: int
    censored_position_count: int
    recorded_operational_cost_usd: Decimal
    starting_equities: Sequence[Decimal]
    closed_trades: Sequence[ClosedTrade]
    equity_points: Sequence[EquityPoint]
    operational_costs: Sequence[OperationalCostEvidence] = ()


def _profit_factor_value(value: str) -> Decimal:
    if value == "Infinity":
        return Decimal("Infinity")
    try:
        return Decimal(value)
    except Exception as exc:
        raise ValueError("invalid profit factor") from exc


def _equity_drawdown(points: Sequence[EquityPoint], protocol: StatisticalProtocol) -> tuple[bool, Decimal, int]:
    ordered = sorted(points, key=lambda point: (_as_utc(point.observed_at), point.sequence_id))
    if len({(_as_utc(point.observed_at), point.sequence_id) for point in ordered}) != len(ordered):
        return False, Decimal("100"), 0
    baseline = [point for point in ordered if _as_utc(point.observed_at) <= protocol.oos_started_at]
    observed = [
        point
        for point in ordered
        if protocol.oos_started_at < _as_utc(point.observed_at) <= protocol.collection_ended_at
    ]
    if not baseline or not observed:
        return False, Decimal("100"), len(observed)
    equity_path = [baseline[-1], *observed]
    max_gap = timedelta(seconds=protocol.maximum_equity_mark_gap_seconds)
    coverage = (
        protocol.oos_started_at - _as_utc(equity_path[0].observed_at) <= max_gap
        and _as_utc(equity_path[-1].observed_at) >= protocol.collection_ended_at - max_gap
        and all(
            _as_utc(current.observed_at) - _as_utc(previous.observed_at) <= max_gap
            for previous, current in zip(equity_path, equity_path[1:], strict=False)
        )
    )
    peak = equity_path[0].equity_usd
    maximum = Decimal("0")
    for point in equity_path[1:]:
        peak = max(peak, point.equity_usd)
        drawdown = Decimal("100") if peak <= 0 else max(
            Decimal("0"), (peak - point.equity_usd) / peak * Decimal("100")
        )
        maximum = max(maximum, drawdown)
    return coverage, maximum, len(observed)


def _statistical_pass_map(
    metrics: StatisticalMetrics, protocol: StatisticalProtocol
) -> dict[str, bool]:
    return {
        "fixed_revision_strategy_cohort": metrics.fixed_revision_strategy_cohort,
        "single_paper_account": metrics.paper_account_count == 1,
        "starting_equity_500": metrics.starting_equity_usd == Decimal("500"),
        "pool_retention_complete": metrics.missing_materialized_pool_count == 0,
        "all_discovered_pools_evaluated": metrics.missing_final_pool_outcome_count == 0,
        "minimum_new_pools": metrics.discovered_pumpswap_pool_count >= 3000,
        "minimum_closed_trades": metrics.closed_trade_count >= 300,
        "minimum_negative_launches": metrics.negative_launch_count >= protocol.minimum_negative_launches,
        "minimum_oos_trades": metrics.oos_trade_count >= protocol.minimum_oos_trades,
        "no_censored_positions": metrics.censored_position_count == 0,
        "separate_oos_period": metrics.in_sample_trade_count > 0 and metrics.oos_trade_count > 0,
        "equity_mark_coverage": metrics.oos_equity_mark_coverage,
        "operational_costs_reconciled": metrics.operational_costs_reconciled,
        "oos_net_pnl_after_all_costs_positive": metrics.oos_net_pnl_after_all_costs_usd > 0,
        "profit_factor": _profit_factor_value(metrics.oos_profit_factor) >= Decimal("1.15"),
        "max_drawdown": metrics.oos_max_drawdown_pct <= Decimal("10"),
        "positive_expectancy": metrics.oos_expectancy_after_all_costs_usd > 0,
        "single_trade_concentration": metrics.largest_trade_profit_share_pct <= Decimal("20"),
        "day_independence": metrics.net_pnl_without_best_day_usd > 0,
        "cluster_attribution_complete": metrics.missing_developer_cluster_trades == 0,
        "developer_cluster_independence": metrics.net_pnl_without_best_developer_cluster_usd > 0,
    }


def _statistical_input_digest(inputs: StatisticalInputs) -> str:
    payload = {
        "fixed_revision_strategy_cohort": inputs.fixed_revision_strategy_cohort,
        "discovered_pool_count": inputs.discovered_pool_count,
        "materialized_pool_count": inputs.materialized_pool_count,
        "missing_materialized_pool_count": inputs.missing_materialized_pool_count,
        "missing_final_pool_outcome_count": inputs.missing_final_pool_outcome_count,
        "negative_launch_count": inputs.negative_launch_count,
        "censored_position_count": inputs.censored_position_count,
        "recorded_operational_cost_usd": str(inputs.recorded_operational_cost_usd),
        "operational_costs": [
            cost.model_dump(mode="json")
            for cost in sorted(
                inputs.operational_costs,
                key=lambda item: item.source_reference_sha256,
            )
        ],
        "starting_equities": [str(value) for value in inputs.starting_equities],
        "closed_trades": [
            {
                "position_id": trade.position_id,
                "entry_time": _as_utc(trade.entry_time).isoformat(),
                "closed_at": _as_utc(trade.closed_at).isoformat(),
                "pnl_usd": str(trade.pnl_usd),
                "developer_cluster": trade.developer_cluster,
                "cluster_evaluated": trade.cluster_evaluated,
            }
            for trade in sorted(inputs.closed_trades, key=lambda item: item.position_id)
        ],
        "equity_points": [
            {
                "observed_at": _as_utc(point.observed_at).isoformat(),
                "equity_usd": str(point.equity_usd),
                "sequence_id": point.sequence_id,
            }
            for point in sorted(
                inputs.equity_points,
                key=lambda item: (_as_utc(item.observed_at), item.sequence_id),
            )
        ],
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()


def _criterion_details(
    metrics: StatisticalMetrics, protocol: StatisticalProtocol
) -> tuple[dict[str, str | int | bool | None], dict[str, str]]:
    actuals: dict[str, str | int | bool | None] = {
        "fixed_revision_strategy_cohort": metrics.fixed_revision_strategy_cohort,
        "single_paper_account": metrics.paper_account_count,
        "starting_equity_500": None if metrics.starting_equity_usd is None else str(metrics.starting_equity_usd),
        "pool_retention_complete": metrics.missing_materialized_pool_count,
        "all_discovered_pools_evaluated": metrics.missing_final_pool_outcome_count,
        "minimum_new_pools": metrics.discovered_pumpswap_pool_count,
        "minimum_closed_trades": metrics.closed_trade_count,
        "minimum_negative_launches": metrics.negative_launch_count,
        "minimum_oos_trades": metrics.oos_trade_count,
        "no_censored_positions": metrics.censored_position_count,
        "separate_oos_period": metrics.in_sample_trade_count > 0 and metrics.oos_trade_count > 0,
        "equity_mark_coverage": metrics.oos_equity_mark_coverage,
        "operational_costs_reconciled": metrics.operational_costs_reconciled,
        "oos_net_pnl_after_all_costs_positive": str(metrics.oos_net_pnl_after_all_costs_usd),
        "profit_factor": metrics.oos_profit_factor,
        "max_drawdown": str(metrics.oos_max_drawdown_pct),
        "positive_expectancy": str(metrics.oos_expectancy_after_all_costs_usd),
        "single_trade_concentration": str(metrics.largest_trade_profit_share_pct),
        "day_independence": str(metrics.net_pnl_without_best_day_usd),
        "cluster_attribution_complete": metrics.missing_developer_cluster_trades,
        "developer_cluster_independence": str(metrics.net_pnl_without_best_developer_cluster_usd),
    }
    expected = {
        "fixed_revision_strategy_cohort": "strategy/config/revision match the frozen protocol",
        "single_paper_account": "exactly 1",
        "starting_equity_500": "500 USDC",
        "pool_retention_complete": "0 discovered PumpSwap pools missing from pools",
        "all_discovered_pools_evaluated": "0 discovered PumpSwap pools without a terminal candidate outcome",
        "minimum_new_pools": ">= 3000 distinct discovered PumpSwap pools",
        "minimum_closed_trades": ">= 300",
        "minimum_negative_launches": f">= {protocol.minimum_negative_launches}",
        "minimum_oos_trades": f">= {protocol.minimum_oos_trades}",
        "no_censored_positions": "0 cohort positions unresolved at collection cutoff",
        "separate_oos_period": "both entry-time IS and OOS trades",
        "equity_mark_coverage": f"full OOS executable-equity path with gaps <= {protocol.maximum_equity_mark_gap_seconds}s",
        "operational_costs_reconciled": "ledger operational costs cover the frozen cost floor",
        "oos_net_pnl_after_all_costs_positive": "> 0 USDC after simulated and operational costs",
        "profit_factor": ">= 1.15 after allocated operational costs",
        "max_drawdown": "<= 10% from durable executable-equity marks",
        "positive_expectancy": "> 0 USDC after all costs",
        "single_trade_concentration": "<= 20% of gross economic profit",
        "day_independence": "economic net PnL remains > 0 without the best day",
        "cluster_attribution_complete": "0 trades without a known funding cluster",
        "developer_cluster_independence": "economic net PnL remains > 0 without the best cluster",
    }
    return actuals, expected


def evaluate_statistical_stage(
    *, inputs: StatisticalInputs, protocol: StatisticalProtocol, protocol_sha256: str
) -> StatisticalReport:
    if re.fullmatch(r"[0-9a-f]{64}", protocol_sha256) is None:
        raise ValueError("protocol_sha256 must be lowercase hexadecimal")
    generated_at = datetime.now(tz=timezone.utc)
    if generated_at < protocol.collection_ended_at:
        raise ValueError("statistical collection has not ended")
    trades = sorted(inputs.closed_trades, key=lambda trade: (_as_utc(trade.entry_time), trade.position_id))
    for trade in trades:
        entry_time = _as_utc(trade.entry_time)
        closed_at = _as_utc(trade.closed_at)
        if not protocol.collection_started_at <= entry_time <= protocol.collection_ended_at:
            raise ValueError("trade entry falls outside the frozen collection interval")
        if not entry_time <= closed_at <= protocol.collection_ended_at:
            raise ValueError("trade closure falls outside the frozen collection interval")
    in_sample = [trade for trade in trades if _as_utc(trade.entry_time) < protocol.oos_started_at]
    oos = [trade for trade in trades if _as_utc(trade.entry_time) >= protocol.oos_started_at]
    zone = ZoneInfo(protocol.time_zone)
    oos_days = (
        protocol.collection_ended_at.astimezone(zone).date() - protocol.oos_started_at.astimezone(zone).date()
    ).days + 1
    planned_operational_cost = protocol.daily_operational_cost_usd * Decimal(oos_days)
    operational_costs = sorted(
        inputs.operational_costs,
        key=lambda cost: cost.source_reference_sha256,
    )
    source_hashes = [cost.source_reference_sha256 for cost in operational_costs]
    receipt_total = sum(
        (cost.amount_usd for cost in operational_costs), Decimal("0")
    ).quantize(Decimal("0.000001"))
    operational_costs_reconciled = (
        bool(operational_costs)
        and len(set(source_hashes)) == len(source_hashes)
        and all(
            protocol.oos_started_at <= cost.incurred_at <= protocol.collection_ended_at
            for cost in operational_costs
        )
        and receipt_total == inputs.recorded_operational_cost_usd
        and inputs.recorded_operational_cost_usd >= planned_operational_cost
    )
    operational_cost = max(planned_operational_cost, inputs.recorded_operational_cost_usd)
    allocated_cost = Decimal("0") if not oos else operational_cost / Decimal(len(oos))
    economic_pnls = [trade.pnl_usd - allocated_cost for trade in oos]
    net_pnl = sum(economic_pnls, -operational_cost if not oos else Decimal("0"))
    gross_profit = sum((pnl for pnl in economic_pnls if pnl > 0), Decimal("0"))
    gross_loss = -sum((pnl for pnl in economic_pnls if pnl < 0), Decimal("0"))
    if not oos and operational_cost > 0:
        gross_loss = operational_cost
    profit_factor = "Infinity" if gross_loss == 0 and gross_profit > 0 else str(
        Decimal("0") if gross_loss == 0 else gross_profit / gross_loss
    )
    expectancy = Decimal("0") if not oos else net_pnl / Decimal(len(oos))
    largest_profit = max((pnl for pnl in economic_pnls if pnl > 0), default=Decimal("0"))
    largest_share = Decimal("100") if gross_profit <= 0 else largest_profit / gross_profit * Decimal("100")

    pnl_by_day: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    pnl_by_cluster: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    missing_cluster_count = 0
    for trade, economic_pnl in zip(oos, economic_pnls, strict=True):
        day = _as_utc(trade.closed_at).astimezone(zone).date().isoformat()
        pnl_by_day[day] += economic_pnl
        if not trade.cluster_evaluated or not (trade.developer_cluster or "").strip():
            missing_cluster_count += 1
        else:
            pnl_by_cluster[str(trade.developer_cluster)] += economic_pnl
    without_best_day = (
        net_pnl - max(pnl_by_day.values()) if len(pnl_by_day) >= 2 else Decimal("0")
    )
    without_best_cluster = (
        net_pnl - max(pnl_by_cluster.values()) if len(pnl_by_cluster) >= 2 else Decimal("0")
    )
    equity_coverage, max_drawdown, equity_mark_count = _equity_drawdown(inputs.equity_points, protocol)
    starting_equity = inputs.starting_equities[0] if len(inputs.starting_equities) == 1 else None
    metrics = StatisticalMetrics(
        fixed_revision_strategy_cohort=inputs.fixed_revision_strategy_cohort,
        paper_account_count=len(inputs.starting_equities),
        starting_equity_usd=starting_equity,
        discovered_pumpswap_pool_count=inputs.discovered_pool_count,
        materialized_pumpswap_pool_count=inputs.materialized_pool_count,
        missing_materialized_pool_count=inputs.missing_materialized_pool_count,
        missing_final_pool_outcome_count=inputs.missing_final_pool_outcome_count,
        negative_launch_count=inputs.negative_launch_count,
        closed_trade_count=len(trades),
        in_sample_trade_count=len(in_sample),
        oos_trade_count=len(oos),
        censored_position_count=inputs.censored_position_count,
        oos_equity_mark_count=equity_mark_count,
        oos_equity_mark_coverage=equity_coverage,
        oos_operational_cost_usd=operational_cost,
        recorded_operational_cost_usd=inputs.recorded_operational_cost_usd,
        operational_costs_reconciled=operational_costs_reconciled,
        oos_net_pnl_after_all_costs_usd=net_pnl,
        oos_gross_profit_after_all_costs_usd=gross_profit,
        oos_gross_loss_after_all_costs_usd=gross_loss,
        oos_profit_factor=profit_factor,
        oos_max_drawdown_pct=max_drawdown,
        oos_expectancy_after_all_costs_usd=expectancy,
        largest_trade_profit_share_pct=largest_share,
        net_pnl_without_best_day_usd=without_best_day,
        net_pnl_without_best_developer_cluster_usd=without_best_cluster,
        missing_developer_cluster_trades=missing_cluster_count,
    )
    pass_map = _statistical_pass_map(metrics, protocol)
    actuals, expected = _criterion_details(metrics, protocol)
    criteria = [
        CriterionResult(name=name, passed=pass_map[name], actual=actuals[name], expected=expected[name])
        for name in sorted(REQUIRED_STATISTICAL_CRITERIA)
    ]
    return StatisticalReport(
        schema_version=4,
        revision=protocol.revision,
        generated_at=generated_at,
        protocol_sha256=protocol_sha256,
        input_snapshot_sha256=_statistical_input_digest(inputs),
        strategy_version_id=protocol.strategy_version_id,
        config_hash=protocol.config_hash,
        collection_started_at=protocol.collection_started_at,
        oos_started_at=protocol.oos_started_at,
        collection_ended_at=protocol.collection_ended_at,
        operational_costs=operational_costs,
        metrics=metrics,
        criteria=criteria,
        passed=all(pass_map.values()),
    )


async def load_statistical_stage_data(database: Database, protocol: StatisticalProtocol) -> StatisticalInputs:
    async with database.sessions() as session:
        strategy = await session.get(StrategyVersionRow, protocol.strategy_version_id)
        fixed_cohort = bool(
            strategy is not None
            and strategy.config_hash == protocol.config_hash
            and strategy.git_commit == protocol.revision
        )
        account_rows = (
            await session.execute(select(PaperAccountRow.id, PaperAccountRow.starting_equity))
        ).all()
        starting_equities = [Decimal(row.starting_equity) for row in account_rows]
        account_ids = [row.id for row in account_rows]
        recorded_operational_cost = Decimal("0")
        operational_costs: list[OperationalCostEvidence] = []
        if len(account_ids) == 1:
            recorded_rows = (
                await session.execute(
                    select(
                        OperationalCostRow.amount_usd,
                        OperationalCostRow.category,
                        OperationalCostRow.incurred_at,
                        OperationalCostRow.source_reference_sha256,
                    ).where(
                        OperationalCostRow.account_id == account_ids[0],
                        OperationalCostRow.incurred_at >= protocol.oos_started_at,
                        OperationalCostRow.incurred_at <= protocol.collection_ended_at,
                    )
                )
            ).all()
            if any(
                re.fullmatch(r"[0-9a-f]{64}", row.source_reference_sha256) is None
                for row in recorded_rows
            ):
                raise ValueError("operational cost source reference is not a valid SHA-256")
            recorded_operational_cost = sum(
                (Decimal(row.amount_usd) for row in recorded_rows),
                Decimal("0"),
            ).quantize(Decimal("0.000001"))
            operational_costs = [
                OperationalCostEvidence(
                    account_id=str(account_ids[0]),
                    category=str(row.category),
                    amount_usd=Decimal(row.amount_usd),
                    incurred_at=(
                        row.incurred_at.replace(tzinfo=timezone.utc)
                        if row.incurred_at.tzinfo is None
                        and database.engine.dialect.name == "sqlite"
                        else row.incurred_at
                    ),
                    source_reference_sha256=str(row.source_reference_sha256),
                )
                for row in recorded_rows
            ]
        raw_pool_addresses = set(
            (
                await session.scalars(
                    select(RawChainEventRow.pool_address)
                    .where(
                        RawChainEventRow.protocol == "pumpswap",
                        RawChainEventRow.event_type == "pool_created",
                        RawChainEventRow.pool_address.is_not(None),
                        RawChainEventRow.block_time >= protocol.collection_started_at,
                        RawChainEventRow.block_time <= protocol.collection_ended_at,
                    )
                    .distinct()
                )
            ).all()
        )
        materialized_pool_addresses = set(
            (
                await session.scalars(
                    select(PoolRow.pool_address).where(
                        PoolRow.protocol == "pumpswap",
                        PoolRow.creation_time >= protocol.collection_started_at,
                        PoolRow.creation_time <= protocol.collection_ended_at,
                    )
                )
            ).all()
        )
        rejected_pool_addresses = set(
            (
                await session.scalars(
                    select(CandidateRow.pool_address)
                    .where(
                        CandidateRow.strategy_version_id == protocol.strategy_version_id,
                        CandidateRow.config_hash == protocol.config_hash,
                        CandidateRow.detected_at >= protocol.collection_started_at,
                        CandidateRow.detected_at <= protocol.collection_ended_at,
                        CandidateRow.state == "REJECTED",
                        CandidateRow.rejected_at.is_not(None),
                        CandidateRow.rejected_at <= protocol.collection_ended_at,
                    )
                    .distinct()
                )
            ).all()
        )
        closed_candidate_pool_addresses = set(
            (
                await session.scalars(
                    select(CandidateRow.pool_address)
                    .where(
                        CandidateRow.strategy_version_id == protocol.strategy_version_id,
                        CandidateRow.config_hash == protocol.config_hash,
                        CandidateRow.detected_at >= protocol.collection_started_at,
                        CandidateRow.detected_at <= protocol.collection_ended_at,
                        CandidateRow.state == "CLOSED",
                    )
                    .distinct()
                )
            ).all()
        )
        rows = (
            await session.execute(
                select(
                    PaperPositionRow.id,
                    PaperPositionRow.pool_address,
                    PaperPositionRow.entry_time,
                    PaperPositionRow.closed_at,
                    PaperPositionRow.realized_pnl,
                    PaperPositionRow.status,
                    TokenRow.creator_address,
                    WalletProfileRow.wallet_address,
                    WalletProfileRow.known_funding_cluster,
                )
                .outerjoin(TokenRow, TokenRow.mint == PaperPositionRow.mint)
                .outerjoin(WalletProfileRow, WalletProfileRow.wallet_address == TokenRow.creator_address)
                .where(
                    PaperPositionRow.strategy_version_id == protocol.strategy_version_id,
                    PaperPositionRow.config_hash == protocol.config_hash,
                    PaperPositionRow.entry_time >= protocol.collection_started_at,
                    PaperPositionRow.entry_time <= protocol.collection_ended_at,
                )
                .order_by(PaperPositionRow.entry_time, PaperPositionRow.id)
            )
        ).all()
        equity_points: list[EquityPoint] = []
        if len(account_ids) == 1:
            baseline = await session.scalar(
                select(PaperEquityMarkRow)
                .where(
                    PaperEquityMarkRow.account_id == account_ids[0],
                    PaperEquityMarkRow.observed_at <= protocol.oos_started_at,
                )
                .order_by(desc(PaperEquityMarkRow.observed_at))
                .order_by(desc(PaperEquityMarkRow.id))
                .limit(1)
            )
            observed = list(
                (
                    await session.scalars(
                        select(PaperEquityMarkRow)
                        .where(
                            PaperEquityMarkRow.account_id == account_ids[0],
                            PaperEquityMarkRow.observed_at > protocol.oos_started_at,
                            PaperEquityMarkRow.observed_at <= protocol.collection_ended_at,
                        )
                        .order_by(PaperEquityMarkRow.observed_at, PaperEquityMarkRow.id)
                        .limit(MAX_STATISTICAL_EQUITY_MARKS + 1)
                    )
                ).all()
            )
            if len(observed) > MAX_STATISTICAL_EQUITY_MARKS:
                raise ValueError("statistical equity-mark cohort exceeds the bounded evaluator limit")
            if baseline is not None:
                equity_points.append(
                    EquityPoint(baseline.observed_at, Decimal(baseline.equity), str(baseline.id))
                )
            equity_points.extend(
                EquityPoint(row.observed_at, Decimal(row.equity), str(row.id)) for row in observed
            )
    trades = []
    closed_pool_addresses: set[str] = set()
    censored_position_count = 0
    for position_id, pool_address, entry_time, closed_at, pnl, status, creator, profile_wallet, known_cluster in rows:
        if status != "CLOSED" or closed_at is None or _as_utc(closed_at) > protocol.collection_ended_at:
            censored_position_count += 1
            continue
        closed_pool_addresses.add(str(pool_address))
        clustering_evaluated = bool(creator and profile_wallet and known_cluster)
        cluster = known_cluster if clustering_evaluated else None
        trades.append(
            ClosedTrade(
                position_id=str(position_id),
                entry_time=entry_time,
                closed_at=closed_at,
                pnl_usd=Decimal(pnl),
                developer_cluster=cluster,
                cluster_evaluated=clustering_evaluated,
            )
        )
    final_pool_addresses = rejected_pool_addresses | (
        closed_pool_addresses & closed_candidate_pool_addresses
    )
    return StatisticalInputs(
        fixed_revision_strategy_cohort=fixed_cohort,
        discovered_pool_count=len(raw_pool_addresses),
        materialized_pool_count=len(materialized_pool_addresses),
        missing_materialized_pool_count=len(raw_pool_addresses - materialized_pool_addresses),
        missing_final_pool_outcome_count=len(raw_pool_addresses - final_pool_addresses),
        negative_launch_count=len(rejected_pool_addresses & raw_pool_addresses),
        censored_position_count=censored_position_count,
        recorded_operational_cost_usd=recorded_operational_cost,
        starting_equities=starting_equities,
        closed_trades=trades,
        equity_points=equity_points,
        operational_costs=operational_costs,
    )


def _read_verified_artifact(artifact: ArtifactEvidence, root: Path) -> bytes:
    unresolved = root / artifact.path
    try:
        target = unresolved.resolve(strict=True)
    except FileNotFoundError as exc:
        raise ValueError(f"acceptance artifact is missing: {artifact.path}") from exc
    try:
        target.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"artifact escapes evidence root: {artifact.path}") from exc
    component = unresolved
    while component != root.parent:
        if component.is_symlink():
            raise ValueError(f"acceptance artifact path contains a symlink: {artifact.path}")
        if component == root:
            break
        component = component.parent
    before = target.stat(follow_symlinks=False)
    if not stat.S_ISREG(before.st_mode):
        raise ValueError(f"acceptance artifact is not a regular file: {artifact.path}")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(target, flags)
    try:
        opened = os.fstat(descriptor)
        if (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
            raise ValueError(f"acceptance artifact changed while opening: {artifact.path}")
        if opened.st_size > MAX_ARTIFACT_BYTES:
            raise ValueError(f"acceptance artifact exceeds {MAX_ARTIFACT_BYTES} bytes: {artifact.path}")
        chunks: list[bytes] = []
        remaining = MAX_ARTIFACT_BYTES + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        content = b"".join(chunks)
        if len(content) > MAX_ARTIFACT_BYTES or os.read(descriptor, 1):
            raise ValueError(f"acceptance artifact exceeds {MAX_ARTIFACT_BYTES} bytes: {artifact.path}")
    finally:
        os.close(descriptor)
    if hashlib.sha256(content).hexdigest() != artifact.sha256:
        raise ValueError(f"acceptance artifact hash mismatch: {artifact.path}")
    text = content.decode("utf-8")
    for pattern in _SECRET_PATTERNS:
        if pattern.search(text):
            raise ValueError(f"acceptance artifact contains secret or private identifiers: {artifact.path}")
    return content


def _require_recent(timestamp: datetime, *, now: datetime, max_age: timedelta, label: str) -> None:
    timestamp = _require_aware(timestamp, label)
    if timestamp > now + timedelta(minutes=5):
        raise ValueError(f"{label} is in the future")
    if now - timestamp > max_age:
        raise ValueError(f"{label} is older than the allowed evidence age")


def verify_acceptance_evidence(
    document: Mapping[str, Any],
    *,
    artifact_root: Path,
    expected_revision: str,
    max_evidence_age_hours: int,
    expected_ci_sha256: str,
    expected_runtime_sha256: str,
    expected_precommit_receipt_sha256: str,
    expected_protocol_sha256: str,
    expected_report_sha256: str,
    trusted_protocol_published_at: datetime,
    now: datetime | None = None,
) -> AcceptanceManifest:
    if re.fullmatch(r"[0-9a-f]{40}", expected_revision) is None:
        raise ValueError("expected_revision must be a 40-character lowercase Git commit")
    if max_evidence_age_hours <= 0:
        raise ValueError("max_evidence_age_hours must be positive")
    manifest = AcceptanceManifest.model_validate(document)
    if manifest.revision != expected_revision:
        raise ValueError("manifest revision does not match trusted expected revision")
    current = _require_aware(now or datetime.now(tz=timezone.utc), "verification time")
    max_age = timedelta(hours=max_evidence_age_hours)
    trusted_hashes = {
        "CI": expected_ci_sha256.lower(),
        "runtime": expected_runtime_sha256.lower(),
        "protocol precommit receipt": expected_precommit_receipt_sha256.lower(),
        "statistical protocol": expected_protocol_sha256.lower(),
        "statistical report": expected_report_sha256.lower(),
    }
    manifest_hashes = {
        "CI": manifest.ci.sha256,
        "runtime": manifest.runtime.sha256,
        "protocol precommit receipt": manifest.protocol_precommit_receipt.sha256,
        "statistical protocol": manifest.statistical_protocol.sha256,
        "statistical report": manifest.statistical_report.sha256,
    }
    for label, digest in trusted_hashes.items():
        if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise ValueError(f"trusted {label} SHA-256 is invalid")
        if manifest_hashes[label] != digest:
            raise ValueError(f"{label} artifact does not match the independently trusted SHA-256")
    _require_recent(manifest.generated_at, now=current, max_age=max_age, label="manifest generated_at")
    root = artifact_root.resolve()
    ci_bytes = _read_verified_artifact(manifest.ci, root)
    runtime_bytes = _read_verified_artifact(manifest.runtime, root)
    precommit_bytes = _read_verified_artifact(manifest.protocol_precommit_receipt, root)
    protocol_bytes = _read_verified_artifact(manifest.statistical_protocol, root)
    report_bytes = _read_verified_artifact(manifest.statistical_report, root)
    ci = CiAcceptanceArtifact.model_validate_json(ci_bytes)
    runtime = RuntimeAcceptanceArtifact.model_validate_json(runtime_bytes)
    precommit = ProtocolPrecommitReceipt.model_validate_json(precommit_bytes)
    protocol = StatisticalProtocol.model_validate_json(protocol_bytes)
    report = StatisticalReport.model_validate_json(report_bytes)
    for label, revision in {
        "CI": ci.revision,
        "runtime": runtime.revision,
        "statistical protocol": protocol.revision,
        "statistical report": report.revision,
    }.items():
        if revision != expected_revision:
            raise ValueError(f"{label} revision does not match trusted expected revision")
    _require_recent(ci.generated_at, now=current, max_age=max_age, label="CI generated_at")
    _require_recent(runtime.generated_at, now=current, max_age=max_age, label="runtime generated_at")
    _require_recent(
        runtime.observation_ended_at,
        now=current,
        max_age=max_age,
        label="runtime observation_ended_at",
    )
    loaded_receipts: set[tuple[str, str]] = set()
    for receipt in runtime.claim_receipts:
        _require_recent(
            receipt.observed_at,
            now=current,
            max_age=max_age,
            label=f"runtime receipt {receipt.claim}",
        )
        source_key = (receipt.source.path, receipt.source.sha256)
        if source_key not in loaded_receipts:
            _read_verified_artifact(receipt.source, root)
            loaded_receipts.add(source_key)
    operational_receipts: dict[str, OperationalCostReceiptArtifact] = {}
    for receipt_ref in runtime.operational_cost_receipts:
        receipt_bytes = _read_verified_artifact(receipt_ref, root)
        operational_receipts[receipt_ref.sha256] = (
            OperationalCostReceiptArtifact.model_validate_json(receipt_bytes)
        )
    report_costs = {
        cost.source_reference_sha256: cost for cost in report.operational_costs
    }
    report_cost_total = sum(
        (cost.amount_usd for cost in report.operational_costs), Decimal("0")
    ).quantize(Decimal("0.000001"))
    if report_cost_total != report.metrics.recorded_operational_cost_usd:
        raise ValueError("operational-cost receipt total does not match statistical metrics")
    oos_days = (
        protocol.collection_ended_at.astimezone(ZoneInfo(protocol.time_zone)).date()
        - protocol.oos_started_at.astimezone(ZoneInfo(protocol.time_zone)).date()
    ).days + 1
    frozen_cost_floor = protocol.daily_operational_cost_usd * Decimal(oos_days)
    expected_cost_reconciliation = report_cost_total >= frozen_cost_floor
    if report.metrics.operational_costs_reconciled is not expected_cost_reconciliation:
        raise ValueError("operational-cost reconciliation does not match the frozen cost floor")
    expected_economic_cost = max(frozen_cost_floor, report_cost_total)
    if report.metrics.oos_operational_cost_usd != expected_economic_cost:
        raise ValueError("reported OOS operational cost does not match the frozen protocol")
    expected_net_pnl = (
        report.metrics.oos_gross_profit_after_all_costs_usd
        - report.metrics.oos_gross_loss_after_all_costs_usd
    )
    if report.metrics.oos_net_pnl_after_all_costs_usd != expected_net_pnl:
        raise ValueError("OOS net PnL does not equal gross profit minus gross loss")
    expected_expectancy = (
        Decimal("0")
        if report.metrics.oos_trade_count == 0
        else expected_net_pnl / Decimal(report.metrics.oos_trade_count)
    )
    if report.metrics.oos_expectancy_after_all_costs_usd != expected_expectancy:
        raise ValueError("OOS expectancy does not equal net PnL per trade")
    expected_profit_factor = (
        Decimal("Infinity")
        if report.metrics.oos_gross_loss_after_all_costs_usd == 0
        and report.metrics.oos_gross_profit_after_all_costs_usd > 0
        else (
            Decimal("0")
            if report.metrics.oos_gross_loss_after_all_costs_usd == 0
            else report.metrics.oos_gross_profit_after_all_costs_usd
            / report.metrics.oos_gross_loss_after_all_costs_usd
        )
    )
    if _profit_factor_value(report.metrics.oos_profit_factor) != expected_profit_factor:
        raise ValueError("OOS profit factor is inconsistent with gross profit and loss")
    if report.metrics.closed_trade_count != (
        report.metrics.in_sample_trade_count + report.metrics.oos_trade_count
    ):
        raise ValueError("closed trade count does not equal the IS and OOS cohort counts")
    if set(operational_receipts) != set(report_costs):
        raise ValueError("operational-cost receipt artifacts do not match the statistical report")
    for source_hash, operational_receipt in operational_receipts.items():
        cost = report_costs[source_hash]
        if (
            operational_receipt.account_id != cost.account_id
            or operational_receipt.category != cost.category
            or operational_receipt.amount_usd != cost.amount_usd
            or operational_receipt.incurred_at != cost.incurred_at
        ):
            raise ValueError("operational-cost receipt content does not match the ledger record")
    _require_recent(report.generated_at, now=current, max_age=max_age, label="statistical report generated_at")
    published_at = _require_aware(trusted_protocol_published_at, "trusted protocol published_at")
    if precommit.protocol_sha256 != hashlib.sha256(protocol_bytes).hexdigest():
        raise ValueError("protocol precommit receipt does not reference the verified protocol")
    if (
        protocol.frozen_at != published_at
        or precommit.published_at != published_at
        or published_at >= protocol.collection_started_at
    ):
        raise ValueError("trusted protocol publication does not prove a pre-collection freeze")
    if report.generated_at < protocol.collection_ended_at or current < protocol.collection_ended_at:
        raise ValueError("statistical collection has not ended")
    protocol_digest = hashlib.sha256(protocol_bytes).hexdigest()
    if report.protocol_sha256 != protocol_digest:
        raise ValueError("statistical report does not reference the verified frozen protocol")
    if report.strategy_version_id != protocol.strategy_version_id or report.config_hash != protocol.config_hash:
        raise ValueError("statistical report cohort does not match the frozen protocol")
    if (
        report.collection_started_at != protocol.collection_started_at
        or report.oos_started_at != protocol.oos_started_at
        or report.collection_ended_at != protocol.collection_ended_at
    ):
        raise ValueError("statistical report time boundaries do not match the frozen protocol")
    expected_pass_map = _statistical_pass_map(report.metrics, protocol)
    expected_actuals, expected_descriptions = _criterion_details(report.metrics, protocol)
    by_name = {criterion.name: criterion for criterion in report.criteria}
    if len(by_name) != len(report.criteria) or set(by_name) != REQUIRED_STATISTICAL_CRITERIA:
        raise ValueError("statistical criteria are missing or duplicated")
    for name, expected_pass in expected_pass_map.items():
        if by_name[name].passed is not expected_pass:
            raise ValueError(f"statistical criterion is inconsistent: {name}")
        if by_name[name].actual != expected_actuals[name] or by_name[name].expected != expected_descriptions[name]:
            raise ValueError(f"statistical criterion display values are inconsistent: {name}")
    if report.passed is not all(expected_pass_map.values()) or not report.passed:
        failed = sorted(name for name, passed in expected_pass_map.items() if not passed)
        raise ValueError(f"statistical acceptance failed: {failed}")
    return manifest
