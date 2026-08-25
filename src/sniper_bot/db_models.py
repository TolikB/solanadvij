"""SQLAlchemy schema for durable audit, paper trading, and replay state."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

MONEY = Numeric(38, 18)


class Base(DeclarativeBase):
    pass


class StrategyVersionRow(Base):
    __tablename__ = "strategy_versions"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    version: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    config_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    config_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    git_commit: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    activated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    deactivated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class SystemRunRow(Base):
    __tablename__ = "system_runs"
    __table_args__ = (
        Index(
            "uq_system_runs_one_active",
            text("(1)"),
            unique=True,
            postgresql_where=text("stopped_at IS NULL"),
            sqlite_where=text("stopped_at IS NULL"),
        ),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    stopped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    strategy_version_id: Mapped[str] = mapped_column(ForeignKey("strategy_versions.id"), nullable=False)
    hostname: Mapped[str] = mapped_column(String(255), nullable=False)
    app_version: Mapped[str] = mapped_column(String(64), nullable=False)
    stop_reason: Mapped[str | None] = mapped_column(Text)
    last_heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class EventDedupRow(Base):
    __tablename__ = "event_dedup"
    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    block_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processing_status: Mapped[str] = mapped_column(
        String(16), nullable=False, default="PENDING", server_default="PROCESSED", index=True
    )
    processing_attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default="1"
    )
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(String(128))
    processing_token: Mapped[str | None] = mapped_column(String(36))


class RawChainEventRow(Base):
    __tablename__ = "raw_chain_events"
    __table_args__ = (
        UniqueConstraint("event_id", "block_date", name="uq_raw_event_partition"),
        Index("ix_raw_chain_events_slot", "slot"),
        Index("ix_raw_chain_events_signature", "signature"),
        Index("ix_raw_chain_events_mint_time", "mint", "block_time"),
        Index("ix_raw_chain_events_pool_time", "pool_address", "block_time"),
        {"postgresql_partition_by": "RANGE (block_date)"},
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    block_date: Mapped[date] = mapped_column(Date, primary_key=True)
    event_id: Mapped[str] = mapped_column(String(64), nullable=False)
    ingest_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    protocol: Mapped[str] = mapped_column(String(32), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    slot: Mapped[int] = mapped_column(BigInteger, nullable=False)
    signature: Mapped[str] = mapped_column(String(128), nullable=False)
    instruction_index: Mapped[int] = mapped_column(Integer, nullable=False)
    inner_instruction_index: Mapped[int] = mapped_column(Integer, nullable=False)
    block_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    commitment: Mapped[str] = mapped_column(String(16), nullable=False)
    mint: Mapped[str | None] = mapped_column(String(64))
    pool_address: Mapped[str | None] = mapped_column(String(64))
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class TokenRow(Base):
    __tablename__ = "tokens"
    mint: Mapped[str] = mapped_column(String(64), primary_key=True)
    token_program: Mapped[str | None] = mapped_column(String(64))
    name: Mapped[str | None] = mapped_column(String(255))
    symbol: Mapped[str | None] = mapped_column(String(64))
    decimals: Mapped[int | None] = mapped_column(Integer)
    total_supply_raw: Mapped[Decimal | None] = mapped_column(MONEY)
    creator_address: Mapped[str | None] = mapped_column(String(64), index=True)
    creation_signature: Mapped[str | None] = mapped_column(String(128))
    creation_slot: Mapped[int | None] = mapped_column(BigInteger)
    creation_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    metadata_uri: Mapped[str | None] = mapped_column(Text)
    metadata_mutable: Mapped[bool | None] = mapped_column(Boolean)
    enrichment_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    enriched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class PoolRow(Base):
    __tablename__ = "pools"
    pool_address: Mapped[str] = mapped_column(String(64), primary_key=True)
    mint: Mapped[str] = mapped_column(ForeignKey("tokens.mint"), nullable=False, index=True)
    protocol: Mapped[str] = mapped_column(String(32), nullable=False)
    quote_mint: Mapped[str] = mapped_column(String(64), nullable=False)
    base_vault: Mapped[str | None] = mapped_column(String(64))
    quote_vault: Mapped[str | None] = mapped_column(String(64))
    creation_signature: Mapped[str] = mapped_column(String(128), nullable=False)
    creation_slot: Mapped[int] = mapped_column(BigInteger, nullable=False)
    creation_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    migration_signature: Mapped[str | None] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class MarketSnapshotRow(Base):
    __tablename__ = "market_snapshots"
    __table_args__ = (
        Index("ix_market_snapshots_pool_time", "pool_address", "snapshot_time"),
        {"postgresql_partition_by": "RANGE (snapshot_date)"},
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    snapshot_date: Mapped[date] = mapped_column(Date, primary_key=True)
    pool_address: Mapped[str] = mapped_column(ForeignKey("pools.pool_address"), nullable=False)
    snapshot_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    price_usd: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    quote_liquidity_usd: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    base_liquidity_usd: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    market_cap_estimate: Mapped[Decimal | None] = mapped_column(MONEY)
    volume_15s: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    volume_30s: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    volume_60s: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    unique_buyers_60s: Mapped[int] = mapped_column(Integer, nullable=False)
    unique_sellers_60s: Mapped[int] = mapped_column(Integer, nullable=False)
    holder_count: Mapped[int] = mapped_column(Integer, nullable=False)
    features_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    data_quality_flags: Mapped[list[str]] = mapped_column(JSON, nullable=False)


class TokenSecurityCheckRow(Base):
    __tablename__ = "token_security_checks"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    mint: Mapped[str] = mapped_column(ForeignKey("tokens.mint"), nullable=False, index=True)
    checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    token_program: Mapped[str] = mapped_column(String(64), nullable=False)
    mint_authority: Mapped[str | None] = mapped_column(String(64))
    freeze_authority: Mapped[str | None] = mapped_column(String(64))
    extensions_json: Mapped[list[Any]] = mapped_column(JSON, nullable=False)
    largest_holder_pct: Mapped[Decimal | None] = mapped_column(MONEY)
    top_5_pct: Mapped[Decimal | None] = mapped_column(MONEY)
    top_10_pct: Mapped[Decimal | None] = mapped_column(MONEY)
    dev_holding_pct: Mapped[Decimal | None] = mapped_column(MONEY)
    dev_cluster_pct: Mapped[Decimal | None] = mapped_column(MONEY)
    largest_related_cluster_pct: Mapped[Decimal | None] = mapped_column(MONEY)
    unknown_supply_pct: Mapped[Decimal | None] = mapped_column(MONEY)
    buy_route_available: Mapped[bool] = mapped_column(Boolean, nullable=False)
    sell_route_available: Mapped[bool] = mapped_column(Boolean, nullable=False)
    hard_reject: Mapped[bool] = mapped_column(Boolean, nullable=False)
    reject_reasons_json: Mapped[list[str]] = mapped_column(JSON, nullable=False)


class WalletProfileRow(Base):
    __tablename__ = "wallet_profiles"
    wallet_address: Mapped[str] = mapped_column(String(64), primary_key=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    initial_funder: Mapped[str | None] = mapped_column(String(64), index=True)
    funding_signature: Mapped[str | None] = mapped_column(String(128))
    known_creator: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    tokens_created: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    tokens_traded: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    tokens_created_7d: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    tokens_created_30d: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    tokens_reaching_pumpswap: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    tokens_reaching_2x_executable: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    tokens_with_liquidity_rug: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    tokens_with_dev_dump_5m: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    median_peak_return: Mapped[Decimal] = mapped_column(MONEY, nullable=False, default=0)
    median_token_lifetime_seconds: Mapped[Decimal] = mapped_column(MONEY, nullable=False, default=0)
    median_dev_sell_delay_seconds: Mapped[Decimal] = mapped_column(MONEY, nullable=False, default=0)
    last_token_created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    known_funding_cluster: Mapped[str | None] = mapped_column(String(64))
    profile_updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class WalletRelationRow(Base):
    __tablename__ = "wallet_relations"
    __table_args__ = (UniqueConstraint("wallet_a", "wallet_b", name="uq_wallet_relation"),)
    wallet_a: Mapped[str] = mapped_column(String(64), primary_key=True)
    wallet_b: Mapped[str] = mapped_column(String(64), primary_key=True)
    relation_score: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    relation_types_json: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    first_detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class CandidateRow(Base):
    __tablename__ = "candidates"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    mint: Mapped[str] = mapped_column(ForeignKey("tokens.mint"), nullable=False, index=True)
    pool_address: Mapped[str] = mapped_column(ForeignKey("pools.pool_address"), nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    eligible_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    armed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    rejected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reject_reason: Mapped[str | None] = mapped_column(String(64), index=True)
    strategy_version_id: Mapped[str] = mapped_column(ForeignKey("strategy_versions.id"), nullable=False)
    config_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    runtime_state_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)


class SignalEvaluationRow(Base):
    __tablename__ = "signal_evaluations"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    candidate_id: Mapped[str] = mapped_column(ForeignKey("candidates.id"), nullable=False, index=True)
    evaluated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    score: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    organic_score: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    distribution_score: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    execution_score: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    liquidity_score: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    developer_score: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    price_structure_score: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    features_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    rules_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


class ExternalApiCallRow(Base):
    __tablename__ = "external_api_calls"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    provider: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    endpoint: Mapped[str] = mapped_column(String(255), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    latency_ms: Mapped[int | None] = mapped_column(Integer)
    http_status: Mapped[int | None] = mapped_column(Integer)
    request_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    response_json: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    error_code: Mapped[str | None] = mapped_column(String(64))


class PaperAccountRow(Base):
    __tablename__ = "paper_accounts"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    base_currency: Mapped[str] = mapped_column(String(16), nullable=False)
    starting_equity: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    cash_balance: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    locked_capital: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    realized_pnl: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    unrealized_pnl: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    simulated_costs: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    operational_costs: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    equity: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    peak_equity: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    drawdown_pct: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    halt_reason: Mapped[str | None] = mapped_column(String(128))
    pause_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    daily_halt_date: Mapped[str | None] = mapped_column(String(10))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class OperationalCostRow(Base):
    __tablename__ = "operational_costs"
    __table_args__ = (
        CheckConstraint(
            "length(source_reference_sha256) = 64 "
            "AND source_reference_sha256 = lower(source_reference_sha256) "
            "AND replace(replace(replace(replace(replace(replace(replace(replace("
            "replace(replace(replace(replace(replace(replace(replace(replace("
            "source_reference_sha256, '0', ''), '1', ''), '2', ''), '3', ''), "
            "'4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), "
            "'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''",
            name="ck_operational_cost_source_sha256",
        ),
        Index("ix_operational_costs_account_time", "account_id", "incurred_at"),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    account_id: Mapped[str] = mapped_column(ForeignKey("paper_accounts.id"), nullable=False)
    category: Mapped[str] = mapped_column(String(64), nullable=False)
    amount_usd: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    incurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source_reference_sha256: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class PaperEquityMarkRow(Base):
    __tablename__ = "paper_equity_marks"
    __table_args__ = (
        Index("ix_paper_equity_marks_account_time", "account_id", "observed_at"),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    account_id: Mapped[str] = mapped_column(
        ForeignKey("paper_accounts.id"), nullable=False
    )
    equity: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    realized_pnl: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    unrealized_pnl: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    locked_capital: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class PaperOrderRow(Base):
    __tablename__ = "paper_orders"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    candidate_id: Mapped[str | None] = mapped_column(ForeignKey("candidates.id"))
    position_id: Mapped[str | None] = mapped_column(String(36), index=True)
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    requested_usd: Mapped[Decimal | None] = mapped_column(MONEY)
    requested_token_raw: Mapped[Decimal | None] = mapped_column(MONEY)
    quote_request_id: Mapped[str | None] = mapped_column(ForeignKey("external_api_calls.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    filled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    rejected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reject_reason: Mapped[str | None] = mapped_column(String(64))


class PaperFillRow(Base):
    __tablename__ = "paper_fills"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    order_id: Mapped[str] = mapped_column(ForeignKey("paper_orders.id"), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    input_raw_amount: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    output_raw_amount: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    input_usd: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    output_usd: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    execution_price: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    quoted_price: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    adverse_fill_bps: Mapped[int] = mapped_column(Integer, nullable=False)
    price_impact_pct: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    platform_fee_usd: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    network_fee_usd: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    other_cost_usd: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    cost_basis_usd: Mapped[Decimal] = mapped_column(MONEY, nullable=False, default=0)
    realized_pnl_usd: Mapped[Decimal] = mapped_column(MONEY, nullable=False, default=0)
    exit_reason: Mapped[str | None] = mapped_column(String(64))
    strategy_version_id: Mapped[str] = mapped_column(
        ForeignKey("strategy_versions.id"), nullable=False
    )
    config_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    filled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)


class PaperPositionRow(Base):
    __tablename__ = "paper_positions"
    __table_args__ = (
        Index(
            "uq_open_paper_position_mint",
            "mint",
            unique=True,
            postgresql_where=text("status IN ('OPEN', 'PARTIAL')"),
            sqlite_where=text("status IN ('OPEN', 'PARTIAL')"),
        ),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    mint: Mapped[str] = mapped_column(ForeignKey("tokens.mint"), nullable=False, index=True)
    pool_address: Mapped[str] = mapped_column(ForeignKey("pools.pool_address"), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    strategy_version_id: Mapped[str] = mapped_column(ForeignKey("strategy_versions.id"), nullable=False)
    entry_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    initial_cost_usd: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    remaining_cost_usd: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    initial_token_amount_raw: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    token_amount_raw: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    open_fill_id: Mapped[str] = mapped_column(ForeignKey("paper_fills.id"), nullable=False)
    realized_pnl: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    unrealized_pnl: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    mfe_pct: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    mae_pct: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    highest_executable_value: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    lowest_executable_value: Mapped[Decimal] = mapped_column(MONEY, nullable=False)
    tp1_taken: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    tp2_taken: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    last_new_high_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    config_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    exit_reason: Mapped[str | None] = mapped_column(String(64))


class RiskEventRow(Base):
    __tablename__ = "risk_events"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    position_id: Mapped[str | None] = mapped_column(ForeignKey("paper_positions.id"))
    candidate_id: Mapped[str | None] = mapped_column(ForeignKey("candidates.id"))
    details_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class DailyReportRow(Base):
    __tablename__ = "daily_reports"
    __table_args__ = (
        UniqueConstraint("report_date", "timezone", "strategy_version_id", name="uq_daily_report"),
    )
    report_date: Mapped[date] = mapped_column(Date, primary_key=True)
    timezone: Mapped[str] = mapped_column(String(64), primary_key=True)
    strategy_version_id: Mapped[str] = mapped_column(ForeignKey("strategy_versions.id"), primary_key=True)
    report_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    telegram_message_id: Mapped[str | None] = mapped_column(String(64))
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OutboxEventRow(Base):
    __tablename__ = "outbox_events"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    delivery_state: Mapped[str] = mapped_column(
        String(16), nullable=False, default="PENDING", server_default="PENDING", index=True
    )
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    claim_token: Mapped[str | None] = mapped_column(String(36))
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    telegram_message_id: Mapped[str | None] = mapped_column(String(64))
    last_error: Mapped[str | None] = mapped_column(Text)


class ReplayRunRow(Base):
    __tablename__ = "replay_runs"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    strategy_version_id: Mapped[str] = mapped_column(ForeignKey("strategy_versions.id"), nullable=False)
    config_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    random_seed: Mapped[int | None] = mapped_column(BigInteger)
    input_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    output_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    speed: Mapped[str] = mapped_column(String(16), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    result_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


class RuntimeCheckpointRow(Base):
    __tablename__ = "runtime_checkpoints"
    checkpoint_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    state_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
