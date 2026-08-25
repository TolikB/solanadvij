"""Configuration loading and validation."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]
from pydantic import AliasChoices, BaseModel, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .errors import LiveTradingNotImplementedError


class AppMode(StrEnum):
    RECORD = "record"
    PAPER = "paper"
    LIVE = "live"


class RiskConfig(BaseModel):
    risk_per_trade_pct: Decimal = Decimal("0.005")
    hard_stop_pct: Decimal = Decimal("0.15")
    max_position_usdc: Decimal = Decimal("20")
    min_position_usdc: Decimal = Decimal("8")
    max_exposure_usdc: Decimal = Decimal("50")
    max_open_positions: int = 3
    daily_loss_limit_usdc: Decimal = Decimal("10")
    all_time_drawdown_limit_pct: Decimal = Decimal("10")
    max_trades_per_day: int = 12
    max_consecutive_losses: int = 3
    daily_halt_after_consecutive_losses: int = 4
    pause_minutes: int = 60
    adverse_fill_bps: int = 50


class ChainConfig(BaseModel):
    network: str = "mainnet-beta"
    primary_commitment: str = "confirmed"
    max_stream_lag_ms: int = 3000
    warmup_seconds: int = 60


class PaperConfig(BaseModel):
    execution_delay_ms: int = 1200
    adverse_fill_bps: int = 50
    exit_retry_interval_ms: int = 1000
    exit_retry_timeout_seconds: int = 30
    account_currency: str = "USDC"


class CandidateConfig(BaseModel):
    min_pool_age_seconds: int = 45
    max_pool_age_seconds: int = 180
    min_observation_seconds: int = 45
    max_return_since_pool_creation_pct: Decimal = Decimal("2.50")
    min_pullback_pct: Decimal = Decimal("0.10")
    max_pullback_pct: Decimal = Decimal("0.25")
    score_watchlist: Decimal = Decimal("72")
    score_entry: Decimal = Decimal("80")
    score_confirmation_windows: int = 2
    score_window_seconds: int = 5


class LiquidityConfig(BaseModel):
    min_quote_liquidity_usd: Decimal = Decimal("40000")
    max_market_cap_to_quote_liquidity: Decimal = Decimal("25")
    max_position_to_quote_liquidity_pct: Decimal = Decimal("0.0025")
    max_liquidity_drop_entry_30s_pct: Decimal = Decimal("0.03")
    emergency_liquidity_drop_pct: Decimal = Decimal("0.08")


class ExecutionConfig(BaseModel):
    quote_timeout_ms: int = 2500
    max_quote_age_ms: int = 1500
    max_quote_retries: int = 2
    max_buy_price_impact_pct: Decimal = Decimal("0.025")
    max_sell_price_impact_pct: Decimal = Decimal("0.035")
    max_round_trip_loss_pct: Decimal = Decimal("0.08")
    min_external_sellers: int = 5


class FlowConfig(BaseModel):
    min_unique_buyers_60s: int = 25
    min_buyer_acceleration: Decimal = Decimal("1.3")
    min_unique_buyer_ratio: Decimal = Decimal("0.30")
    max_transactions_per_trader: Decimal = Decimal("4")
    min_buy_sell_volume_ratio: Decimal = Decimal("1.5")
    max_buy_sell_volume_ratio: Decimal = Decimal("5")
    max_top_5_buy_volume_share: Decimal = Decimal("0.35")
    max_same_funder_buy_share: Decimal = Decimal("0.20")


class HolderConfig(BaseModel):
    max_largest_holder_pct: Decimal = Decimal("0.07")
    max_top_5_pct: Decimal = Decimal("0.22")
    max_top_10_pct: Decimal = Decimal("0.30")
    max_dev_holding_pct: Decimal = Decimal("0.02")
    max_dev_cluster_pct: Decimal = Decimal("0.05")
    max_related_cluster_pct: Decimal = Decimal("0.15")
    max_unknown_supply_pct: Decimal = Decimal("0.05")


class ExitConfig(BaseModel):
    take_profit_1_pct: Decimal = Decimal("0.30")
    take_profit_1_size_pct: Decimal = Decimal("0.50")
    take_profit_2_pct: Decimal = Decimal("0.60")
    take_profit_2_size_pct: Decimal = Decimal("0.25")
    trailing_stop_pct: Decimal = Decimal("0.15")
    momentum_exit_windows: int = 2
    no_new_high_timeout_seconds: int = 120
    maximum_holding_seconds: int = 600


class TelegramConfig(BaseModel):
    enabled: bool = True
    daily_report_time: str = "00:05"
    include_all_time_with_daily: bool = True

    @field_validator("daily_report_time")
    @classmethod
    def validate_daily_report_time(cls, value: str) -> str:
        try:
            hour_text, minute_text = value.split(":", maxsplit=1)
            hour, minute = int(hour_text), int(minute_text)
        except (TypeError, ValueError) as exc:
            raise ValueError("daily_report_time must use HH:MM") from exc
        if not 0 <= hour <= 23 or not 0 <= minute <= 59:
            raise ValueError("daily_report_time must use a valid 24-hour time")
        return f"{hour:02d}:{minute:02d}"


class EnrichmentConfig(BaseModel):
    enabled: bool = True
    timeout_ms: int = Field(2500, gt=0)
    cache_seconds: int = Field(300, ge=0)
    minimum_interval_ms: int = Field(250, ge=0)


class StorageConfig(BaseModel):
    raw_events_enabled: bool = True
    raw_compression: str = "zstd"
    raw_retention_days: int = Field(90, ge=90)


class ReportingConfig(BaseModel):
    include_operational_costs: bool = True
    monthly_infrastructure_cost_usd: Decimal = Decimal("0")
    minimum_sample_warning_trades: int = 30


class LoggingConfig(BaseModel):
    level: str = "INFO"
    json_logs: bool = True


class AppConfig(BaseSettings):
    """Application settings loaded from YAML + environment."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_mode: AppMode = Field(..., alias="APP_MODE")
    time_zone: str = Field("Europe/Kyiv", alias="TIME_ZONE")
    base_quote_mint: str = Field(
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        alias="BASE_QUOTE_MINT",
    )
    base_quote_decimals: int = Field(6, alias="BASE_QUOTE_DECIMALS")
    replay_mode: bool = Field(False, alias="JUPITER_REPLAY_MODE")
    quote_journal_path: str = Field("", alias="JUPITER_QUOTE_JOURNAL_PATH")
    quote_journal_record: bool = Field(False, alias="JUPITER_QUOTE_JOURNAL_RECORD")
    replay_seed: int | None = Field(default=None, alias="REPLAY_SEED")
    helius_api_key: str = Field(..., alias="HELIUS_API_KEY")
    helius_wss_url: str = Field("", alias="HELIUS_WSS_URL", exclude=True)
    helius_rpc_url: str = Field("", alias="HELIUS_RPC_URL", exclude=True)
    jupiter_api_key: SecretStr = Field(..., alias="JUPITER_API_KEY")
    postgres_dsn: str = Field(..., alias="POSTGRES_DSN")
    telegram_bot_token: SecretStr = Field(..., alias="TELEGRAM_BOT_TOKEN")
    telegram_admin_chat_id: int = Field(..., alias="TELEGRAM_ADMIN_CHAT_ID")
    telegram_allowlist_chat_ids: list[int] = Field(
        default_factory=list,
        validation_alias=AliasChoices(
            "TELEGRAM_ALLOWED_CHAT_IDS", "TELEGRAM_ALLOWLIST_CHAT_IDS"
        ),
        serialization_alias="TELEGRAM_ALLOWED_CHAT_IDS",
    )
    telegram_allowlist_user_ids: list[int] = Field(
        default_factory=list,
        alias="TELEGRAM_ALLOWED_USER_IDS",
    )
    starting_equity_usd: Decimal = Field(Decimal("500"), alias="STARTING_EQUITY_USD")
    risk: RiskConfig = Field(default_factory=RiskConfig)
    chain: ChainConfig = Field(default_factory=ChainConfig)
    paper: PaperConfig = Field(default_factory=PaperConfig)
    candidate: CandidateConfig = Field(default_factory=CandidateConfig)
    liquidity: LiquidityConfig = Field(default_factory=LiquidityConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    flow: FlowConfig = Field(default_factory=FlowConfig)
    holders: HolderConfig = Field(default_factory=HolderConfig)
    exits: ExitConfig = Field(default_factory=ExitConfig)
    telegram: TelegramConfig = Field(default_factory=TelegramConfig)
    enrichment: EnrichmentConfig = EnrichmentConfig(
        timeout_ms=2500,
        cache_seconds=300,
        minimum_interval_ms=250,
    )
    storage: StorageConfig = StorageConfig(raw_retention_days=90)
    reporting: ReportingConfig = Field(default_factory=ReportingConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    # Runtime fields
    config_hash: str = Field(default="", exclude=True)
    strategy_version: str = Field(default="", exclude=True)
    release_revision: str = Field(
        default_factory=lambda: os.environ.get("APP_REVISION", "").strip().lower(),
        exclude=True,
        validation_alias=AliasChoices("APP_REVISION", "release_revision"),
    )

    @field_validator("release_revision")
    @classmethod
    def validate_release_revision(cls, value: str) -> str:
        value = value.strip().lower()
        if not value:
            return value
        if len(value) != 40:
            raise ValueError("APP_REVISION must be a 40-character Git commit")
        try:
            int(value, 16)
        except ValueError as exc:
            raise ValueError("APP_REVISION must be hexadecimal") from exc
        if value == "0" * 40:
            raise ValueError("APP_REVISION must identify a real Git commit")
        return value

    @field_validator("starting_equity_usd")
    @classmethod
    def validate_starting_equity(cls, value: Decimal) -> Decimal:
        if value <= 0:
            raise ValueError("STARTING_EQUITY_USD must be greater than 0")
        return value

    @field_validator("helius_api_key", "postgres_dsn", "base_quote_mint")
    @classmethod
    def validate_non_empty_text_fields(cls, value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("must be a non-empty string")
        return value.strip()

    @field_validator("quote_journal_path")
    @classmethod
    def validate_quote_journal_path(cls, value: str) -> str:
        return (value or "").strip()

    @field_validator("telegram_admin_chat_id")
    @classmethod
    def validate_admin_chat_id(cls, value: int) -> int:
        if value == 0:
            raise ValueError("TELEGRAM_ADMIN_CHAT_ID must not be 0")
        return value

    @field_validator("base_quote_decimals")
    @classmethod
    def validate_quote_decimals(cls, value: int) -> int:
        if value <= 0 or value > 18:
            raise ValueError("BASE_QUOTE_DECIMALS must be in range 1..18")
        return value

    @field_validator("jupiter_api_key")
    @classmethod
    def validate_secret(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().strip():
            raise ValueError("JUPITER_API_KEY must be non-empty")
        return value

    @field_validator("telegram_bot_token")
    @classmethod
    def validate_telegram_token(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().strip():
            raise ValueError("TELEGRAM_BOT_TOKEN must be non-empty")
        return value

    @field_validator("telegram_allowlist_chat_ids", "telegram_allowlist_user_ids", mode="before")
    @classmethod
    def validate_telegram_allowlist(cls, value: Any) -> list[int]:
        if value is None or value == "":
            return []
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return []
            if text.startswith("[") and text.endswith("]"):
                parsed = json.loads(text)
            else:
                parsed = [item.strip() for item in text.split(",") if item.strip()]
            return list(_intify_telegram_ids(parsed))
        if isinstance(value, list):
            return list(_intify_telegram_ids(value))
        raise TypeError(
            "Telegram allowlist must be a list[int], comma-separated string, or JSON array"
        )

    @model_validator(mode="after")
    def validate_mode_and_hash(self) -> "AppConfig":
        if self.app_mode == AppMode.LIVE:
            raise LiveTradingNotImplementedError()
        if (self.replay_mode or self.quote_journal_record) and not self.quote_journal_path:
            raise ValueError(
                "JUPITER_QUOTE_JOURNAL_PATH must be set when replay or record is enabled"
            )
        if self.paper.adverse_fill_bps != self.risk.adverse_fill_bps:
            raise ValueError("paper.adverse_fill_bps and risk.adverse_fill_bps must match")
        if self.candidate.min_pool_age_seconds < self.candidate.min_observation_seconds:
            raise ValueError("candidate min pool age cannot be shorter than observation window")
        if self.candidate.max_pool_age_seconds <= self.candidate.min_pool_age_seconds:
            raise ValueError("candidate max pool age must exceed min pool age")
        self.config_hash = self._compute_hash()
        self.strategy_version = (
            f"{self.config_hash[:16]}-{self.release_revision[:12]}"
            if self.release_revision
            else self.config_hash[:16]
        )
        return self

    def _compute_hash(self) -> str:
        payload = self._normalized_payload()
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _normalized_payload(self) -> dict[str, Any]:
        data = self.model_dump(
            mode="json",
            by_alias=True,
            exclude={
                "config_hash",
                "strategy_version",
                "release_revision",
                "telegram_bot_token",
                "jupiter_api_key",
                "helius_api_key",
                "helius_wss_url",
                "helius_rpc_url",
                "postgres_dsn",
            },
        )
        data.pop("JUPITER_QUOTE_JOURNAL_PATH", None)
        for key in (
            "jupiter_api_key",
            "telegram_bot_token",
            "helius_api_key",
            "helius_wss_url",
            "helius_rpc_url",
            "postgres_dsn",
        ):
            data.pop(key, None)
        data["risk"] = data.get("risk", {})
        return data

    def masked_view(self) -> dict[str, Any]:
        dumped = self.model_dump(
            mode="json",
            by_alias=True,
            exclude={
                "jupiter_api_key",
                "telegram_bot_token",
                "helius_api_key",
                "helius_wss_url",
                "helius_rpc_url",
                "postgres_dsn",
            },
        )
        for key in (
            "JUPITER_API_KEY",
            "TELEGRAM_BOT_TOKEN",
            "HELIUS_API_KEY",
            "HELIUS_WSS_URL",
            "HELIUS_RPC_URL",
            "POSTGRES_DSN",
        ):
            dumped[key] = "***"
        dumped["config_hash"] = self.config_hash
        dumped["strategy_version"] = self.strategy_version
        return dumped

    def resolved_helius_wss_url(self) -> str:
        if self.helius_wss_url:
            return self.helius_wss_url
        return f"wss://mainnet.helius-rpc.com/?api-key={self.helius_api_key}"

    def resolved_helius_rpc_url(self) -> str:
        if self.helius_rpc_url:
            return self.helius_rpc_url
        return f"https://mainnet.helius-rpc.com/?api-key={self.helius_api_key}"

    @classmethod
    def _coerce_yaml_values(cls, raw: Mapping[str, Any]) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        for key, value in raw.items():
            if value is None:
                continue
            if isinstance(key, str):
                merged[key] = value
        return merged

    @classmethod
    def load(cls, config_path: str | None = None) -> "AppConfig":
        yaml_values: dict[str, Any] = {}
        config_file = config_path or os.getenv("CONFIG_PATH")
        path = Path(config_file).expanduser().resolve() if config_file else None
        if path and path.exists() and path.is_file():
            with path.open("r", encoding="utf-8") as stream:
                loaded = yaml.safe_load(stream) or {}
            if not isinstance(loaded, dict):
                raise ValueError(
                    f"CONFIG_PATH must contain YAML object, got {type(loaded).__name__}"
                )
            yaml_values = cls._coerce_yaml_values(loaded)

        # Pydantic init values outrank BaseSettings environment sources, so
        # remove every YAML spelling when a non-empty environment value exists.
        # BaseSettings then performs its normal case handling and JSON decoding.
        case_sensitive = bool(cls.model_config.get("case_sensitive", False))
        environment = {
            key if case_sensitive else key.casefold(): value
            for key, value in os.environ.items()
        }
        for field_name, model_field in cls.model_fields.items():
            validation_aliases: list[str] = []
            for alias in (model_field.alias, model_field.validation_alias):
                if isinstance(alias, str):
                    validation_aliases.append(alias)
                elif isinstance(alias, AliasChoices):
                    validation_aliases.extend(
                        choice for choice in alias.choices if isinstance(choice, str)
                    )

            env_names = list(dict.fromkeys(validation_aliases or [field_name]))
            for env_name in env_names:
                lookup_name = env_name if case_sensitive else env_name.casefold()
                if not environment.get(lookup_name):
                    continue

                yaml_values.pop(field_name, None)
                for alias in env_names:
                    yaml_values.pop(alias, None)
                break

        return cls(**yaml_values)


def _intify_telegram_ids(items: list[object]) -> list[int]:
    ids: list[int] = []
    for item in items:
        if isinstance(item, str) and item.strip() == "":
            continue
        if isinstance(item, bool) or not isinstance(item, (int, str)):
            raise ValueError("Telegram allowlist entries must be integer IDs")
        ids.append(int(item))

    # keep unique IDs, preserve order
    seen: set[int] = set()
    unique_ids: list[int] = []
    for chat_id in ids:
        if chat_id in seen:
            continue
        if chat_id == 0:
            raise ValueError("TELEGRAM_ALLOWLIST_CHAT_IDS entries must be non-zero")
        seen.add(chat_id)
        unique_ids.append(chat_id)
    return unique_ids
