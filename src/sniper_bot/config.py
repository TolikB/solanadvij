"""Configuration loading and validation."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

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
    # Position size caps as fractions of equity and of the pool's quote side.
    max_position_equity_pct: Decimal = Decimal("0.04")
    # Extra loss assumed beyond the stop when sizing (slippage through it).
    adverse_execution_buffer_pct: Decimal = Decimal("0.01")
    max_exposure_usdc: Decimal = Decimal("50")
    max_open_positions: int = 3
    daily_loss_limit_usdc: Decimal = Decimal("10")
    all_time_drawdown_limit_pct: Decimal = Decimal("10")
    max_trades_per_day: int = 12
    max_consecutive_losses: int = 3
    daily_halt_after_consecutive_losses: int = 4
    pause_minutes: int = 60


class ChainConfig(BaseModel):
    network: str = "mainnet-beta"
    primary_commitment: str = "confirmed"
    max_stream_lag_ms: int = 3000
    warmup_seconds: int = 60
    # A checkpoint older than the bounded recovery window cannot be backfilled
    # before the live buffer overflows, so by default the bot records the gap
    # permanently as an ACCEPTED row and resumes on a fresh non-tradable
    # baseline. It never trades over the hole: entries stay blocked through the
    # baseline warmup, and the recorded range is excluded from canonical replay
    # evidence. Set this to halt instead and wait for a human.
    halt_on_unrecoverable_gap: bool = False


class PaperConfig(BaseModel):
    # Signal-to-landing latency, applied to entries and exits alike.
    execution_delay_ms: int = 1200
    adverse_fill_bps: int = 50
    # A real swap carries a minimum output; when the price moves further
    # than this during the delay the transaction fails and only pays fees.
    max_entry_slippage_bps: int = Field(default=300, ge=0, le=5000)
    # Charged per transaction when the quote reports no network fee at all
    # (5000 lamports is the base signature fee; priority fees are measured).
    min_network_fee_lamports: int = Field(default=5000, ge=0)
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
    # Two qualifying scores still confirm when the evaluation loop slows down;
    # only a longer silence (or any score below the entry bar) starts over.
    score_confirmation_max_gap_seconds: int = Field(default=15, ge=6)


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
    # Candidate security inputs are refreshed on a cadence instead of every
    # evaluation: round-trip quotes feed the score every few seconds and are
    # always re-quoted for the entry decision itself (max_quote_age_ms);
    # holder snapshots stay within the 15 s holder staleness bound.
    quote_refresh_seconds: int = Field(default=5, ge=1)
    holder_refresh_seconds: int = Field(default=10, ge=1, le=14)


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
    # The holder index must account for the mint supply within this fraction
    # (0 = exactly) and lag the confirmed slot by at most this many slots.
    index_supply_tolerance_pct: Decimal = Field(default=Decimal("0"), ge=0, le=Decimal("0.01"))
    max_index_slot_lag: int = Field(default=20, ge=1, le=150)


class ExitConfig(BaseModel):
    take_profit_1_pct: Decimal = Decimal("0.30")
    take_profit_1_size_pct: Decimal = Decimal("0.50")
    take_profit_2_pct: Decimal = Decimal("0.60")
    take_profit_2_size_pct: Decimal = Decimal("0.25")
    trailing_stop_pct: Decimal = Decimal("0.15")
    momentum_exit_windows: int = 2
    no_new_high_timeout_seconds: int = 120
    maximum_holding_seconds: int = 600
    # Where open positions are valued for exit decisions: a Jupiter sell quote
    # every pass, or the tracked pool reserves (no provider call; Jupiter still
    # fills). Both are always compared and logged; switch only by the rule.
    mark_source: Literal["jupiter", "reserves"] = "jupiter"


class TelegramConfig(BaseModel):
    enabled: bool = True
    daily_report_time: str = "00:00"
    include_all_time_with_daily: bool = False

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


class ScoreAnchor(BaseModel):
    """One score component: zero points at one bound, full points at the other.

    For rising components (more is better) ``zero`` < ``full``; for falling
    ones (less is better) ``zero`` > ``full``.
    """

    zero: Decimal
    full: Decimal
    points: Decimal = Field(gt=0)

    @model_validator(mode="after")
    def bounds_differ(self) -> "ScoreAnchor":
        if self.zero == self.full:
            raise ValueError("score anchor bounds must differ")
        return self


class ScoringConfig(BaseModel):
    """Score anchors, independent of the entry thresholds on purpose.

    The calibration funnel replays stricter liquidity and buyer thresholds on
    candidates scored during the loosest rung; that is only exact while a
    threshold change leaves every score as it was.
    """

    unique_buyers_60s: ScoreAnchor = ScoreAnchor(zero=Decimal("5"), full=Decimal("25"), points=Decimal("8"))
    buyer_acceleration: ScoreAnchor = ScoreAnchor(zero=Decimal("0.8"), full=Decimal("1.5"), points=Decimal("6"))
    unique_buyer_ratio: ScoreAnchor = ScoreAnchor(zero=Decimal("0.2"), full=Decimal("0.75"), points=Decimal("4"))
    buy_sell_volume_ratio: ScoreAnchor = ScoreAnchor(zero=Decimal("1"), full=Decimal("3"), points=Decimal("4"))
    transactions_per_trader: ScoreAnchor = ScoreAnchor(zero=Decimal("6"), full=Decimal("2"), points=Decimal("3"))
    top_10_holders: ScoreAnchor = ScoreAnchor(zero=Decimal("0.30"), full=Decimal("0.15"), points=Decimal("6"))
    largest_related_cluster: ScoreAnchor = ScoreAnchor(zero=Decimal("0.15"), full=Decimal("0.05"), points=Decimal("6"))
    dev_cluster: ScoreAnchor = ScoreAnchor(zero=Decimal("0.05"), full=Decimal("0.02"), points=Decimal("4"))
    top_5_buyers_share: ScoreAnchor = ScoreAnchor(zero=Decimal("0.70"), full=Decimal("0.35"), points=Decimal("4"))
    round_trip_loss: ScoreAnchor = ScoreAnchor(zero=Decimal("0.08"), full=Decimal("0.03"), points=Decimal("8"))
    price_impact: ScoreAnchor = ScoreAnchor(zero=Decimal("0.035"), full=Decimal("0.01"), points=Decimal("6"))
    sell_route_points: Decimal = Decimal("6")
    quote_liquidity: ScoreAnchor = ScoreAnchor(zero=Decimal("40000"), full=Decimal("100000"), points=Decimal("5"))
    liquidity_stability: ScoreAnchor = ScoreAnchor(zero=Decimal("-0.03"), full=Decimal("0"), points=Decimal("6"))
    market_cap_to_liquidity: ScoreAnchor = ScoreAnchor(zero=Decimal("15"), full=Decimal("5"), points=Decimal("4"))
    pullback_min: Decimal = Decimal("0.10")
    pullback_max: Decimal = Decimal("0.25")
    pullback_points: Decimal = Decimal("4")
    vwap_reclaim_points: Decimal = Decimal("4")
    overextension_max_return: Decimal = Decimal("2.5")
    overextension_points: Decimal = Decimal("2")


class ProvidersConfig(BaseModel):
    """Provider endpoints and plan limits; operational, never a decision."""

    jupiter_base_url: str = "https://api.jup.ag/swap/v2"
    jupiter_requests_per_second: float = Field(default=3.0, gt=0)
    rpc_requests_per_second: float = Field(default=4.0, gt=0)


class StorageConfig(BaseModel):
    raw_events_enabled: bool = True
    raw_compression: str = "zstd"
    raw_retention_days: int = Field(90, ge=90)
    # Provider request/response audit rows; quotes behind fills are kept with
    # the fills themselves, so only recent calls are needed for debugging.
    api_call_retention_days: int = Field(default=3, ge=1)


class ReportingConfig(BaseModel):
    include_operational_costs: bool = True
    monthly_infrastructure_cost_usd: Decimal = Decimal("0")
    minimum_sample_warning_trades: int = 30


class LoggingConfig(BaseModel):
    level: str = "INFO"
    json_logs: bool = True


class CollectionConfig(BaseModel):
    """End of the frozen statistical collection window.

    The statistical gate needs every discovered pool to reach a terminal
    outcome and every position to close by ``ends_at``. From ``ends_at`` minus
    ``entry_cutoff_seconds`` new pools are rejected on sight, open candidates
    are rejected, and no entry is taken, while open positions keep being
    managed until they close. ``None`` means no window is frozen.
    """

    ends_at: datetime | None = None
    entry_cutoff_seconds: int = Field(default=1800, ge=0)

    @field_validator("ends_at")
    @classmethod
    def validate_ends_at(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError("collection.ends_at must include a UTC offset")
        return value.astimezone(timezone.utc)

    @property
    def closes_at(self) -> datetime | None:
        if self.ends_at is None:
            return None
        return self.ends_at - timedelta(seconds=self.entry_cutoff_seconds)


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
    providers: ProvidersConfig = Field(default_factory=ProvidersConfig)
    scoring: ScoringConfig = Field(default_factory=ScoringConfig)
    reporting: ReportingConfig = Field(default_factory=ReportingConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    collection: CollectionConfig = Field(default_factory=CollectionConfig)

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
        if self.telegram.enabled and not self.replay_mode:
            if self.time_zone != "Europe/Kyiv":
                raise ValueError(
                    "TIME_ZONE must be Europe/Kyiv when Telegram is enabled"
                )
            if self.telegram.daily_report_time != "00:00":
                raise ValueError(
                    "telegram.daily_report_time must be 00:00 when Telegram is enabled"
                )
        revision_file = Path(
            os.environ.get("APP_REVISION_FILE", "/app/REVISION")
        )
        if self.release_revision and revision_file.is_file():
            image_revision = (
                revision_file.read_text(encoding="ascii").strip().lower()
            )
            if image_revision != self.release_revision:
                raise ValueError(
                    "APP_REVISION does not match immutable image revision"
                )
        if (self.replay_mode or self.quote_journal_record) and not self.quote_journal_path:
            raise ValueError(
                "JUPITER_QUOTE_JOURNAL_PATH must be set when replay or record is enabled"
            )
        if self.candidate.min_pool_age_seconds < self.candidate.min_observation_seconds:
            raise ValueError("candidate min pool age cannot be shorter than observation window")
        if self.candidate.max_pool_age_seconds <= self.candidate.min_pool_age_seconds:
            raise ValueError("candidate max pool age must exceed min pool age")
        minimum_cutoff = (
            self.candidate.max_pool_age_seconds
            + self.exits.maximum_holding_seconds
            + self.paper.exit_retry_timeout_seconds
        )
        if (
            self.collection.ends_at is not None
            and self.collection.entry_cutoff_seconds < minimum_cutoff
        ):
            raise ValueError(
                "collection.entry_cutoff_seconds must leave room for the last "
                f"candidate and position to finish ({minimum_cutoff} s)"
            )
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
        chain = data.get("chain")
        if isinstance(chain, dict):
            # Operational recovery switch: toggling it during an incident must
            # not fork the strategy identity, report keys, or replay evidence.
            chain.pop("halt_on_unrecoverable_gap", None)
        storage = data.get("storage")
        if isinstance(storage, dict):
            # Housekeeping only: how long debug rows are kept decides nothing.
            storage.pop("api_call_retention_days", None)
        # Endpoints and plan limits move with the provider account, not the
        # strategy; a fake-provider rehearsal keeps the frozen identity.
        data.pop("providers", None)
        if self.collection.ends_at is None:
            # No frozen window changes no decision, so it keeps the identity
            # of configurations that predate the setting.
            data.pop("collection", None)
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

        # A blank env var means "not set". Nested models are decoded as JSON by
        # BaseSettings before any validator runs, so an empty value would abort
        # startup instead of falling back to YAML. Compose always defines the
        # optional overrides, so hide the blank ones while the model is built.
        blanked = {
            name: os.environ[name]
            for name in _optional_model_env_names(cls)
            if name in os.environ and not os.environ[name].strip()
        }
        for name in blanked:
            del os.environ[name]
        try:
            return cls(**yaml_values)
        finally:
            os.environ.update(blanked)


def _optional_model_env_names(cls: type["AppConfig"]) -> list[str]:
    names: list[str] = []
    for field_name, model_field in cls.model_fields.items():
        annotation = model_field.annotation
        if isinstance(annotation, type) and issubclass(annotation, BaseModel):
            names.append(field_name.upper())
    return names


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
