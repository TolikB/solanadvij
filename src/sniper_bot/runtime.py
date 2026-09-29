"""Runtime orchestrator."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import signal
import socket
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional, TypeVar
from uuid import uuid4

from .broker import FillReference, PaperBroker
from .candidates import Candidate, CandidateState
from .config import AppConfig, AppMode
from .database import ActiveRuntimeError, Database, _telegram_report_text
from .enrichment import DexscreenerClient
from .errors import EntrySlippageExceededError, ExecutionBlockedError
from .events import ChainEventType, EventEnvelope, EventSource, Protocol
from .exit_engine import ExitDecision, ExitPolicy, ExitReason, evaluate_exit
from .external_journal import ExternalJournal
from .features import FeatureSnapshot, HolderObservation
from .id_utils import DeterministicIdFactory
from .jupiter import JupiterQuoteProvider
from .ledger import PaperLedger
from .maintenance import RawRetentionManager
from .metrics import BotMetrics
from .models import PositionRecord, RoundTripQuote
from .outbox import TelegramOutboxWorker
from .pipeline import (
    SECURITY_CANDIDATE_STATES,
    TERMINAL_CANDIDATE_RETENTION,
    ConfirmationPipeline,
)
from .rate_limit import QuotePriority
from .registry import (
    USDC_MINT,
    WSOL_MINT,
    QuoteAssetPrice,
    pool_evidence,
    reserve_sell_value_usd,
)
from .reports import ReportBuilder
from .risk import RiskManager
from .scoring import ScoreBreakdown
from .security import (
    ExecutionChecks,
    HolderBalance,
    HolderMetrics,
    MintInfo,
    RejectReason,
    SecurityContext,
    aggregate_holders,
)
from .shadow import SHADOW_BLOCK_REASONS, ShadowBook, ShadowEntry
from .sizing import PositionSizingInput, SizingRejectReason, calculate_position_size
from .solana_rpc import SolanaRpcClient
from .stream import EntryGate, HeliusStreamGateway
from .telegram import NoopTelegramNotifier, TelegramNotifier
from .wallet_analysis import (
    CREATOR_MEMORY_RETENTION,
    RELATION_MEMORY_RETENTION,
    WalletAnalyzer,
)

logger = logging.getLogger(__name__)
_T = TypeVar("_T")


# The statistical gate needs a durable executable-equity path with bounded
# gaps even while no position is open; marks otherwise only follow fills and
# quoted open positions.
EQUITY_MARK_HEARTBEAT = timedelta(seconds=60)
# With positions open the durable path is marked every few seconds instead of
# every exit-loop pass: fills write their own exact marks, and a month of
# one-second marks would outgrow the bounded statistical evaluator.
EQUITY_MARK_OPEN_INTERVAL = timedelta(seconds=5)
MAX_ENRICHMENT_REMEMBERED = 10_000
# Reserve-based versus Jupiter marks of open positions, one JSON per line,
# summarised by scripts/mark_divergence.py for the pre-registered decision.
MARK_COMPARISON_LOG = "mark_comparisons.ndjson"
MARK_COMPARISON_LOG_BYTES = 50_000_000


class RecoveryStateError(RuntimeError):
    """Raised when persisted runtime state cannot be recovered safely."""


@dataclass(slots=True)
class _SecurityInputs:
    """Provider reads behind one candidate's security context."""

    mint_info: MintInfo
    holder_accounts: list[HolderBalance]
    holders_at: datetime
    round_trip: RoundTripQuote
    quote_at: datetime


class SniperRuntime:
    def __init__(self, config: AppConfig, *, data_dir: str | Path = "data") -> None:
        self.config = config
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.metrics = BotMetrics()
        self.entry_gate = EntryGate(self.metrics)
        self.database: Database | None = None if config.replay_mode else Database(config.postgres_dsn, metrics=self.metrics)
        self.database_available = config.replay_mode
        self._background_tasks: list[asyncio.Task[None]] = []
        self._persistence_tasks: set[asyncio.Task[None]] = set()
        self._started = False
        self._system_run_id: str | None = None
        self._external_journal = ExternalJournal(self.data_dir / "external_responses.ndjson")
        self._enrichment_queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1000)
        self._enrichment_seen: OrderedDict[str, None] = OrderedDict()
        self._momentum_windows: dict[str, int] = {}
        self._mark_quote_failures: dict[str, tuple[datetime, int]] = {}
        self._security_inputs: dict[str, _SecurityInputs] = {}
        self._last_equity_mark_at: datetime | None = None
        self.wallet_analyzer = WalletAnalyzer(
            reloadable_history=self.database is not None
        )
        id_factory = self._build_id_factory()
        self._report_runs_path = self.data_dir / "report_runs.json"
        self._report_runs = self._load_report_runs()

        self.ledger = PaperLedger(
            storage_path=self.data_dir / "paper_ledger.json",
            starting_equity_usd=config.starting_equity_usd,
            strategy_version=config.strategy_version,
            config_hash=config.config_hash,
            id_factory=id_factory,
            time_zone=config.time_zone,
        )
        self.risk_manager = RiskManager(config.risk, self.ledger)
        self.quote_provider = JupiterQuoteProvider(
            config.jupiter_api_key.get_secret_value(),
            base_url=config.providers.jupiter_base_url,
            rate_limit_per_second=config.providers.jupiter_requests_per_second,
            quote_mint=config.base_quote_mint,
            quote_mint_decimals=config.base_quote_decimals,
            timeout_seconds=config.execution.quote_timeout_ms / 1000,
            max_retries=config.execution.max_quote_retries,
            replay_mode=config.replay_mode,
            record_quotes=config.quote_journal_record,
            quote_journal_path=config.quote_journal_path,
            recorder=(self.database.record_external_api_call if self.database else None),
            metrics=self.metrics,
            minimum_network_fee_lamports=config.paper.min_network_fee_lamports,
        )

        self.broker: Optional[PaperBroker] = None
        if config.app_mode == AppMode.PAPER:
            self.broker = PaperBroker(
                quote_provider=self.quote_provider,
                ledger=self.ledger,
                risk_manager=self.risk_manager,
                base_quote_mint=config.base_quote_mint,
                id_factory=id_factory,
                execution_delay_ms=config.paper.execution_delay_ms,
                adverse_fill_bps=config.paper.adverse_fill_bps,
                strategy_version=config.strategy_version,
                config_hash=config.config_hash,
                exit_retry_interval_ms=config.paper.exit_retry_interval_ms,
                exit_retry_timeout_seconds=config.paper.exit_retry_timeout_seconds,
                max_entry_slippage_bps=config.paper.max_entry_slippage_bps,
                pool_evidence=self._pool_evidence,
                metrics=self.metrics,
            )
        self.notifier: NoopTelegramNotifier | TelegramNotifier
        if config.replay_mode or not config.telegram.enabled:
            self.notifier = NoopTelegramNotifier()
        else:
            self.notifier = TelegramNotifier(
                config.telegram_bot_token.get_secret_value(),
                config.telegram_admin_chat_id,
                allowlist=config.telegram_allowlist_chat_ids,
                user_allowlist=config.telegram_allowlist_user_ids,
            )
        self.outbox_worker: TelegramOutboxWorker | None = None
        if (
            self.database is not None
            and not config.replay_mode
            and config.telegram.enabled
        ):
            self.outbox_worker = TelegramOutboxWorker(
                self.database,
                self.notifier,
                metrics=self.metrics,
            )
        self.rpc = SolanaRpcClient(
            config.resolved_helius_rpc_url(),
            rpc_requests_per_second=config.providers.rpc_requests_per_second,
            replay_mode=config.replay_mode,
            journal=self._external_journal,
            record_responses=config.quote_journal_record,
            recorder=(self.database.record_external_api_call if self.database else None),
        )
        self.enrichment = DexscreenerClient(
            timeout_seconds=config.enrichment.timeout_ms / 1000,
            minimum_interval_seconds=config.enrichment.minimum_interval_ms / 1000,
            cache_seconds=config.enrichment.cache_seconds,
            replay_mode=config.replay_mode,
            journal=self._external_journal,
            recorder=(self.database.record_external_api_call if self.database else None),
        )
        self.pipeline = ConfirmationPipeline(
            data_dir=str(self.data_dir),
            strategy_version=config.strategy_version,
            config_hash=config.config_hash,
            entry_gate=self.entry_gate,
            metrics=self.metrics,
            database=self.database,
            security_provider=self._build_security_context,
            entry_handler=self._open_candidate,
            event_observer=self._observe_event,
            fatal_handler=self._request_fatal_restart,
            record_raw=not config.replay_mode,
            config=config,
        )
        self.shadow: ShadowBook | None = None
        if config.app_mode == AppMode.PAPER:
            self.shadow = ShadowBook(
                config=config,
                quote_provider=self.quote_provider,
                database=lambda: self.database,
                id_factory=id_factory or (lambda: str(uuid4())),
                pool_evidence=self._pool_evidence,
                reserve_mark=self._reserve_mark_usd,
                on_hold=self.pipeline.hold_pool,
                on_release=self.pipeline.release_pool,
                metrics=self.metrics,
            )
        now = datetime.now(tz=timezone.utc)
        self.pipeline.pools.set_quote_price(
            QuoteAssetPrice(mint=USDC_MINT, price_usd=Decimal("1"), observed_at=now)
        )
        self.stream_gateway = HeliusStreamGateway(
            websocket_url=config.resolved_helius_wss_url(),
            rpc=self.rpc,
            handler=self.pipeline.process_transaction,
            batch_handler=self.pipeline.process_transactions,
            fatal_handler=self._request_fatal_restart,
            gap_handler=(
                self.database.record_stream_recovery_gap
                if self.database is not None
                else None
            ),
            gap_resolved_handler=(
                self.database.resolve_stream_recovery_gaps
                if self.database is not None
                else None
            ),
            entry_gate=self.entry_gate,
            metrics=self.metrics,
            max_processing_lag_seconds=(
                float(config.chain.max_stream_lag_ms) / 1000.0
            ),
            halt_on_unrecoverable_gap=(
                config.chain.halt_on_unrecoverable_gap
            ),
        )
        self.report_builder = ReportBuilder(self)
        self.manages_lifecycle_notifications = True
        self._lifecycle_start_sent = False
        self.retention = RawRetentionManager(
            self.data_dir / "raw", config.storage.raw_retention_days
        )

    async def start(self) -> None:
        if self._started or self.config.replay_mode:
            self._started = True
            return
        if self.database is not None:
            try:
                self.database_available = await self.database.ping()
                await self.database.acquire_runtime_lease()
                await self.database.register_strategy(
                    strategy_id=self.config.strategy_version,
                    version=self.config.strategy_version,
                    config_hash=self.config.config_hash,
                    config_json=self.config.masked_view(),
                    git_commit=self.config.release_revision or None,
                    now=datetime.now(tz=timezone.utc),
                )
                await self.database.initialize_paper_account(
                    account_id="paper-main",
                    starting_equity=self.config.starting_equity_usd,
                    now=datetime.now(tz=timezone.utc),
                )
                self._system_run_id = await self.database.start_system_run(
                    mode=self.config.app_mode.value,
                    strategy_version_id=self.config.strategy_version,
                    hostname=socket.gethostname(),
                    app_version="0.1.0",
                    now=datetime.now(tz=timezone.utc),
                    account_id="paper-main",
                )
                restored = await self.database.load_paper_ledger(account_id="paper-main")
                if restored is not None:
                    self.ledger.restore_from_database(restored)
                today = self._today_key()
                daily_bounds = await self.database.load_daily_equity_bounds(
                    account_id="paper-main",
                    report_date=today,
                    timezone_name=self.config.time_zone,
                )
                if daily_bounds is not None:
                    self.ledger.set_daily_equity_baseline(
                        today,
                        Decimal(str(daily_bounds["starting_equity_usd"])),
                    )
                restored_at = datetime.now(tz=timezone.utc)
                profiles, relations = await self.database.load_wallet_analysis(
                    profiles_since=restored_at - CREATOR_MEMORY_RETENTION,
                    relations_since=restored_at - RELATION_MEMORY_RETENTION,
                )
                self.wallet_analyzer.restore(
                    profiles, relations, restored_at=restored_at
                )
                restored_candidates = await self.database.load_active_candidates(
                    self.config.strategy_version,
                    terminal_since=(
                        datetime.now(tz=timezone.utc)
                        - TERMINAL_CANDIDATE_RETENTION
                        - self._rehydration_window()
                    ),
                )
                self.pipeline.restore_candidates(restored_candidates)
                self.pipeline.restore_score_totals(
                    await self.database.load_candidate_score_totals(
                        self.config.strategy_version,
                        candidate_ids=[
                            candidate.candidate_id for candidate in restored_candidates
                        ],
                    )
                )
                shadow_pools = (
                    set(await self.shadow.restore()) if self.shadow is not None else set()
                )
                tracked_pool_addresses = {
                    candidate.pool_address
                    for candidate in restored_candidates
                    if candidate.state
                    not in {
                        CandidateState.CLOSED,
                        CandidateState.REJECTED,
                    }
                } | shadow_pools
                pool_bootstrap_events = (
                    await self.database.load_processed_pool_creation_events(
                        tracked_pool_addresses
                    )
                )
                bootstrapped_pool_addresses = {
                    event.pool_address
                    for event in pool_bootstrap_events
                    if event.pool_address is not None
                }
                missing_pool_addresses = (
                    tracked_pool_addresses - bootstrapped_pool_addresses
                )
                if missing_pool_addresses:
                    raise RecoveryStateError(
                        "active candidate pool state is unavailable for recovery"
                    )
                bootstrap_event_ids = {
                    event.event_id for event in pool_bootstrap_events
                }
                for event in pool_bootstrap_events:
                    if await self.pipeline.rehydrate_event(event):
                        await self._ensure_wallet_history_for(event)
                        self.wallet_analyzer.observe(event)
                quarantined_protocols = (
                    await self.database.load_quarantined_event_protocols()
                )
                recovery_since = (
                    datetime.now(tz=timezone.utc) - self._rehydration_window()
                )
                for event in await self.database.load_processed_events_since(
                    recovery_since
                ):
                    if event.source != EventSource.REPLAY:
                        self.stream_gateway.restore_protocol_checkpoint(
                            event.protocol,
                            event.signature,
                        )
                    if event.event_id in bootstrap_event_ids:
                        continue
                    if await self.pipeline.rehydrate_event(event):
                        await self._ensure_wallet_history_for(event)
                        self.wallet_analyzer.observe(event)
                runtime_checkpoint = await self.database.load_runtime_checkpoint(
                    "paper-main:exit-monitor"
                )
                if runtime_checkpoint is not None:
                    self._momentum_windows = {
                        str(key): int(value)
                        for key, value in (
                            runtime_checkpoint.get("momentum_windows") or {}
                        ).items()
                    }
                for protocol in quarantined_protocols:
                    self.entry_gate.block_protocol(Protocol(protocol))
                if quarantined_protocols:
                    self.entry_gate.block("event_quarantine")
                else:
                    for event in await self.database.load_unprocessed_events(
                        include_owned_processing=True
                    ):
                        await self.pipeline.process_event(event, recovering=True)
                for protocol, checkpoint in (
                    await self.database.load_protocol_checkpoints()
                ).items():
                    self.stream_gateway.restore_protocol_checkpoint(
                        Protocol(protocol), checkpoint
                    )
                slot, signature, observed_at = await self.database.load_stream_checkpoint()
                self.stream_gateway.restore_checkpoint(slot, signature, observed_at)
                if self.broker is not None:
                    self.broker._database = self.database
                self.entry_gate.unblock("database_unavailable")
            except Exception:
                run_id = self._system_run_id
                self._system_run_id = None
                if run_id is not None:
                    try:
                        await self.database.stop_system_run(
                            run_id,
                            reason="startup_failed",
                            now=datetime.now(tz=timezone.utc),
                        )
                    except Exception:
                        logger.exception("failed to close system run after startup failure")
                self.database_available = False
                self.entry_gate.block("database_unavailable")
                try:
                    await self.database.release_runtime_lease()
                except Exception:
                    logger.exception("failed to release runtime lease after startup failure")
                raise
        await self.pipeline.start_background_workers()
        if self.outbox_worker is not None and self.database_available:
            await self.outbox_worker.start()
        if "event_quarantine" not in self.entry_gate.reasons:
            await self.stream_gateway.start()
        else:
            logger.critical("chain stream disabled because terminal events require review")
        self.entry_gate.block("warmup")
        self._background_tasks = [
            asyncio.create_task(self._candidate_loop(), name="candidate-loop"),
            asyncio.create_task(self._exit_loop(), name="exit-loop"),
            asyncio.create_task(self._shadow_exit_loop(), name="shadow-exit-loop"),
            asyncio.create_task(self._freshness_loop(), name="freshness-loop"),
            asyncio.create_task(self._quote_asset_loop(), name="quote-asset-loop"),
            asyncio.create_task(self._enrichment_loop(), name="enrichment-loop"),
            asyncio.create_task(self._maintenance_loop(), name="maintenance-loop"),
            asyncio.create_task(self._heartbeat_loop(), name="heartbeat-loop"),
            asyncio.create_task(self._warmup(), name="warmup"),
        ]
        if self.config.telegram.enabled:
            self._background_tasks.append(
                asyncio.create_task(
                    self._daily_report_loop(),
                    name="daily-report-loop",
                )
            )
        self._install_signal_controls()
        from sniper_bot.process_identity import write_process_identity_file

        try:
            write_process_identity_file(self.data_dir / "sniper.pid")
        except BaseException:
            try:
                await self.shutdown()
            except BaseException:
                logger.exception("failed to clean up after PID identity publication failure")
            raise
        self._started = True

    def report(self) -> dict[str, object]:
        return {
            "today": self.build_daily_report(),
            "all_time": self.build_all_time_report(),
            "system": self.system_status(),
        }

    def system_status(self) -> dict[str, object]:
        self._sync_paper_metrics()
        return {
            "health": self.health_status(),
            "entry_enabled": self.entry_gate.enabled,
            "entry_block_reasons": self.entry_gate.reasons,
            "database_available": self.database_available,
            "last_stream_slot": self.stream_gateway.last_slot,
            "last_stream_observed_at": (
                self.stream_gateway.last_observed_at.isoformat()
                if self.stream_gateway.last_observed_at
                else None
            ),
            "last_stream_chain_block_time": (
                self.stream_gateway.last_chain_block_time.isoformat()
                if self.stream_gateway.last_chain_block_time
                else None
            ),
            "last_stream_processed_block_time": (
                self.stream_gateway.last_processed_block_time.isoformat()
                if self.stream_gateway.last_processed_block_time
                else None
            ),
            "stream_processing_lag_seconds": (
                self.stream_gateway.last_processing_lag_seconds
            ),
            "candidate_count": len(self.pipeline.candidates),
        }

    def health_status(self) -> str:
        if self.ledger.state.is_halted:
            return "HALTED"
        reasons = set(self.entry_gate.reasons)
        if (
            not self.database_available
            or "event_processing_error" in reasons
            or any(reason.startswith("protocol:") for reason in reasons)
        ):
            return "HALTED"
        if not self.entry_gate.enabled:
            return "DEGRADED"
        return "HEALTHY"

    async def wait_until_ready(
        self,
        *,
        timeout_seconds: float = 120.0,
    ) -> bool:
        deadline = (
            asyncio.get_running_loop().time() + timeout_seconds
        )
        while asyncio.get_running_loop().time() < deadline:
            if (
                self.health_status() == "HEALTHY"
                and self.config.app_mode != AppMode.LIVE
            ):
                return True
            await asyncio.sleep(0.25)
        return False

    def _sync_paper_metrics(self) -> None:
        snapshot = self.ledger.snapshot()
        equity = Decimal(str(snapshot["equity_usd"]))
        peak = Decimal(str(snapshot["peak_equity_usd"]))
        drawdown = (peak - equity) / peak * Decimal("100") if peak > 0 else Decimal("0")
        self.metrics.paper_positions_open.set(len(self.ledger.open_positions))
        self.metrics.paper_pnl_usd.set(float(snapshot["realized_pnl_usd"]))
        self.metrics.paper_equity_usd.set(float(equity))
        self.metrics.paper_drawdown_pct.set(float(max(Decimal("0"), drawdown)))

    def build_daily_report(self, date: str | None = None) -> dict[str, object]:
        return self.report_builder.daily(date)

    def build_all_time_report(self) -> dict[str, object]:
        return self.report_builder.all_time()

    async def build_all_time_report_with_history(self) -> dict[str, object]:
        maximum = None
        if self.database is not None and self.database_available:
            maximum = await self.database.load_all_time_max_drawdown_pct(
                account_id="paper-main"
            )
        return self.report_builder.all_time(max_drawdown_pct=maximum)

    def _open_positions_snapshot(self) -> list[dict[str, str]]:
        positions = self.ledger.open_positions
        return [
            {
                "position_id": position.position_id,
                "token_mint": position.token_mint,
                "remaining_token_amount": str(position.remaining_token_amount),
                "remaining_cost_usd": str(position.remaining_cost_usd),
                "realized_pnl_usd": str(position.realized_pnl_usd),
                "status": position.status.value,
                "opened_at": position.opened_at.isoformat(),
            }
            for position in positions
        ]

    async def daily_report_if_not_sent(self, date: str | None = None, *, send: bool = False) -> dict[str, object] | None:
        target = date or self._today_key()
        if self._daily_report_already_sent(target):
            return None
        capital_bounds = None
        historical_target = target < self._today_key()
        snapshot_lookup_failed = False
        if self.database is not None and self.database_available:
            try:
                capital_bounds = await self.database.load_daily_equity_bounds(
                    account_id="paper-main",
                    report_date=target,
                    timezone_name=self.config.time_zone,
                )
            except Exception:
                if not historical_target:
                    raise
                snapshot_lookup_failed = True
                logger.exception("historical daily equity snapshot lookup failed")
        if historical_target and capital_bounds is None:
            unavailable_reason = "historical_equity_snapshot_unavailable"
            report: dict[str, object] = {
                "period": "daily",
                "date": target,
                "timezone": self.config.time_zone,
                "strategy_version": self.config.strategy_version,
                "data_status": "unavailable",
                "data_status_reason": unavailable_reason,
                "candidate_count": None,
                "closed_trade_count": None,
                "equity_usd": None,
                "realized_pnl_usd": None,
                "unrealized_pnl_usd": None,
                "net_pnl_usd": None,
                "pnl": None,
                "sample_size_warning": True,
                "reconcile": {
                    "is_reconciled": False,
                    "reason": unavailable_reason,
                },
                "open_positions": None,
            }
            report["report_id"] = self._report_id(report)
            if not send:
                return report
            if self.database is None or not self.database_available or snapshot_lookup_failed:
                return None
            inserted = await self.database.enqueue_outbox(
                idempotency_key=(
                    f"telegram:daily-report-unavailable:{target}:"
                    f"{self.config.strategy_version}"
                ),
                event_type="daily_report",
                payload={"text": _telegram_report_text(report)},
            )
            return report if inserted else None
        report = self.report_builder.daily(
            target,
            capital_bounds=capital_bounds,
        )
        if (
            self.database is not None
            and self.database_available
            and hasattr(
                self.database,
                "load_daily_trading_summary",
            )
        ):
            start_utc = datetime.fromisoformat(
                str(report["period_start_utc"])
            )
            end_utc = datetime.fromisoformat(
                str(report["period_end_utc"])
            )
            summary = (
                await self.database.load_daily_trading_summary(
                    start_utc=start_utc,
                    end_utc=end_utc,
                )
            )
            report["signals"] = summary["signals"]
            report_trades_value = report.get("trades")
            report_trades: dict[str, Any] = (
                dict(report_trades_value)
                if isinstance(report_trades_value, dict)
                else {}
            )
            report_trades.update(summary["trades"])
            report["trades"] = report_trades
            execution_value = report.get(
                "execution_quality"
            )
            execution: dict[str, Any] = (
                dict(execution_value)
                if isinstance(execution_value, dict)
                else {}
            )
            execution.update(summary["execution_quality"])
            report["execution_quality"] = execution
            report["exit_reasons"] = summary["exit_reasons"]
            report["shadow"] = summary.get("shadow")
            report["failed_entries"] = summary.get("failed_entries", 0)
            report["rejections"] = summary["rejections"]
            report["open_positions"] = summary["open_positions"]
            capital_value = report.get("capital")
            capital: dict[str, Any] = (
                dict(capital_value)
                if isinstance(capital_value, dict)
                else {}
            )
            capital["realized_pnl_usd"] = summary[
                "realized_pnl_usd"
            ]
            capital["simulated_costs_usd"] = summary[
                "simulated_costs_usd"
            ]
            report["capital"] = capital
            report["realized_pnl_usd"] = summary[
                "realized_pnl_usd"
            ]
            report["pnl"] = summary["realized_pnl_usd"]
            report["report_id"] = self._report_id(report)
        all_time_report = None
        if send and self.database is not None and self.database_available:
            inserted = await self.database.store_daily_report(
                report=report,
                include_all_time=all_time_report,
            )
            if not inserted:
                return None
        elif send:
            if not await self._notify_daily_report(report):
                return None
        self._record_daily_report_sent(target, str(report["report_id"]))
        return report

    def daily_loss_exceeded(self, limit: float | int | None = None) -> bool:
        limit_value = self.config.risk.daily_loss_limit_usdc if limit is None else limit
        return self.ledger.daily_loss_exceeded(Decimal(str(limit_value)))

    def _build_id_factory(self) -> Callable[[], str] | None:
        if self.config.replay_mode and self.config.replay_seed is not None:
            return DeterministicIdFactory(self.config.replay_seed)
        return None

    def drawdown_exceeded(self, limit_pct: float | int | None = None) -> bool:
        limit = self.config.risk.all_time_drawdown_limit_pct if limit_pct is None else limit_pct
        snapshot = self.ledger.snapshot()
        peak = Decimal(str(snapshot["peak_equity_usd"]))
        equity = Decimal(str(snapshot["equity_usd"]))
        if peak <= 0:
            return False
        drawdown = (peak - equity) / peak * Decimal("100")
        return drawdown >= Decimal(str(limit))

    def _rehydration_window(self) -> timedelta:
        return timedelta(
            seconds=max(
                300,
                self.config.candidate.max_pool_age_seconds
                + self.config.exits.maximum_holding_seconds
                + 60,
            )
        )

    def is_paper(self) -> bool:
        return self.config.app_mode == AppMode.PAPER

    def is_record(self) -> bool:
        return self.config.app_mode == AppMode.RECORD

    def _request_fatal_restart(self, error: BaseException) -> None:
        self.entry_gate.block("database_unavailable")
        logger.critical(
            "event state became ambiguous; requesting supervised process restart",
            exc_info=(type(error), error, error.__traceback__),
        )
        os.kill(os.getpid(), signal.SIGTERM)

    def halt(self, reason: str) -> None:
        self.risk_manager.set_halt(reason)
        self._schedule_risk_state_persist()

    def resume(self) -> bool:
        resumed = self.risk_manager.clear_halt()
        if resumed:
            self._schedule_risk_state_persist()
        return resumed

    def _schedule_risk_state_persist(self) -> None:
        if self.database is None or not self.database_available:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        task = loop.create_task(
            self.database.persist_risk_state(
                account_id="paper-main",
                halt_reason=self.ledger.state.halt_reason,
                pause_until=self.ledger.state.pause_until,
                daily_halt_date=self.ledger.state.daily_halt_date,
                updated_at=datetime.now(tz=timezone.utc),
            ),
            name="risk-state-persist",
        )
        task.add_done_callback(self._log_background_task_failure)
        self._persistence_tasks.add(task)
        task.add_done_callback(self._persistence_tasks.discard)

    @staticmethod
    def _log_background_task_failure(task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.error(
                "background state persistence failed",
                exc_info=(type(error), error, error.__traceback__),
            )

    async def shutdown(self) -> None:
        from sniper_bot.process_identity import remove_own_process_identity_file

        pid_path = self.data_dir / "sniper.pid"
        try:
            remove_own_process_identity_file(pid_path)
        except (OSError, ValueError):
            logger.exception("failed to remove owned PID identity during shutdown")
        for task in self._background_tasks:
            task.cancel()
        for task in self._background_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._background_tasks = []
        errors: list[BaseException] = []
        try:
            if not self.config.replay_mode:
                await self.stream_gateway.stop()
        except BaseException as exc:
            errors.append(exc)
            logger.exception("stream gateway stop failed")

        try:
            await self.pipeline.stop_background_workers(
                timeout_seconds=120.0
            )
        except BaseException as exc:
            errors.append(exc)
            logger.exception("pipeline stop_background_workers failed")

        if self.config.telegram.enabled:
            try:
                await self._notify_lifecycle_alert(
                    "system_stop",
                    "Бот зупинено.",
                )
            except BaseException as exc:
                errors.append(exc)
                logger.exception("lifecycle stop notification failed")

        if self.outbox_worker is not None:
            try:
                drained = await self.outbox_worker.drain(timeout_seconds=5.0)
                if not drained:
                    logger.warning(
                        "telegram outbox drain did not complete; undelivered events remain durable"
                    )
                await self.outbox_worker.stop()
            except BaseException as exc:
                errors.append(exc)
                logger.exception("outbox worker stop failed")

        if self._persistence_tasks:
            try:
                await asyncio.gather(*self._persistence_tasks, return_exceptions=True)
            finally:
                self._persistence_tasks.clear()

        if self.database is not None:
            try:
                if self._system_run_id is not None:
                    await self.database.stop_system_run(
                        self._system_run_id,
                        reason="graceful_shutdown",
                        now=datetime.now(tz=timezone.utc),
                    )
            except BaseException as exc:
                errors.append(exc)
                logger.exception("database stop_system_run failed")
            finally:
                try:
                    await self.database.close()
                except BaseException as exc:
                    errors.append(exc)
                    logger.exception("database close failed")

        try:
            await self.notifier.stop()
        except BaseException as exc:
            errors.append(exc)
            logger.exception("notifier stop failed")

        self._started = False
        if errors:
            if len(errors) == 1:
                raise errors[0]
            raise BaseExceptionGroup("multiple errors during runtime shutdown", errors)

    def _install_signal_controls(self) -> None:
        if os.name == "nt":
            return
        loop = asyncio.get_running_loop()
        if hasattr(signal, "SIGUSR1"):
            loop.add_signal_handler(signal.SIGUSR1, self.halt, "unix signal pause")
        if hasattr(signal, "SIGUSR2"):
            loop.add_signal_handler(signal.SIGUSR2, self.resume)

    async def _warmup(self) -> None:
        await asyncio.sleep(self.config.chain.warmup_seconds)
        self.entry_gate.unblock("warmup")

    async def _counted(self, source: str, read: Awaitable[_T]) -> _T:
        """Await one security read and count it by source and outcome."""
        try:
            result = await read
        except asyncio.CancelledError:
            raise
        except Exception:
            self.metrics.security_input_requests.labels(
                source=source, outcome="failed"
            ).inc()
            raise
        self.metrics.security_input_requests.labels(source=source, outcome="ok").inc()
        return result

    def _reserve_mark_usd(self, position: PositionRecord, now: datetime) -> Decimal | None:
        """Executable value of a position from the tracked pool reserves."""
        if not position.pool_address or position.remaining_token_amount <= 0:
            return None
        pool = self.pipeline.pools.pool(position.pool_address)
        state = self.pipeline.pools.state(position.pool_address)
        if pool is None or state is None:
            return None
        quote_price = self.pipeline.pools.quote_price(pool.quote_mint)
        if quote_price is None or (
            pool.quote_mint != USDC_MINT and quote_price.is_stale(now)
        ):
            return None
        return reserve_sell_value_usd(
            state,
            pool,
            position.remaining_token_amount,
            quote_price_usd=quote_price.price_usd,
        )

    def _record_mark_comparison(
        self,
        position: PositionRecord,
        reserve_usd: Decimal,
        jupiter_usd: Decimal,
        now: datetime,
        *,
        source: str = "position",
    ) -> None:
        """Log both marks so the soak can decide on reserve-based marking."""
        if jupiter_usd <= 0:
            return
        divergence_bps = (reserve_usd / jupiter_usd - Decimal("1")) * Decimal("10000")
        self.metrics.mark_divergence_bps.observe(float(abs(divergence_bps)))
        record = {
            "at": now.isoformat(),
            "source": source,
            "position_id": position.position_id,
            "mint": position.token_mint,
            "pool_address": position.pool_address,
            "jupiter_usd": str(jupiter_usd),
            "reserve_usd": str(reserve_usd),
            "divergence_bps": str(divergence_bps.quantize(Decimal("0.01"))),
        }
        path = self.data_dir / MARK_COMPARISON_LOG
        try:
            if path.exists() and path.stat().st_size > MARK_COMPARISON_LOG_BYTES:
                path.replace(path.with_suffix(path.suffix + ".1"))
            with path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(record, sort_keys=True) + "\n")
        except OSError:
            logger.warning("mark comparison log unavailable", exc_info=True)

    def _size_entry(
        self, security: SecurityContext, score: ScoreBreakdown, *, fresh_day: bool
    ) -> Any:
        account = self.ledger.snapshot()
        return calculate_position_size(
            PositionSizingInput(
                current_equity_usd=Decimal(str(account["equity_usd"])),
                # A shadow trade sizes as on a fresh day: the signal is what it
                # measures, not what the account has left to risk today.
                daily_pnl_usd=(
                    Decimal("0")
                    if fresh_day
                    else self.ledger.daily_pnl(self.ledger.current_date_key())
                ),
                quote_liquidity_usd=security.quote_liquidity_usd,
                estimated_round_trip_cost_pct=security.execution.round_trip_loss_pct,
                score=score.total_score,
                hard_stop_pct=self.config.risk.hard_stop_pct,
                adverse_execution_buffer_pct=self.config.risk.adverse_execution_buffer_pct,
                daily_loss_limit_usd=self.config.risk.daily_loss_limit_usdc,
                maximum_position_usd=self.config.risk.max_position_usdc,
                minimum_position_usd=self.config.risk.min_position_usdc,
                risk_per_trade_pct=self.config.risk.risk_per_trade_pct,
                maximum_position_equity_pct=self.config.risk.max_position_equity_pct,
                maximum_position_to_liquidity_pct=(
                    self.config.liquidity.max_position_to_quote_liquidity_pct
                ),
            )
        )

    async def _open_shadow(
        self,
        candidate: Candidate,
        security: SecurityContext,
        score: ScoreBreakdown,
        reference: FillReference | None,
        *,
        block_reason: str,
    ) -> None:
        """Take a risk-blocked entry in the shadow book (signal sample)."""
        if self.shadow is None:
            return
        sizing = self._size_entry(security, score, fresh_day=True)
        if not sizing.allowed:
            return
        try:
            await self.shadow.open(
                ShadowEntry(
                    candidate_id=candidate.candidate_id,
                    mint=candidate.mint,
                    pool_address=candidate.pool_address,
                    size_usd=sizing.position_size_usd,
                    block_reason=block_reason,
                    tokens_per_usd=reference.tokens_per_usd if reference else None,
                )
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "shadow entry failed", extra={"candidate_id": candidate.candidate_id}
            )

    async def evaluate_shadow_exits(self, *, now: datetime | None = None) -> list[ExitDecision]:
        if self.shadow is None:
            return []
        now = now or datetime.now(tz=timezone.utc)

        def features(pool_address: str, at: datetime) -> FeatureSnapshot | None:
            if not pool_address or self.pipeline.pools.pool(pool_address) is None:
                return None
            return self.pipeline.features.snapshot(pool_address, at)

        def dev_sold(position: PositionRecord, at: datetime) -> bool:
            token = self.pipeline.tokens.get(position.token_mint)
            dev_wallet = token.creator_address if token else None
            return bool(
                dev_wallet
                and position.pool_address
                and any(
                    trade.wallet == dev_wallet
                    and trade.side.value == "sell"
                    and trade.event_time >= position.opened_at
                    for trade in self.pipeline.features.trades(position.pool_address, at=at)
                )
            )

        return await self.shadow.evaluate_exits(
            now, policy=self._exit_policy(), features=features, dev_sold=dev_sold
        )

    def _exit_policy(self) -> ExitPolicy:
        return ExitPolicy(
            tp1_return=self.config.exits.take_profit_1_pct,
            tp1_size=self.config.exits.take_profit_1_size_pct,
            tp2_return=self.config.exits.take_profit_2_pct,
            tp2_size_of_initial=self.config.exits.take_profit_2_size_pct,
            stop_loss_return=-abs(self.config.risk.hard_stop_pct),
            trailing_stop_pct=self.config.exits.trailing_stop_pct,
            max_hold_seconds=self.config.exits.maximum_holding_seconds,
            no_new_high_seconds=self.config.exits.no_new_high_timeout_seconds,
        )

    def _compare_round_trip_to_reserves(
        self, candidate: Candidate, round_trip: RoundTripQuote, at: datetime
    ) -> None:
        """Every security round trip is also a reserve-versus-Jupiter sample.

        Record-mode soaks hold no positions, so this is where the evidence
        for the mark-source decision comes from before paper trading.
        """
        sell_usd = (
            round_trip.sell.out_amount_usd
            if round_trip.sell.out_amount_usd
            else round_trip.sell.out_amount
        )
        probe = PositionRecord(
            position_id=f"round-trip:{candidate.candidate_id}",
            token_mint=candidate.mint,
            open_fill_id="",
            entry_token_amount=round_trip.buy.out_amount,
            entry_cost_usd=round_trip.starting_usd,
            open_ratio=Decimal("1"),
            opened_at=at,
            locked_usd=round_trip.starting_usd,
            remaining_token_amount=round_trip.buy.out_amount,
            remaining_cost_usd=round_trip.starting_usd,
            pool_address=candidate.pool_address,
        )
        reserve_usd = self._reserve_mark_usd(probe, at)
        if reserve_usd is not None and sell_usd:
            self._record_mark_comparison(
                probe, reserve_usd, sell_usd, at, source="security_round_trip"
            )

    def _pool_evidence(self, pool_address: str) -> dict[str, str] | None:
        return pool_evidence(
            self.pipeline.pools.state(pool_address),
            self.pipeline.pools.pool(pool_address),
        )

    def _prune_security_inputs(self) -> None:
        for candidate_id in list(self._security_inputs):
            candidate = self.pipeline.candidates.get(candidate_id)
            if candidate is None or candidate.state not in SECURITY_CANDIDATE_STATES:
                del self._security_inputs[candidate_id]

    def _ingest_holder_observation(
        self,
        candidate: Candidate,
        snapshot: FeatureSnapshot,
        holders: HolderMetrics,
    ) -> None:
        self.pipeline.features.ingest_holders(
            HolderObservation(
                event_id=(
                    f"holders:{candidate.candidate_id}:"
                    f"{snapshot.snapshot_time.isoformat()}"
                ),
                pool_address=candidate.pool_address,
                event_time=snapshot.snapshot_time,
                holder_count=holders.holder_count,
                top_10_holders_pct=holders.top_10_holders_pct,
                dev_cluster_holding_pct=holders.dev_cluster_holding_pct,
                largest_related_cluster_pct=holders.related_cluster_holding_pct,
            )
        )

    async def _candidate_loop(self) -> None:
        while True:
            try:
                await self.pipeline.evaluate_candidates()
                self._prune_security_inputs()
                self.entry_gate.unblock("security_data_unavailable")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("candidate evaluation loop failed")
                self.entry_gate.block("security_data_unavailable")
            await asyncio.sleep(1)

    async def _shadow_exit_loop(self) -> None:
        # Separate from the account's exit loop: a shadow sell's execution
        # delay must never hold up a real position's stop.
        while True:
            try:
                await self.evaluate_shadow_exits()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("shadow exit loop failed")
            await asyncio.sleep(1)

    async def _exit_loop(self) -> None:
        while True:
            try:
                await self.evaluate_and_close_exits()
                self.entry_gate.unblock("exit_monitor_error")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("exit monitoring loop failed")
                self.entry_gate.block("exit_monitor_error")
            await asyncio.sleep(1)

    async def _freshness_loop(self) -> None:
        while True:
            try:
                self.stream_gateway.refresh_freshness()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("stream freshness loop failed")
                self.entry_gate.block("stream_stale")
            await asyncio.sleep(1)

    async def _quote_asset_loop(self) -> None:
        while True:
            try:
                price = await self.quote_provider.get_sol_usd_price(WSOL_MINT, USDC_MINT)
                self.pipeline.pools.set_quote_price(
                    QuoteAssetPrice(
                        mint=WSOL_MINT,
                        price_usd=price,
                        observed_at=datetime.now(tz=timezone.utc),
                    )
                )
                self.entry_gate.unblock("sol_price_unavailable")
            except Exception:
                logger.exception("SOL/USD quote refresh failed")
                self.entry_gate.block("sol_price_unavailable")
            await asyncio.sleep(5)

    async def _ensure_wallet_history(
        self, wallet: str | None, *, at: datetime | None = None
    ) -> None:
        """Load a wallet's persisted history before memory writes over it."""
        if (
            wallet is None
            or self.database is None
            or not self.wallet_analyzer.needs_history(wallet)
        ):
            return
        profile = await self.database.load_wallet_profile(wallet)
        self.wallet_analyzer.restore_history(
            wallet, profile, at=at or datetime.now(tz=timezone.utc)
        )

    async def _ensure_wallet_history_for(self, event: EventEnvelope) -> None:
        # Launches rebuild the creator's history; every other touch of a wallet
        # only merges identity fields and never needs the persisted history.
        if event.event_type != ChainEventType.TOKEN_CREATED:
            return
        for key in ("creator", "user"):
            value = event.payload.get(key)
            if value:
                await self._ensure_wallet_history(str(value), at=event.observed_at)

    async def _observe_event(self, event: EventEnvelope) -> None:
        await self._ensure_wallet_history_for(event)
        profiles, relations = self.wallet_analyzer.observe(event)
        wallet = str(event.payload.get("user") or "")
        if event.mint and wallet:
            event.payload["same_funder_cluster"] = self.wallet_analyzer.is_same_funder_cluster(
                event.mint, wallet
            )
        if self.database is not None:
            for profile in profiles:
                await self.database.upsert_wallet_profile(profile)
            for relation in relations:
                await self.database.upsert_wallet_relation(relation, event.observed_at)

    def _request_enrichment(self, mint: str) -> None:
        """Enrich only tokens that reached a candidate decision.

        Enrichment feeds reports, never a decision, so the rare candidate
        that passes the market screen is worth a Dexscreener call and the
        tens of thousands of daily launches are not.
        """
        if (
            not self.config.enrichment.enabled
            or self.config.replay_mode
            or mint in self._enrichment_seen
        ):
            return
        self._enrichment_seen[mint] = None
        if len(self._enrichment_seen) > MAX_ENRICHMENT_REMEMBERED:
            self._enrichment_seen.popitem(last=False)
        try:
            self._enrichment_queue.put_nowait(mint)
        except asyncio.QueueFull:
            self._enrichment_seen.pop(mint, None)

    async def _enrichment_loop(self) -> None:
        while True:
            mint = await self._enrichment_queue.get()
            try:
                enrichment = await self.enrichment.get_token(mint)
                token = self.pipeline.tokens.apply_enrichment(
                    mint,
                    enrichment.model_dump(mode="json"),
                    enrichment.observed_at,
                )
                if token is not None and self.database is not None:
                    await self.database.upsert_token(token)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("token enrichment failed", extra={"mint": mint})
                self._enrichment_seen.pop(mint, None)
            finally:
                self._enrichment_queue.task_done()

    async def _daily_report_loop(self) -> None:
        from datetime import timedelta
        from zoneinfo import ZoneInfo

        zone = ZoneInfo(self.config.time_zone)
        while True:
            try:
                now = datetime.now(zone)
                hour, minute = (
                    int(item)
                    for item in self.config.telegram.daily_report_time.split(":")
                )
                if (now.hour, now.minute) >= (hour, minute):
                    await self.daily_report_if_not_sent(
                        (now.date() - timedelta(days=1)).isoformat(), send=True
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("daily report loop failed")
            await asyncio.sleep(30)

    async def _maintenance_loop(self) -> None:
        while True:
            try:
                await asyncio.to_thread(self.retention.run)
                if self.database is not None:
                    await self.database.run_retention(
                        raw_retention_days=self.config.storage.raw_retention_days,
                        api_call_retention_days=(
                            self.config.storage.api_call_retention_days
                        ),
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("retention maintenance loop failed")
            await asyncio.sleep(21600)

    async def _heartbeat_loop(self) -> None:
        while True:
            try:
                if self.database is not None and self._system_run_id is not None:
                    await self.database.heartbeat_system_run(
                        self._system_run_id, datetime.now(tz=timezone.utc)
                    )
            except asyncio.CancelledError:
                raise
            except ActiveRuntimeError:
                self.entry_gate.block("runtime_lease_lost")
                logger.critical("runtime lease lost; terminating fail-closed", exc_info=True)
                os.kill(os.getpid(), signal.SIGTERM)
                return
            except Exception:
                logger.exception("system heartbeat loop failed")
            await asyncio.sleep(30)

    async def _build_security_context(
        self,
        candidate: Candidate,
        snapshot: FeatureSnapshot,
    ) -> SecurityContext:
        token = self.pipeline.tokens.get(candidate.mint)
        pool = self.pipeline.pools.pool(candidate.pool_address)
        if pool is None:
            raise RuntimeError("candidate pool is unavailable")
        dev_wallet = token.creator_address if token else None
        await self._ensure_wallet_history(dev_wallet)
        self._request_enrichment(candidate.mint)
        at = snapshot.snapshot_time
        entry_decision = candidate.state == CandidateState.ENTRY_PENDING
        cached = self._security_inputs.get(candidate.candidate_id)
        refresh_holders = cached is None or at - cached.holders_at >= timedelta(
            seconds=self.config.execution.holder_refresh_seconds
        )
        # The score may use the last periodic round trip; the entry decision
        # itself always re-quotes so it passes the strict quote-age bound.
        refresh_quote = (
            entry_decision
            or cached is None
            or at - cached.quote_at
            >= timedelta(seconds=self.config.execution.quote_refresh_seconds)
        )
        quote_priority = (
            QuotePriority.ENTRY if entry_decision else QuotePriority.SECURITY
        )
        if refresh_holders:
            mint_info = await self._counted(
                "mint", self.rpc.get_mint_info(candidate.mint)
            )
            holders_read = self._counted(
                "holders",
                self.rpc.get_all_holders(
                    candidate.mint,
                    expected_supply_raw=mint_info.total_supply_raw,
                    maximum_index_slot_lag=self.config.holders.max_index_slot_lag,
                    supply_tolerance_pct=self.config.holders.index_supply_tolerance_pct,
                ),
            )
            if refresh_quote:
                holder_accounts, round_trip = await asyncio.gather(
                    holders_read,
                    self._counted(
                        "round_trip",
                        self.quote_provider.get_round_trip_quote(
                            quote_token=self.config.base_quote_mint,
                            token=candidate.mint,
                            usdc_amount=Decimal("10"),
                            priority=quote_priority,
                        ),
                    ),
                )
            else:
                assert cached is not None
                round_trip = cached.round_trip
                holder_accounts = await holders_read
            if token is not None and token.total_supply_raw is not None:
                mint_info = mint_info.model_copy(
                    update={"supply_changed": token.total_supply_raw != mint_info.total_supply_raw}
                )
            updated_token = self.pipeline.tokens.apply_mint_state(
                candidate.mint,
                token_program=mint_info.token_program,
                decimals=mint_info.decimals,
                total_supply_raw=mint_info.total_supply_raw,
                observed_at=mint_info.observed_at,
            )
            if updated_token is not None and self.database is not None:
                await self.database.upsert_token(updated_token)
            self.pipeline.update_pool_supply(
                candidate.pool_address,
                total_supply_raw=mint_info.total_supply_raw,
                observed_at=mint_info.observed_at,
            )
        else:
            assert cached is not None
            mint_info = cached.mint_info
            holder_accounts = cached.holder_accounts
            round_trip = (
                await self._counted(
                    "round_trip",
                    self.quote_provider.get_round_trip_quote(
                        quote_token=self.config.base_quote_mint,
                        token=candidate.mint,
                        usdc_amount=Decimal("10"),
                        priority=quote_priority,
                    ),
                )
                if refresh_quote
                else cached.round_trip
            )
        if refresh_quote:
            self._compare_round_trip_to_reserves(candidate, round_trip, at)
        self._security_inputs[candidate.candidate_id] = _SecurityInputs(
            mint_info=mint_info,
            holder_accounts=holder_accounts,
            holders_at=(
                at if refresh_holders or cached is None else cached.holders_at
            ),
            round_trip=round_trip,
            quote_at=at if refresh_quote or cached is None else cached.quote_at,
        )
        owner_scope = {holder.owner for holder in holder_accounts if holder.owner}
        dev_cluster = self.wallet_analyzer.cluster_for(dev_wallet, owner_scope)
        related_cluster = self.wallet_analyzer.largest_related_cluster(
            owner_scope, excluded=dev_cluster
        )
        early_buyers = self.wallet_analyzer.first_buyers(
            candidate.mint, at=snapshot.snapshot_time
        )
        system_addresses = {candidate.pool_address}
        if pool.base_vault:
            system_addresses.add(pool.base_vault)
        if pool.quote_vault:
            system_addresses.add(pool.quote_vault)
        if token is not None and token.bonding_curve_address:
            system_addresses.add(token.bonding_curve_address)
        holders = aggregate_holders(
            holder_accounts,
            total_supply_raw=mint_info.total_supply_raw,
            dev_wallet=dev_wallet,
            dev_cluster=dev_cluster,
            related_cluster=related_cluster,
            early_buyers=early_buyers,
            system_addresses=system_addresses,
        )
        if refresh_holders:
            self._ingest_holder_observation(candidate, snapshot, holders)
        trades = self.pipeline.features.trades(
            candidate.pool_address, at=snapshot.snapshot_time
        )
        dev_sold = bool(
            dev_wallet
            and any(
                event.wallet == dev_wallet
                and event.side.value == "sell"
                and event.event_time <= snapshot.snapshot_time
                for event in trades
            )
        )
        stream_time = self.stream_gateway.last_observed_at or datetime.fromtimestamp(0, tz=timezone.utc)
        developer_profile = self.wallet_analyzer.profile(
            dev_wallet, at=snapshot.snapshot_time
        )
        return SecurityContext(
            mint=mint_info,
            holders=holders,
            holders_observed_at=mint_info.observed_at,
            execution=ExecutionChecks(
                buy_route_available=True,
                sell_route_available=True,
                round_trip_loss_pct=round_trip.loss_pct,
                buy_price_impact_pct=round_trip.buy.price_impact_pct,
                sell_price_impact_pct=round_trip.sell.price_impact_pct,
                quote_observed_at=round_trip.sell.received_at,
            ),
            quote_mint=pool.quote_mint,
            quote_liquidity_usd=snapshot.quote_liquidity_usd,
            liquidity_change_30s=snapshot.quote_liquidity_change_30s,
            pool_age_seconds=snapshot.pool_age_seconds,
            external_successful_sellers=snapshot.external_successful_sellers,
            stream_observed_at=stream_time,
            dev_sold=dev_sold,
            previous_rugs=(
                developer_profile.tokens_with_liquidity_rug if developer_profile else 0
            ),
            previous_dev_dumps_5m=(
                developer_profile.tokens_with_dev_dump_5m if developer_profile else 0
            ),
            developer_tokens_created_7d=(
                developer_profile.tokens_created_7d if developer_profile else 0
            ),
            developer_successful_tokens=(
                developer_profile.tokens_reaching_2x_executable if developer_profile else 0
            ),
            developer_history_known=bool(
                developer_profile and developer_profile.tokens_created_total > 1
            ),
            protocol_layout_known=not any(
                reason == f"protocol:{pool.protocol}" for reason in self.entry_gate.reasons
            ),
            critical_api_available=self.database_available,
            wash_trading_pattern=(
                snapshot.same_funder_buy_share > self.config.flow.max_same_funder_buy_share
                or (
                    snapshot.buyer_volume_hhi > Decimal("0.15")
                    and snapshot.transactions_per_trader > self.config.flow.max_transactions_per_trader
                )
            ),
            return_since_pool_creation=snapshot.return_since_pool_creation,
        )

    async def _open_candidate(
        self,
        candidate: Candidate,
        snapshot: FeatureSnapshot,
        score: ScoreBreakdown,
        security: SecurityContext,
    ) -> RejectReason | None:
        if self.broker is None:
            return RejectReason.RISK_MANAGER_BLOCKED
        sizing = self._size_entry(security, score, fresh_day=False)
        inputs = self._security_inputs.get(candidate.candidate_id)
        reference = (
            FillReference(
                tokens_per_usd=inputs.round_trip.buy.out_amount
                / inputs.round_trip.starting_usd,
                quoted_at=inputs.round_trip.buy.received_at,
            )
            if inputs is not None and inputs.round_trip.starting_usd > 0
            else None
        )
        if not sizing.allowed:
            if sizing.reject_reason == SizingRejectReason.POSITION_TOO_SMALL_AFTER_COSTS:
                return RejectReason.POSITION_TOO_SMALL_AFTER_COSTS
            await self._open_shadow(
                candidate, security, score, reference, block_reason="NO_DAILY_RISK_BUDGET"
            )
            return RejectReason.DAILY_RISK_LIMIT
        risk = self.risk_manager.evaluate_entry(sizing.position_size_usd, candidate.mint)
        if risk.decision.value != "allow":
            if risk.reason in SHADOW_BLOCK_REASONS:
                await self._open_shadow(
                    candidate, security, score, reference, block_reason=str(risk.reason)
                )
            if self.database is not None and self.database_available:
                await self.database.persist_risk_state(
                    account_id="paper-main",
                    halt_reason=self.ledger.state.halt_reason,
                    pause_until=self.ledger.state.pause_until,
                    daily_halt_date=self.ledger.state.daily_halt_date,
                    updated_at=datetime.now(tz=timezone.utc),
                )
            if risk.reason == "MAX_OPEN_POSITIONS_LIMIT":
                return RejectReason.MAX_OPEN_POSITIONS
            if risk.reason == "DAILY_LOSS_LIMIT":
                return RejectReason.DAILY_RISK_LIMIT
            return RejectReason.RISK_MANAGER_BLOCKED
        try:
            await self.broker.open(
                candidate.mint,
                sizing.position_size_usd,
                order_id=f"entry:{candidate.candidate_id}",
                candidate_id=candidate.candidate_id,
                pool_address=candidate.pool_address,
                entry_score=score.total_score,
                entry_liquidity_usd=snapshot.quote_liquidity_usd,
                entry_pool_age_seconds=snapshot.pool_age_seconds,
                round_trip_cost_pct=security.execution.round_trip_loss_pct,
                alert_text=(
                    f"paper entry | mint={candidate.mint[:6]}...{candidate.mint[-4:]} "
                    f"size=${sizing.position_size_usd} score={score.total_score}"
                ),
                reference=reference,
            )
        except EntrySlippageExceededError:
            self._sync_paper_metrics()
            return RejectReason.ENTRY_SLIPPAGE_EXCEEDED
        except ExecutionBlockedError:
            # A limit reached between the check above and the atomic commit
            # (e.g. another entry filled meanwhile) rejects this candidate
            # instead of aborting the whole evaluation pass.
            await self._open_shadow(
                candidate, security, score, reference, block_reason="ATOMIC_RISK_LIMIT"
            )
            return RejectReason.RISK_MANAGER_BLOCKED
        self.metrics.paper_orders.labels(status="filled").inc()
        self._sync_paper_metrics()
        return None

    async def evaluate_and_close_exits(
        self,
        *,
        now: datetime | None = None,
        policy: ExitPolicy | None = None,
        max_positions: int | None = None,
    ) -> list[ExitDecision]:
        if self.broker is None:
            return []

        now = now or datetime.now(tz=timezone.utc)
        policy = policy or self._exit_policy()
        decisions: list[ExitDecision] = []
        positions = list(self.ledger.open_positions)
        close_limit = max_positions if max_positions is not None else len(positions)

        prices: dict[str, Decimal] = {}
        executable_values: dict[str, Decimal] = {}
        for position in positions:
            reserve_usd = self._reserve_mark_usd(position, now)
            if self.config.exits.mark_source == "reserves" and reserve_usd is not None:
                # Valued from the tracked pool reserves: no provider call per
                # pass. Fills still go through Jupiter after the delay.
                self._mark_quote_failures.pop(position.position_id, None)
                executable_values[position.position_id] = reserve_usd
                prices[position.token_mint] = (
                    reserve_usd / position.remaining_token_amount
                    if position.remaining_token_amount > 0 else Decimal("0")
                )
                continue
            try:
                quote = await self.quote_provider.get_sell_quote_mark_to_market(
                    token=position.token_mint,
                    quote_token=self.config.base_quote_mint,
                    token_amount=position.remaining_token_amount,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                first_failed_at, attempts = self._mark_quote_failures.get(
                    position.position_id, (now, 0)
                )
                self._mark_quote_failures[position.position_id] = (
                    first_failed_at,
                    attempts + 1,
                )
                if (
                    now - first_failed_at
                ).total_seconds() >= self.config.paper.exit_retry_timeout_seconds:
                    candidate_id = position.candidate_id
                    candidate = (
                        self.pipeline.candidates.get(candidate_id)
                        if candidate_id
                        else None
                    )
                    if candidate is not None and candidate.state in {
                        CandidateState.POSITION_OPEN,
                        CandidateState.POSITION_PARTIAL,
                        CandidateState.RETRYING_EXIT,
                    }:
                        await self.pipeline.transition_candidate(
                            candidate.candidate_id,
                            CandidateState.EXIT_PENDING,
                            now,
                        )
                    try:
                        await self.broker.close(
                            token_mint=position.token_mint,
                            token_amount=position.remaining_token_amount,
                            order_id=f"exit:{position.position_id}:UNRECOVERABLE",
                            exit_reason="UNRECOVERABLE",
                        )
                    except Exception:
                        if candidate is not None:
                            await self.pipeline.transition_candidate(
                                candidate.candidate_id,
                                CandidateState.RETRYING_EXIT,
                                now,
                            )
                        raise
                    if candidate is not None:
                        await self.pipeline.transition_candidate(
                            candidate.candidate_id, CandidateState.CLOSED, now
                        )
                    decisions.append(
                        ExitDecision(
                            True,
                            ExitReason.EMERGENCY,
                            -position.remaining_cost_usd,
                            Decimal("-1"),
                            Decimal("1"),
                        )
                    )
                    self._mark_quote_failures.pop(position.position_id, None)
                continue
            self._mark_quote_failures.pop(position.position_id, None)
            executable_usd = quote.out_amount_usd if quote.out_amount_usd else quote.out_amount
            executable_values[position.position_id] = executable_usd
            if reserve_usd is not None:
                self._record_mark_comparison(position, reserve_usd, executable_usd, now)
            prices[position.token_mint] = (
                executable_usd / position.remaining_token_amount
                if position.remaining_token_amount > 0 else Decimal("0")
            )
        if prices and len(prices) == len(positions):
            self.ledger.mark_to_market(prices, observed_at=now)
            if (
                self.database is not None
                and self.database_available
                and (
                    self._last_equity_mark_at is None
                    or now - self._last_equity_mark_at >= EQUITY_MARK_OPEN_INTERVAL
                )
            ):
                await self.database.update_paper_marks(
                    account_id="paper-main",
                    positions=list(self.ledger.open_positions),
                    executable_values=executable_values,
                    account_snapshot=self.ledger.snapshot(),
                    observed_at=now,
                )
                self._last_equity_mark_at = now
        elif (
            self.database is not None
            and self.database_available
            and (
                self._last_equity_mark_at is None
                or now - self._last_equity_mark_at >= EQUITY_MARK_HEARTBEAT
            )
        ):
            await self.database.record_equity_heartbeat(
                account_id="paper-main", observed_at=now
            )
            self._last_equity_mark_at = now

        for index, position in enumerate(positions):
            if index >= close_limit:
                break
            marked_usd = executable_values.get(position.position_id)
            if marked_usd is None:
                continue
            executable_usd = marked_usd
            if position.remaining_token_amount <= 0:
                executable_price = Decimal("0")
            else:
                executable_price = executable_usd / position.remaining_token_amount
            candidate = next(
                (
                    item for item in self.pipeline.candidates.values()
                    if item.mint == position.token_mint
                ),
                None,
            )
            feature = (
                self.pipeline.features.snapshot(candidate.pool_address, now)
                if candidate is not None else None
            )
            momentum_now = bool(
                feature
                and feature.buy_sell_volume_ratio < Decimal("0.8")
                and feature.unique_sellers_30s > feature.unique_buyers_30s
            )
            self._momentum_windows[position.position_id] = (
                self._momentum_windows.get(position.position_id, 0) + 1
                if momentum_now else 0
            )
            token = self.pipeline.tokens.get(position.token_mint)
            dev_wallet = token.creator_address if token else None
            dev_sold = bool(
                dev_wallet and candidate and any(
                    trade.wallet == dev_wallet
                    and trade.side.value == "sell"
                    and trade.event_time >= position.opened_at
                    for trade in self.pipeline.features.trades(candidate.pool_address, at=now)
                )
            )
            emergency = bool(
                self.daily_loss_exceeded()
                or self.drawdown_exceeded()
                or dev_sold
                or (
                    feature
                    and feature.quote_liquidity_change_30s
                    <= -self.config.liquidity.emergency_liquidity_drop_pct
                )
            )
            if self.drawdown_exceeded():
                self.risk_manager.set_hard_halt("ALL_TIME_DRAWDOWN_LIMIT")
            elif self.daily_loss_exceeded():
                self.ledger.set_daily_halt(
                    self.ledger.current_date_key(now), "DAILY_LOSS_LIMIT"
                )
            if self.database is not None and self.database_available and (
                self.drawdown_exceeded() or self.daily_loss_exceeded()
            ):
                await self.database.persist_risk_state(
                    account_id="paper-main",
                    halt_reason=self.ledger.state.halt_reason,
                    pause_until=self.ledger.state.pause_until,
                    daily_halt_date=self.ledger.state.daily_halt_date,
                    updated_at=now,
                )
            decision = evaluate_exit(
                position,
                executable_price,
                now,
                policy=policy,
                momentum_exit=(
                    self._momentum_windows[position.position_id]
                    >= self.config.exits.momentum_exit_windows
                ),
                emergency_exit=emergency,
            )
            if not decision.should_exit:
                decisions.append(decision)
                continue

            candidate_id = position.candidate_id
            if candidate_id:
                candidate = self.pipeline.candidates.get(candidate_id)
                if candidate is not None and candidate.state in {
                    CandidateState.POSITION_OPEN,
                    CandidateState.POSITION_PARTIAL,
                    CandidateState.RETRYING_EXIT,
                }:
                    await self.pipeline.transition_candidate(
                        candidate_id, CandidateState.EXIT_PENDING, now
                    )
            try:
                close_amount = position.remaining_token_amount * decision.close_fraction
                await self.broker.close(
                    token_mint=position.token_mint,
                    token_amount=close_amount,
                    order_id=f"exit:{position.position_id}:{decision.reason.value}",
                    exit_reason=decision.reason.value,
                )
            except Exception as exc:
                await self._notify_system_alert(
                    "system alert: auto_exit failed token={token} reason={reason} error={error}".format(
                        token=position.token_mint,
                        reason=decision.reason.value,
                        error=exc,
                    )
                )
                if candidate_id:
                    candidate = self.pipeline.candidates.get(candidate_id)
                    if candidate is not None and candidate.state == CandidateState.EXIT_PENDING:
                        await self.pipeline.transition_candidate(
                            candidate_id, CandidateState.RETRYING_EXIT, now
                        )
                raise

            if candidate_id:
                candidate = self.pipeline.candidates.get(candidate_id)
                if candidate is not None and candidate.state == CandidateState.EXIT_PENDING:
                    target = (
                        CandidateState.POSITION_PARTIAL
                        if position.status.value == "open"
                        else CandidateState.CLOSED
                    )
                    await self.pipeline.transition_candidate(candidate_id, target, now)

            if not self.database_available:
                await self._notify_trade_alert(
                    "trade alert: auto_exit token={token} reason={reason} pnl={pnl}".format(
                        token=position.token_mint,
                        reason=decision.reason.value,
                        pnl=decision.executable_pnl_usd,
                    )
                )
            decisions.append(decision)

        self._sync_paper_metrics()
        if self.database is not None and self.database_available:
            await self.database.save_runtime_checkpoint(
                checkpoint_key="paper-main:exit-monitor",
                state={"momentum_windows": self._momentum_windows},
                updated_at=now,
            )

        return decisions

    async def _notify_trade_alert(self, message: str) -> None:
        logger.info("proactive Telegram trade alert suppressed")

    async def _notify_lifecycle_alert(
        self, event_type: str, message: str
    ) -> None:
        if event_type not in {"system_start", "system_stop"}:
            raise ValueError("unsupported Telegram lifecycle event type")
        if not self.config.telegram.enabled:
            return
        if event_type == "system_stop" and not self._lifecycle_start_sent:
            return
        if self.database is not None and self.database_available:
            run_id = self._system_run_id or "unregistered"
            try:
                await self.database.enqueue_outbox(
                    idempotency_key=f"telegram:{event_type}:{run_id}",
                    event_type=event_type,
                    payload={"text": message},
                )
            except Exception as exc:
                logger.error(
                    "lifecycle alert outbox enqueue failed error_type=%s",
                    type(exc).__name__,
                )
            else:
                self._lifecycle_start_sent = event_type == "system_start"
                return
        notifier = getattr(self, "notifier", None)
        if notifier is None or not hasattr(notifier, "send"):
            return
        try:
            await notifier.send(message)
        except Exception as exc:
            logger.warning(
                "lifecycle alert delivery failed error_type=%s",
                type(exc).__name__,
            )
        else:
            self._lifecycle_start_sent = event_type == "system_start"

    async def _notify_daily_report(self, report: dict[str, Any]) -> bool:
        if not self.config.telegram.enabled:
            return False
        notifier = getattr(self, "notifier", None)
        if notifier is None or not hasattr(notifier, "send"):
            return False
        try:
            await notifier.send(_telegram_report_text(report))
        except Exception as exc:
            logger.warning(
                "daily report delivery failed error_type=%s",
                type(exc).__name__,
            )
            return False
        return True

    async def _notify_system_alert(self, message: str) -> None:
        logger.warning("proactive Telegram system alert suppressed")
    def _report_id(self, payload: dict[str, object]) -> str:
        report_payload = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(report_payload.encode("utf-8")).hexdigest()[:24]

    @staticmethod
    def _normalize_value(value: object) -> object:
        if isinstance(value, Decimal):
            return str(value)
        return value

    def _normalize_report_payload(self, payload: dict[str, object]) -> dict[str, object]:
        return {key: self._normalize_value(value) for key, value in payload.items()}

    def _is_sample_size_low(self, date: str, *, min_samples: int = 3) -> bool:
        fills = self.ledger.iter_fills()
        filled_today = [
            fill for fill in fills
            if fill.fill_type is not None and fill.created_at.strftime("%Y-%m-%d") == date
        ]
        return len(filled_today) < min_samples

    def _load_report_runs(self) -> dict[str, object]:
        if not self._report_runs_path.exists():
            return {"last_daily_report_date": None, "last_daily_report_id": None}
        try:
            with self._report_runs_path.open("r", encoding="utf-8") as file:
                data = json.load(file)
            if not isinstance(data, dict):
                raise TypeError("report run payload must be dict")
            return {
                "last_daily_report_date": data.get("last_daily_report_date"),
                "last_daily_report_id": data.get("last_daily_report_id"),
            }
        except Exception:
            return {"last_daily_report_date": None, "last_daily_report_id": None}

    def _daily_report_already_sent(self, date: str) -> bool:
        return self._report_runs.get("last_daily_report_date") == date

    def _record_daily_report_sent(self, date: str, report_id: str) -> None:
        self._report_runs["last_daily_report_date"] = date
        self._report_runs["last_daily_report_id"] = report_id
        self._persist_report_runs()

    def _persist_report_runs(self) -> None:
        with self._report_runs_path.open("w", encoding="utf-8") as file:
            json.dump(self._report_runs, file, ensure_ascii=False, indent=2)

    @staticmethod
    def _today_key() -> str:
        from zoneinfo import ZoneInfo

        return datetime.now(ZoneInfo("Europe/Kyiv")).strftime("%Y-%m-%d")
