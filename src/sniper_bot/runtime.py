"""Runtime orchestrator."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import signal
import socket
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

from .broker import PaperBroker
from .candidates import Candidate, CandidateState
from .config import AppConfig, AppMode
from .database import ActiveRuntimeError, Database, _telegram_report_text
from .enrichment import DexscreenerClient
from .events import ChainEventType, EventEnvelope, Protocol
from .exit_engine import ExitDecision, ExitPolicy, ExitReason, evaluate_exit
from .external_journal import ExternalJournal
from .features import FeatureSnapshot, HolderObservation
from .id_utils import DeterministicIdFactory
from .jupiter import JupiterQuoteProvider
from .ledger import PaperLedger
from .maintenance import RawRetentionManager
from .metrics import BotMetrics
from .models import QuoteResponse
from .outbox import TelegramOutboxWorker
from .pipeline import ConfirmationPipeline
from .registry import USDC_MINT, WSOL_MINT, QuoteAssetPrice
from .reports import ReportBuilder
from .risk import RiskManager
from .scoring import ScoreBreakdown
from .security import ExecutionChecks, RejectReason, SecurityContext, aggregate_holders
from .sizing import PositionSizingInput, SizingRejectReason, calculate_position_size
from .solana_rpc import SolanaRpcClient
from .stream import EntryGate, HeliusStreamGateway
from .telegram import NoopTelegramNotifier, TelegramNotifier
from .wallet_analysis import WalletAnalyzer

logger = logging.getLogger(__name__)


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
        self._enrichment_seen: set[str] = set()
        self._momentum_windows: dict[str, int] = {}
        self._mark_quote_failures: dict[str, tuple[datetime, int]] = {}
        self.wallet_analyzer = WalletAnalyzer()
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
            base_url="https://api.jup.ag/swap/v2",
            quote_mint=config.base_quote_mint,
            quote_mint_decimals=config.base_quote_decimals,
            timeout_seconds=config.execution.quote_timeout_ms / 1000,
            max_retries=config.execution.max_quote_retries,
            replay_mode=config.replay_mode,
            record_quotes=config.quote_journal_record,
            quote_journal_path=config.quote_journal_path,
            recorder=(self.database.record_external_api_call if self.database else None),
            metrics=self.metrics,
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
            )
        self.notifier: NoopTelegramNotifier | TelegramNotifier
        if config.replay_mode:
            self.notifier = NoopTelegramNotifier()
        else:
            self.notifier = TelegramNotifier(
                config.telegram_bot_token.get_secret_value(),
                config.telegram_admin_chat_id,
                allowlist=config.telegram_allowlist_chat_ids,
                user_allowlist=config.telegram_allowlist_user_ids,
            )
        self.outbox_worker: TelegramOutboxWorker | None = None
        if self.database is not None and not config.replay_mode:
            self.outbox_worker = TelegramOutboxWorker(
                self.database,
                self.notifier,
                metrics=self.metrics,
            )
        self.rpc = SolanaRpcClient(
            config.resolved_helius_rpc_url(),
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
            entry_gate=self.entry_gate,
            metrics=self.metrics,
            max_processing_lag_seconds=(
                float(config.chain.max_stream_lag_ms) / 1000.0
            ),
        )
        self.report_builder = ReportBuilder(self)
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
                profiles, relations = await self.database.load_wallet_analysis()
                self.wallet_analyzer.restore(profiles, relations)
                quarantined_protocols = (
                    await self.database.load_quarantined_event_protocols()
                )
                recovery_since = datetime.now(tz=timezone.utc) - timedelta(
                    seconds=max(
                        300,
                        self.config.candidate.max_pool_age_seconds + 60,
                        self.config.exits.maximum_holding_seconds + 60,
                    )
                )
                for event in await self.database.load_processed_events_since(recovery_since):
                    self.stream_gateway.restore_protocol_checkpoint(
                        event.protocol, event.signature
                    )
                    self.wallet_analyzer.observe(event)
                    await self.pipeline.rehydrate_event(event)
                self.pipeline.restore_candidates(
                    await self.database.load_active_candidates(self.config.strategy_version)
                )
                self.pipeline.restore_score_totals(
                    await self.database.load_candidate_score_totals(
                        self.config.strategy_version
                    )
                )
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
            except Exception as exc:
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
                if isinstance(exc, ActiveRuntimeError):
                    raise
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
            asyncio.create_task(self._freshness_loop(), name="freshness-loop"),
            asyncio.create_task(self._quote_asset_loop(), name="quote-asset-loop"),
            asyncio.create_task(self._enrichment_loop(), name="enrichment-loop"),
            asyncio.create_task(self._daily_report_loop(), name="daily-report-loop"),
            asyncio.create_task(self._maintenance_loop(), name="maintenance-loop"),
            asyncio.create_task(self._heartbeat_loop(), name="heartbeat-loop"),
            asyncio.create_task(self._warmup(), name="warmup"),
        ]
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
        date = date or self._today_key()
        snapshot = self.ledger.snapshot()
        open_positions = self._open_positions_snapshot()
        reconciled = self._normalize_report_payload(self.ledger.reconcile())
        report = {
            "period": "daily",
            "date": date,
            "timezone": self.config.time_zone,
            "config_hash": snapshot["config_hash"],
            "strategy_version": snapshot["strategy_version"],
            "equity_usd": self._normalize_value(snapshot["equity_usd"]),
            "realized_pnl_usd": self._normalize_value(snapshot["realized_pnl_usd"]),
            "unrealized_pnl_usd": self._normalize_value(snapshot["unrealized_pnl_usd"]),
            "pnl": self._normalize_value(self.ledger.daily_pnl(date)),
            "sample_size_warning": self._is_sample_size_low(date),
            "operational_costs_usd": "0",
            "reconcile": reconciled,
            "open_positions": open_positions,
        }
        report["report_id"] = self._report_id(report)
        return report

    def build_all_time_report(self) -> dict[str, object]:
        return self.report_builder.all_time()
        snapshot = self.ledger.snapshot()
        open_positions = self._open_positions_snapshot()
        report = {
            "period": "all_time",
            "date": "all_time",
            "timezone": self.config.time_zone,
            "config_hash": snapshot["config_hash"],
            "strategy_version": snapshot["strategy_version"],
            "equity_usd": self._normalize_value(snapshot["equity_usd"]),
            "realized_pnl_usd": self._normalize_value(snapshot["realized_pnl_usd"]),
            "unrealized_pnl_usd": self._normalize_value(snapshot["unrealized_pnl_usd"]),
            "pnl": self._normalize_value(self.ledger.reconcile()["realized_pnl_usd"]),
            "operational_costs_usd": "0",
            "reconcile": self._normalize_report_payload(self.ledger.reconcile()),
            "open_positions": open_positions,
        }
        report["report_id"] = self._report_id(report)
        return report

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
                event_type="daily_report_unavailable",
                payload={
                    "text": (
                        "Щоденний звіт про тестову торгівлю\n"
                        f"Дата: {target}\n"
                        "Дані за цей день недоступні."
                    )
                },
            )
            return report if inserted else None
        report = self.report_builder.daily(target, capital_bounds=capital_bounds)
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
        if not self.config.replay_mode:
            await self.stream_gateway.stop()
        if self.outbox_worker is not None:
            drained = await self.outbox_worker.drain(timeout_seconds=5.0)
            if not drained:
                logger.warning(
                    "telegram outbox drain did not complete; undelivered events remain durable"
                )
            await self.outbox_worker.stop()
        if self._persistence_tasks:
            await asyncio.gather(*self._persistence_tasks, return_exceptions=True)
            self._persistence_tasks.clear()
        if self.database is not None:
            try:
                if self._system_run_id is not None:
                    await self.database.stop_system_run(
                        self._system_run_id,
                        reason="graceful_shutdown",
                        now=datetime.now(tz=timezone.utc),
                    )
            finally:
                await self.database.close()
        await self.notifier.stop()
        self._started = False

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

    async def _candidate_loop(self) -> None:
        while True:
            try:
                await self.pipeline.evaluate_candidates()
                self.entry_gate.unblock("security_data_unavailable")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("candidate evaluation loop failed")
                self.entry_gate.block("security_data_unavailable")
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

    async def _observe_event(self, event: EventEnvelope) -> None:
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
        if (
            self.config.enrichment.enabled
            and not self.config.replay_mode
            and event.event_type == ChainEventType.TOKEN_CREATED
            and event.mint
            and event.mint not in self._enrichment_seen
        ):
            self._enrichment_seen.add(event.mint)
            try:
                self._enrichment_queue.put_nowait(event.mint)
            except asyncio.QueueFull:
                self._enrichment_seen.discard(event.mint)

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
                self._enrichment_seen.discard(mint)
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
                        raw_retention_days=self.config.storage.raw_retention_days
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
        mint_info = await self.rpc.get_mint_info(candidate.mint)
        holder_accounts, round_trip = await asyncio.gather(
            self.rpc.get_all_holders(
                candidate.mint,
                expected_supply_raw=mint_info.total_supply_raw,
            ),
            self.quote_provider.get_round_trip_quote(
                quote_token=self.config.base_quote_mint,
                token=candidate.mint,
                usdc_amount=Decimal("10"),
            ),
        )
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
        account = self.ledger.snapshot()
        sizing = calculate_position_size(
            PositionSizingInput(
                current_equity_usd=Decimal(str(account["equity_usd"])),
                daily_pnl_usd=self.ledger.daily_pnl(self.ledger.current_date_key()),
                quote_liquidity_usd=security.quote_liquidity_usd,
                estimated_round_trip_cost_pct=security.execution.round_trip_loss_pct,
                score=score.total_score,
                hard_stop_pct=self.config.risk.hard_stop_pct,
                daily_loss_limit_usd=self.config.risk.daily_loss_limit_usdc,
                maximum_position_usd=self.config.risk.max_position_usdc,
                minimum_position_usd=self.config.risk.min_position_usdc,
            )
        )
        if not sizing.allowed:
            if sizing.reject_reason == SizingRejectReason.POSITION_TOO_SMALL_AFTER_COSTS:
                return RejectReason.POSITION_TOO_SMALL_AFTER_COSTS
            return RejectReason.DAILY_RISK_LIMIT
        risk = self.risk_manager.evaluate_entry(sizing.position_size_usd, candidate.mint)
        if risk.decision.value != "allow":
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
        )
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
        policy = policy or ExitPolicy(
            tp1_return=self.config.exits.take_profit_1_pct,
            tp1_size=self.config.exits.take_profit_1_size_pct,
            tp2_return=self.config.exits.take_profit_2_pct,
            tp2_size_of_initial=self.config.exits.take_profit_2_size_pct,
            stop_loss_return=-abs(self.config.risk.hard_stop_pct),
            trailing_stop_pct=self.config.exits.trailing_stop_pct,
            max_hold_seconds=self.config.exits.maximum_holding_seconds,
            no_new_high_seconds=self.config.exits.no_new_high_timeout_seconds,
        )
        decisions: list[ExitDecision] = []
        positions = list(self.ledger.open_positions)
        close_limit = max_positions if max_positions is not None else len(positions)

        quotes: dict[str, QuoteResponse] = {}
        prices: dict[str, Decimal] = {}
        executable_values: dict[str, Decimal] = {}
        for position in positions:
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
            quotes[position.position_id] = quote
            executable_usd = quote.out_amount_usd if quote.out_amount_usd else quote.out_amount
            executable_values[position.position_id] = executable_usd
            prices[position.token_mint] = (
                executable_usd / position.remaining_token_amount
                if position.remaining_token_amount > 0 else Decimal("0")
            )
        if prices and len(prices) == len(positions):
            self.ledger.mark_to_market(prices, observed_at=now)
            if self.database is not None and self.database_available:
                await self.database.update_paper_marks(
                    account_id="paper-main",
                    positions=list(self.ledger.open_positions),
                    executable_values=executable_values,
                    account_snapshot=self.ledger.snapshot(),
                    observed_at=now,
                )

        for index, position in enumerate(positions):
            if index >= close_limit:
                break
            position_quote = quotes.get(position.position_id)
            if position_quote is None:
                continue
            executable_usd = (
                position_quote.out_amount_usd
                if position_quote.out_amount_usd
                else position_quote.out_amount
            )
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
            if self.database is not None and (
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
        if self.database is not None and self.database_available:
            run_id = self._system_run_id or "unregistered"
            try:
                await self.database.enqueue_outbox(
                    idempotency_key=f"telegram:{event_type}:{run_id}",
                    event_type=event_type,
                    payload={"text": message},
                )
                return
            except Exception as exc:
                logger.error(
                    "lifecycle alert outbox enqueue failed error_type=%s",
                    type(exc).__name__,
                )
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

    async def _notify_daily_report(self, report: dict[str, Any]) -> bool:
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
