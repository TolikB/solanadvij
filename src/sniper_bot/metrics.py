"""Prometheus instrumentation isolated per runtime instance."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest


class BotMetrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry(auto_describe=True)
        self.chain_events_received = Counter(
            "chain_events_received_total", "Decoded chain events", registry=self.registry
        )
        self.chain_events_duplicate = Counter(
            "chain_events_duplicate_total", "Duplicate chain events", registry=self.registry
        )
        self.chain_event_state_filter_decisions = Counter(
            "chain_event_state_filter_decisions_total",
            "Decoded swap decisions excluded from derived live state",
            ["reason"],
            registry=self.registry,
        )
        self.chain_event_processing_lag_ms = Histogram(
            "chain_event_processing_lag_ms",
            "Observed processing lag for chain events",
            buckets=(25, 50, 100, 250, 500, 1000, 3000, 10000),
            registry=self.registry,
        )
        self.websocket_reconnects = Counter(
            "websocket_reconnects_total", "WebSocket reconnects", registry=self.registry
        )
        self.websocket_gap_recoveries = Counter(
            "websocket_gap_recoveries_total", "Recovered stream gaps", registry=self.registry
        )
        self.event_queue_depth = Gauge(
            "event_queue_depth", "Pending events in the processing queue", registry=self.registry
        )
        self.event_notification_queue_depth = Gauge(
            "event_notification_queue_depth",
            "Pending WebSocket notifications awaiting ordered dispatch",
            registry=self.registry,
        )
        self.event_log_dispatch_tasks = Gauge(
            "event_log_dispatch_tasks",
            "Ordered Solana log dispatch tasks awaiting completion",
            registry=self.registry,
        )
        self.event_processing_queue_depth = Gauge(
            "event_processing_queue_depth",
            "Transactions awaiting ordered pipeline processing",
            registry=self.registry,
        )
        self.chain_transaction_batch_size = Histogram(
            "chain_transaction_batch_size",
            "Transactions received by one ordered pipeline batch",
            buckets=(1, 8, 32, 64, 128, 256, 512),
            registry=self.registry,
        )
        self.chain_decoded_event_batch_size = Histogram(
            "chain_decoded_event_batch_size",
            "Decoded events in one durable persistence batch",
            buckets=(1, 8, 32, 64, 128, 256, 512, 1024),
            registry=self.registry,
        )
        self.chain_batch_phase_seconds = Histogram(
            "chain_batch_phase_seconds",
            "Wall time for a bounded ordered chain batch phase",
            labelnames=("phase",),
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
            registry=self.registry,
        )
        self.candidate_count = Gauge(
            "candidate_count", "Current candidates", registry=self.registry
        )
        self.candidate_rejections = Counter(
            "candidate_rejections_total",
            "Candidate rejections",
            labelnames=("reason",),
            registry=self.registry,
        )
        self.signals = Counter("signals_total", "Entry signals", registry=self.registry)
        self.paper_orders = Counter(
            "paper_orders_total",
            "Paper orders",
            labelnames=("status",),
            registry=self.registry,
        )
        self.paper_positions_open = Gauge(
            "paper_positions_open", "Open paper positions", registry=self.registry
        )
        self.paper_pnl_usd = Gauge("paper_pnl_usd", "Paper PnL USD", registry=self.registry)
        self.paper_equity_usd = Gauge(
            "paper_equity_usd", "Paper equity USD", registry=self.registry
        )
        self.paper_drawdown_pct = Gauge(
            "paper_drawdown_pct", "Paper drawdown percent", registry=self.registry
        )
        self.jupiter_requests = Counter(
            "jupiter_requests_total",
            "Jupiter quote requests",
            labelnames=("status",),
            registry=self.registry,
        )
        self.jupiter_latency_ms = Histogram(
            "jupiter_latency_ms", "Jupiter quote latency", registry=self.registry
        )
        self.jupiter_no_route = Counter(
            "jupiter_no_route_total", "Jupiter no-route responses", registry=self.registry
        )
        self.telegram_messages = Counter(
            "telegram_messages_total",
            "Telegram deliveries",
            labelnames=("status",),
            registry=self.registry,
        )
        self.database_query_latency_ms = Histogram(
            "database_query_latency_ms", "Database query latency", registry=self.registry
        )
        self.postgres_event_ingest_phase_seconds = Histogram(
            "postgres_event_ingest_phase_seconds",
            "Wall time for a fixed PostgreSQL durable event-ingest phase",
            labelnames=("phase",),
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
            registry=self.registry,
        )
        self.ingestion_backlog_events = Gauge(
            "ingestion_backlog_events",
            "Queued events by ordered processing stage",
            ["stage"],
            registry=self.registry,
        )
        self.ingestion_oldest_event_age_seconds = Gauge(
            "ingestion_oldest_event_age_seconds",
            "Age of the oldest queued event by stage",
            ["stage"],
            registry=self.registry,
        )
        self.stream_recovery_gap_active = Gauge(
            "stream_recovery_gap_active",
            "One while a provider stream gap remains unresolved",
            registry=self.registry,
        )
        self.ingestion_events_dropped = Counter(
            "ingestion_events_dropped_total",
            "Events rejected before durable persistence",
            ["stage"],
            registry=self.registry,
        )
        self.shutdown_drain_seconds = Histogram(
            "shutdown_drain_seconds",
            "Graceful drain duration by queue stage",
            ["stage"],
            buckets=(0.01, 0.1, 0.5, 1, 5, 15, 30, 60, 120),
            registry=self.registry,
        )
        self.outbox_pending = Gauge(
            "outbox_pending_total", "Undelivered outbox events", registry=self.registry
        )
        self.system_entry_enabled = Gauge(
            "system_entry_enabled", "One when new entries are allowed", registry=self.registry
        )
        self.system_entry_enabled.set(0)

    def render(self) -> bytes:
        return generate_latest(self.registry)
