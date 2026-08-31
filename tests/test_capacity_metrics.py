from sniper_bot.metrics import BotMetrics


def test_capacity_metrics_expose_queue_breakdown_and_bounded_batch_phases() -> None:
    metrics = BotMetrics()
    metrics.event_notification_queue_depth.set(11)
    metrics.event_log_dispatch_tasks.set(7)
    metrics.event_processing_queue_depth.set(3)
    metrics.chain_transaction_batch_size.observe(512)
    metrics.chain_decoded_event_batch_size.observe(128)
    metrics.chain_batch_phase_seconds.labels(phase="decode").observe(0.25)

    rendered = metrics.render().decode("utf-8")

    assert "event_notification_queue_depth 11.0" in rendered
    assert "event_log_dispatch_tasks 7.0" in rendered
    assert "event_processing_queue_depth 3.0" in rendered
    assert "chain_transaction_batch_size_count 1.0" in rendered
    assert "chain_decoded_event_batch_size_count 1.0" in rendered
    assert 'chain_batch_phase_seconds_count{phase="decode"} 1.0' in rendered