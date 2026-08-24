from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from sniper_bot.ledger import PaperLedger


def test_reconcile_after_partial_close(tmp_path: Path) -> None:
    ledger = PaperLedger(
        storage_path=tmp_path / "paper_ledger.json",
        starting_equity_usd=Decimal("500"),
        strategy_version="test",
        config_hash="hash",
    )

    position = ledger.open_position(
        token_mint="TOKEN",
        usd_amount=Decimal("100"),
        token_amount=Decimal("10"),
        order_id="order-open",
        quote_id="quote-open",
    )
    close_order = "order-close-1"
    ledger.close_position(
        token_mint="TOKEN",
        usd_received=Decimal("60"),
        token_closed=Decimal("2"),
        order_id=close_order,
        quote_id="quote-close",
    )

    report = ledger.reconcile()
    assert report["open_positions_count"] == 1
    assert report["open_positions_notional_usd"] == Decimal("80")
    assert report["is_reconciled"] is True
    assert report["total_exit_notional_usd"] == Decimal("60")
    assert report["equity_usd"] == Decimal("540")
    assert position.position_id == ledger.snapshot()["positions"][0]["position_id"]


def test_partial_close_keeps_position_without_final_exit_reason(tmp_path: Path) -> None:
    ledger = PaperLedger(
        storage_path=tmp_path / "paper_ledger.json",
        starting_equity_usd=Decimal("500"),
        strategy_version="test",
        config_hash="hash",
    )
    ledger.open_position(
        token_mint="TOKEN",
        usd_amount=Decimal("100"),
        token_amount=Decimal("10"),
        order_id="order-open",
        quote_id="quote-open",
    )
    ledger.close_position(
        token_mint="TOKEN",
        usd_received=Decimal("20"),
        token_closed=Decimal("4"),
        order_id="close-1",
        quote_id="quote-close-1",
        exit_reason="TP1",
    )

    snapshot = ledger.snapshot()
    assert snapshot["positions"][0]["final_exit_reason"] is None
    assert snapshot["positions"][0]["status"] == "open"


def test_final_close_sets_final_exit_reason(tmp_path: Path) -> None:
    ledger = PaperLedger(
        storage_path=tmp_path / "paper_ledger.json",
        starting_equity_usd=Decimal("500"),
        strategy_version="test",
        config_hash="hash",
    )
    position = ledger.open_position(
        token_mint="TOKEN",
        usd_amount=Decimal("100"),
        token_amount=Decimal("10"),
        order_id="order-open",
        quote_id="quote-open",
    )
    ledger.close_position(
        token_mint="TOKEN",
        usd_received=Decimal("120"),
        token_closed=Decimal("10"),
        order_id="close-final",
        quote_id="quote-close",
        exit_reason="EMERGENCY_EXIT",
    )

    snapshot = ledger.snapshot()
    assert snapshot["positions"][0]["final_exit_reason"] == "EMERGENCY_EXIT"
    assert snapshot["positions"][0]["status"] == "closed"
    assert position.final_exit_reason == "EMERGENCY_EXIT"


def test_partial_close_scales_executable_marks_and_unrealized_value(tmp_path: Path) -> None:
    ledger = PaperLedger(
        storage_path=tmp_path / "paper_ledger.json",
        starting_equity_usd=Decimal("500"),
        strategy_version="test",
        config_hash="hash",
    )
    position = ledger.open_position(
        token_mint="TOKEN",
        usd_amount=Decimal("100"),
        token_amount=Decimal("10"),
        order_id="open",
        quote_id="open-quote",
    )
    ledger.mark_to_market(
        {"TOKEN": Decimal("15")},
        observed_at=datetime(2026, 8, 24, tzinfo=timezone.utc),
    )
    ledger.close_position(
        token_mint="TOKEN",
        usd_received=Decimal("60"),
        token_closed=Decimal("5"),
        order_id="partial",
        quote_id="partial-quote",
        exit_reason="TP1",
    )

    assert position.remaining_token_amount == Decimal("5")
    assert position.remaining_cost_usd == Decimal("50")
    assert position.last_executable_value_usd == Decimal("75")
    assert position.highest_executable_value_usd == Decimal("75")
    assert position.peak_unrealized_usd == Decimal("25")
    assert position.realized_pnl_usd == Decimal("10")
    assert ledger.reconcile()["equity_usd"] == Decimal("535")
