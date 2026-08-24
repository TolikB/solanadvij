from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sniper_bot.ledger import PaperLedger


def test_daily_loss_uses_equity_delta_not_absolute_overnight_unrealized(tmp_path) -> None:
    ledger = PaperLedger(
        tmp_path / "ledger.json",
        Decimal("500"),
        "strategy-v1",
        "config-hash",
        time_zone="Europe/Kyiv",
    )
    opened_at = datetime(2026, 8, 23, 20, tzinfo=timezone.utc)
    ledger.open_position(
        "TOKEN",
        Decimal("20"),
        Decimal("20"),
        "entry",
        "quote-entry",
        created_at=opened_at,
    )
    day_start_mark = opened_at + timedelta(hours=1, seconds=1)
    ledger.mark_to_market(
        {"TOKEN": Decimal("0.5")}, observed_at=day_start_mark
    )
    assert ledger.daily_loss_exceeded(
        Decimal("10"), at=day_start_mark
    ) is False

    later = day_start_mark + timedelta(minutes=1)
    ledger.mark_to_market({"TOKEN": Decimal("0")}, observed_at=later)
    assert ledger.daily_loss_exceeded(Decimal("10"), at=later) is True
