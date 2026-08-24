from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sniper_bot.config import AppConfig
from sniper_bot.runtime import SniperRuntime


def test_daily_report_uses_dst_aware_kyiv_boundaries(tmp_path) -> None:
    runtime = SniperRuntime(
        AppConfig(
            **{
                "APP_MODE": "record", "HELIUS_API_KEY": "helius", "JUPITER_API_KEY": "jupiter",
                "POSTGRES_DSN": "postgresql://user:pass@localhost/db", "TELEGRAM_BOT_TOKEN": "tg",
                "TELEGRAM_ADMIN_CHAT_ID": 123, "STARTING_EQUITY_USD": Decimal("500"),
                "TIME_ZONE": "Europe/Kyiv",
            }
        ),
        data_dir=tmp_path,
    )

    report = runtime.build_daily_report("2026-03-29")
    start = datetime.fromisoformat(report["period_start_utc"])
    end = datetime.fromisoformat(report["period_end_utc"])

    assert (end - start).total_seconds() == 23 * 60 * 60
