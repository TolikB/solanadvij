from __future__ import annotations

from decimal import Decimal

import pytest

from sniper_bot.config import AppConfig
from sniper_bot.outbox import PROACTIVE_TELEGRAM_EVENT_TYPES


def _base_config(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "APP_MODE": "paper",
        "HELIUS_API_KEY": "helius-key",
        "JUPITER_API_KEY": "jupiter-key",
        "POSTGRES_DSN": "postgresql://user:pass@localhost:5432/db",
        "TELEGRAM_BOT_TOKEN": "telegram-token",
        "TELEGRAM_ADMIN_CHAT_ID": 123456,
        "STARTING_EQUITY_USD": Decimal("500"),
    }
    values.update(overrides)
    return values


def test_proactive_telegram_policy_has_exactly_three_message_types() -> None:
    assert PROACTIVE_TELEGRAM_EVENT_TYPES == {
        "system_start",
        "system_stop",
        "daily_report",
    }


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"TIME_ZONE": "UTC"}, "TIME_ZONE must be Europe/Kyiv"),
        (
            {"telegram": {"enabled": True, "daily_report_time": "00:01"}},
            "daily_report_time must be 00:00",
        ),
    ],
)
def test_enabled_telegram_schedule_is_fail_closed(
    override: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        AppConfig(**_base_config(**override))


def test_replay_mode_can_use_an_alternate_reporting_timezone() -> None:
    config = AppConfig(
        **_base_config(
            TIME_ZONE="UTC",
            JUPITER_REPLAY_MODE=True,
            JUPITER_QUOTE_JOURNAL_PATH="fixtures/quotes.ndjson",
        )
    )

    assert config.replay_mode is True
    assert config.time_zone == "UTC"
