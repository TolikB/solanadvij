from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from sniper_bot.config import AppConfig
from sniper_bot.runtime import SniperRuntime
from sniper_bot.service import PaperService
from sniper_bot.telegram import TelegramNotifier


def _base_record_config(mode: str, **extra: object) -> dict:
    data: dict[str, object] = {
        "APP_MODE": mode,
        "HELIUS_API_KEY": "helius-key",
        "JUPITER_API_KEY": "jupiter-key",
        "POSTGRES_DSN": "postgresql://user:pass@localhost:5432/db",
        "TELEGRAM_BOT_TOKEN": "telegram-token",
        "TELEGRAM_ADMIN_CHAT_ID": 123456,
        "STARTING_EQUITY_USD": Decimal("500"),
    }
    data.update(extra)
    return data


def test_record_mode_disables_paper_broker(tmp_path: Path) -> None:
    runtime = SniperRuntime(AppConfig(**_base_record_config("record")), data_dir=tmp_path)

    assert runtime.broker is None
    assert runtime.is_record() is True
    assert runtime.is_paper() is False


def test_paper_mode_enables_broker(tmp_path: Path) -> None:
    runtime = SniperRuntime(AppConfig(**_base_record_config("paper")), data_dir=tmp_path)

    assert runtime.broker is not None
    assert runtime.is_paper() is True
    assert runtime.is_record() is False


@pytest.mark.asyncio
async def test_service_forbidden_in_record_mode(tmp_path: Path) -> None:
    runtime = SniperRuntime(AppConfig(**_base_record_config("record")), data_dir=tmp_path)
    service = PaperService(runtime)

    with pytest.raises(RuntimeError, match="broker is disabled in record mode"):
        await service.open_position("TOKEN", Decimal("10"))


def test_runtime_includes_admin_in_allowlist(tmp_path: Path) -> None:
    runtime = SniperRuntime(
        AppConfig(
            **{
                **_base_record_config("paper"),
                "TELEGRAM_ALLOWLIST_CHAT_IDS": [100, 200],
            }
        ),
        data_dir=tmp_path,
    )

    assert isinstance(runtime.notifier, TelegramNotifier)
    assert runtime.notifier.allow_chat(123456) is True
