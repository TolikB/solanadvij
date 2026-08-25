from __future__ import annotations

from decimal import Decimal

import pytest

from sniper_bot.config import AppConfig, AppMode


def _base_config() -> dict:
    return {
        "APP_MODE": AppMode.PAPER,
        "HELIUS_API_KEY": "helius-key",
        "JUPITER_API_KEY": "jupiter-key",
        "POSTGRES_DSN": "postgresql://user:pass@localhost:5432/db",
        "TELEGRAM_BOT_TOKEN": "telegram-token",
        "TELEGRAM_ADMIN_CHAT_ID": 123456,
        "STARTING_EQUITY_USD": Decimal("500"),
    }


def test_replay_mode_requires_quote_journal_path() -> None:
    data = _base_config()
    data["JUPITER_REPLAY_MODE"] = True

    with pytest.raises(ValueError, match="JUPITER_QUOTE_JOURNAL_PATH"):
        AppConfig(**data)


def test_record_mode_requires_quote_journal_path() -> None:
    data = _base_config()
    data["JUPITER_QUOTE_JOURNAL_RECORD"] = True

    with pytest.raises(ValueError, match="JUPITER_QUOTE_JOURNAL_PATH"):
        AppConfig(**data)


def test_quote_journal_path_optional_without_replay_or_record() -> None:
    config = AppConfig(**_base_config())
    assert config.quote_journal_path == ""
    assert config.replay_mode is False
    assert config.quote_journal_record is False


def test_telegram_allowlist_supports_comma_separated_env_values() -> None:
    config = AppConfig(**{**_base_config(), "TELEGRAM_ALLOWLIST_CHAT_IDS": "100, 200, 100"})

    assert config.telegram_allowlist_chat_ids == [100, 200]


def test_load_environment_aliases_override_empty_yaml(tmp_path, monkeypatch) -> None:
    config_path = tmp_path / "default.yaml"
    config_path.write_text(
        "\n".join(
            (
                "APP_MODE: paper",
                'HELIUS_API_KEY: ""',
                'JUPITER_API_KEY: ""',
                'POSTGRES_DSN: ""',
                'TELEGRAM_BOT_TOKEN: ""',
                "TELEGRAM_ADMIN_CHAT_ID: 123456",
                "TELEGRAM_ALLOWED_CHAT_IDS: []",
                "TELEGRAM_ALLOWED_USER_IDS: []",
                "risk:",
                '  max_position_usdc: "25"',
            )
        ),
        encoding="utf-8",
    )
    environment = {
        "HELIUS_API_KEY": "helius-from-env",
        "JUPITER_API_KEY": "jupiter-from-env",
        "POSTGRES_DSN": "postgresql://user:pass@localhost:5432/db",
        "TELEGRAM_BOT_TOKEN": "telegram-from-env",
        "TELEGRAM_ADMIN_CHAT_ID": "654321",
        "TELEGRAM_ALLOWED_CHAT_IDS": "[654321]",
        "TELEGRAM_ALLOWED_USER_IDS": "[654321]",
        "RISK": '{"max_position_usdc":"30"}',
    }
    for name, value in environment.items():
        monkeypatch.setenv(name, value)

    config = AppConfig.load(str(config_path))

    assert config.helius_api_key == "helius-from-env"
    assert config.jupiter_api_key.get_secret_value() == "jupiter-from-env"
    assert config.postgres_dsn == environment["POSTGRES_DSN"]
    assert config.telegram_bot_token.get_secret_value() == "telegram-from-env"
    assert config.telegram_admin_chat_id == 654321
    assert config.telegram_allowlist_chat_ids == [654321]
    assert config.telegram_allowlist_user_ids == [654321]
    assert config.risk.max_position_usdc == Decimal("30")


def test_release_revision_creates_distinct_immutable_strategy_identity() -> None:
    first = AppConfig(**{**_base_config(), "APP_REVISION": "a" * 40})
    second = AppConfig(**{**_base_config(), "APP_REVISION": "b" * 40})
    uppercase = AppConfig(**{**_base_config(), "APP_REVISION": "A" * 40})

    assert first.config_hash == second.config_hash
    assert first.strategy_version != second.strategy_version
    assert first.strategy_version.endswith("-" + "a" * 12)
    assert second.strategy_version.endswith("-" + "b" * 12)
    assert uppercase.release_revision == "a" * 40
    assert uppercase.strategy_version == first.strategy_version

    with pytest.raises(ValueError, match="APP_REVISION"):
        AppConfig(**{**_base_config(), "APP_REVISION": "not-a-commit"})
    with pytest.raises(ValueError, match="real Git commit"):
        AppConfig(**{**_base_config(), "APP_REVISION": "0" * 40})
