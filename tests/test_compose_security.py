import json
from pathlib import Path

import yaml


def _compose_services() -> dict[str, dict]:
    payload = yaml.safe_load(Path("docker-compose.yml").read_text(encoding="utf-8"))
    return payload["services"]


def test_compose_scopes_secrets_per_service() -> None:
    services = _compose_services()

    assert all("env_file" not in service for service in services.values())
    assert set(services["migrate"]["environment"]) == {
        "MIGRATION_POSTGRES_DSN",
        "SNIPER_DB_USER",
    }
    assert set(services["backup"]["environment"]) == {
        "MIGRATION_POSTGRES_DSN",
        "BACKUP_RETENTION_DAYS",
    }

    bot_environment = set(services["sniper-bot"]["environment"])
    assert {
        "MIGRATION_POSTGRES_DSN",
        "POSTGRES_ADMIN_PASSWORD",
        "POSTGRES_ADMIN_USER",
        "SNIPER_DB_PASSWORD",
        "SNIPER_DB_USER",
    }.isdisjoint(bot_environment)
    assert {
        "APP_MODE",
        "HELIUS_API_KEY",
        "JUPITER_API_KEY",
        "POSTGRES_DSN",
        "TELEGRAM_BOT_TOKEN",
    }.issubset(bot_environment)


def test_compose_requires_admin_dsn_for_migrations() -> None:
    services = _compose_services()

    migration_dsn = services["migrate"]["environment"]["MIGRATION_POSTGRES_DSN"]
    assert migration_dsn.startswith("${MIGRATION_POSTGRES_DSN:?")


def test_backup_command_is_one_complete_shell_script_argument() -> None:
    command = _compose_services()["backup"]["command"]

    assert isinstance(command, list)
    assert len(command) == 1
    assert "while true; do" in command[0]
    assert "/bin/sh /scripts/backup.sh" in command[0]
    assert "sleep 86400" in command[0]
    assert command[0].strip().endswith("done")


def test_example_telegram_allowlists_are_json_arrays() -> None:
    values = {}
    for line in Path(".env.example").read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            values[key] = value

    assert json.loads(values["TELEGRAM_ALLOWED_CHAT_IDS"]) == [123456]
    assert json.loads(values["TELEGRAM_ALLOWED_USER_IDS"]) == [123456]

def test_compose_forwards_optional_telegram_runtime_config() -> None:
    compose = Path("docker-compose.yml").read_text(encoding="utf-8")

    assert 'TELEGRAM: "${TELEGRAM:-}"' in compose
