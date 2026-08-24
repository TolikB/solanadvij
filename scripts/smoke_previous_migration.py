"""Prove that the frozen previous Alembic revision upgrades to head."""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path


def columns(database: Path, table: str) -> set[str]:
    connection = sqlite3.connect(database)
    try:
        return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}
    finally:
        connection.close()


def main() -> None:
    previous = os.environ.get("MIGRATION_POSTGRES_DSN")
    try:
        with tempfile.TemporaryDirectory(prefix="sniper-migration-") as directory:
            database = Path(directory) / "previous.db"
            migration_dsn = f"sqlite+aiosqlite:///{database.as_posix()}"
            os.environ["MIGRATION_POSTGRES_DSN"] = migration_dsn
            environment = os.environ.copy()
            subprocess.run(
                [sys.executable, "-m", "alembic", "upgrade", "20260824_0001"],
                env=environment,
                check=True,
            )
            assert "processing_status" not in columns(database, "event_dedup")
            assert "runtime_state_json" not in columns(database, "candidates")
            subprocess.run(
                [sys.executable, "-m", "alembic", "upgrade", "20260824_0005"],
                env=environment,
                check=True,
            )
            connection = sqlite3.connect(database)
            try:
                connection.execute("DROP TRIGGER trg_strategy_versions_no_update")
                connection.execute("DROP TRIGGER trg_strategy_versions_no_delete")
                connection.execute(
                    """
                    INSERT INTO strategy_versions
                        (id, version, config_hash, config_json, git_commit, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    ("strategy-legacy", "legacy", "b" * 64, "{}", "c" * 40, "2000-01-01T00:00:00Z"),
                )
                connection.execute(
                    """
                    INSERT INTO operational_costs
                        (id, account_id, category, amount_usd, incurred_at,
                         source_reference_sha256, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("legacy-invalid", "account-1", "vps", 1, "2000-01-01T00:00:00Z", "z" * 64, "2000-01-01T00:00:00Z"),
                )
                for run_id in ("legacy-run-1", "legacy-run-2"):
                    connection.execute(
                        """
                        INSERT INTO system_runs
                            (id, started_at, mode, strategy_version_id, hostname, app_version,
                             last_heartbeat_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (run_id, "2000-01-01T00:00:00Z", "paper", "strategy-legacy", "test", "test", "2000-01-01T00:00:00Z"),
                    )
                connection.commit()
            finally:
                connection.close()
            blocked = subprocess.run(
                [sys.executable, "-m", "alembic", "upgrade", "head"],
                env=environment,
                check=False,
                capture_output=True,
                text=True,
            )
            assert blocked.returncode != 0
            assert "invalid legacy operational-cost source hashes" in (
                blocked.stdout + blocked.stderr
            )
            connection = sqlite3.connect(database)
            try:
                connection.execute("DROP TRIGGER trg_operational_costs_no_delete")
                connection.execute("DELETE FROM operational_costs WHERE id = 'legacy-invalid'")
                connection.commit()
            finally:
                connection.close()
            subprocess.run(
                [sys.executable, "-m", "alembic", "upgrade", "head"],
                env=environment,
                check=True,
            )
            assert {
                "processing_status", "processing_attempts", "last_attempt_at",
                "processed_at", "last_error", "processing_token",
            }.issubset(columns(database, "event_dedup"))
            assert "runtime_state_json" in columns(database, "candidates")
            assert {"halt_reason", "pause_until", "daily_halt_date"}.issubset(
                columns(database, "paper_accounts")
            )
            assert {"delivery_state", "claimed_at", "claim_token"}.issubset(
                columns(database, "outbox_events")
            )
            connection = sqlite3.connect(database)
            try:
                tables = {
                    str(row[0])
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                triggers = {
                    str(row[0])
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='trigger'"
                    )
                }
                connection.execute(
                    """
                    INSERT INTO operational_costs
                        (id, account_id, category, amount_usd, incurred_at,
                         source_reference_sha256, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("cost-1", "account-1", "vps", 1, "2026-08-24T00:00:00Z", "a" * 64, "2026-08-24T00:00:00Z"),
                )
                active_legacy_runs = int(
                    connection.execute(
                        "SELECT count(*) FROM system_runs WHERE stopped_at IS NULL"
                    ).fetchone()[0]
                )
                assert active_legacy_runs == 0
                try:
                    connection.execute(
                        """
                        INSERT INTO operational_costs
                            (id, account_id, category, amount_usd, incurred_at,
                             source_reference_sha256, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        ("invalid", "account-1", "vps", 1, "2026-08-24T00:00:00Z", "z" * 64, "2026-08-24T00:00:00Z"),
                    )
                except sqlite3.IntegrityError as exc:
                    assert "SHA-256" in str(exc)
                else:
                    raise AssertionError("operational cost ledger accepted an invalid source hash")
                connection.execute(
                    """
                    INSERT INTO system_runs
                        (id, started_at, mode, strategy_version_id, hostname, app_version,
                         last_heartbeat_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("run-1", "2026-08-24T00:00:00Z", "paper", "strategy-legacy", "test", "test", "2026-08-24T00:00:00Z"),
                )
                try:
                    connection.execute(
                        """
                        INSERT INTO system_runs
                            (id, started_at, mode, strategy_version_id, hostname, app_version,
                             last_heartbeat_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        ("run-2", "2026-08-24T00:00:01Z", "paper", "strategy-legacy", "test", "test", "2026-08-24T00:00:01Z"),
                    )
                except sqlite3.IntegrityError:
                    pass
                else:
                    raise AssertionError("database accepted two active system runs")
                for statement in (
                    "UPDATE operational_costs SET amount_usd = 2 WHERE id = 'cost-1'",
                    "DELETE FROM operational_costs WHERE id = 'cost-1'",
                    "UPDATE strategy_versions SET git_commit = 'changed' WHERE id = 'strategy-legacy'",
                    "DELETE FROM strategy_versions WHERE id = 'strategy-legacy'",
                ):
                    try:
                        connection.execute(statement)
                    except sqlite3.IntegrityError as exc:
                        assert "append-only" in str(exc)
                    else:
                        raise AssertionError("operational cost ledger accepted a mutation")
            finally:
                connection.close()
            assert {"runtime_checkpoints", "paper_equity_marks", "operational_costs"}.issubset(tables)
            assert {
                "trg_operational_costs_no_update",
                "trg_operational_costs_no_delete",
                "trg_strategy_versions_no_update",
                "trg_strategy_versions_no_delete",
                "trg_operational_costs_valid_source",
            }.issubset(triggers)
            connection = sqlite3.connect(database)
            try:
                connection.execute(
                    """
                    INSERT INTO system_runs
                        (id, started_at, mode, strategy_version_id, hostname, app_version,
                         last_heartbeat_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("downgrade-active", "2000-01-01T00:00:00Z", "paper", "strategy-legacy", "test", "test", "2000-01-01T00:00:00Z"),
                )
                connection.commit()
            finally:
                connection.close()
            blocked_downgrade = subprocess.run(
                [sys.executable, "-m", "alembic", "downgrade", "20260824_0005"],
                env=environment,
                check=False,
                capture_output=True,
                text=True,
            )
            assert blocked_downgrade.returncode != 0
            assert "active system runs" in (
                blocked_downgrade.stdout + blocked_downgrade.stderr
            )
            connection = sqlite3.connect(database)
            try:
                connection.execute(
                    """
                    UPDATE system_runs
                    SET stopped_at = '2000-01-01T00:01:00Z'
                    WHERE id = 'downgrade-active'
                    """
                )
                connection.commit()
            finally:
                connection.close()
            subprocess.run(
                [sys.executable, "-m", "alembic", "downgrade", "20260824_0005"],
                env=environment,
                check=True,
            )
            connection = sqlite3.connect(database)
            try:
                downgraded_indexes = {
                    str(row[0])
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='index'"
                    )
                }
                downgraded_triggers = {
                    str(row[0])
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='trigger'"
                    )
                }
            finally:
                connection.close()
            assert "uq_system_runs_one_active" not in downgraded_indexes
            assert "trg_operational_costs_valid_source" not in downgraded_triggers
            assert {
                "trg_operational_costs_no_update",
                "trg_operational_costs_no_delete",
                "trg_strategy_versions_no_update",
                "trg_strategy_versions_no_delete",
            }.issubset(downgraded_triggers)
    finally:
        if previous is None:
            os.environ.pop("MIGRATION_POSTGRES_DSN", None)
        else:
            os.environ["MIGRATION_POSTGRES_DSN"] = previous


if __name__ == "__main__":
    main()
