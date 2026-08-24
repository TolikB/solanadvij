"""Enforce acceptance invariants on databases already stamped at revision 0005.

Revision ID: 20260824_0006
Revises: 20260824_0005
"""

import sqlalchemy as sa
from alembic import op

revision = "20260824_0006"
down_revision = "20260824_0005"
branch_labels = None
depends_on = None
RUNTIME_ADVISORY_LOCK_KEY = 0x534E49504552

SHA256_CHECK = """
length(source_reference_sha256) = 64
AND source_reference_sha256 = lower(source_reference_sha256)
AND replace(replace(replace(replace(replace(replace(replace(replace(
    replace(replace(replace(replace(replace(replace(replace(replace(
        source_reference_sha256, '0', ''), '1', ''), '2', ''), '3', ''),
        '4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''),
        'a', ''), 'b', ''), 'c', ''), 'd', ''), 'e', ''), 'f', '') = ''
"""
SQLITE_SHA256_CHECK = SHA256_CHECK.replace(
    "source_reference_sha256", "NEW.source_reference_sha256"
)


def upgrade() -> None:
    bind = op.get_bind()
    dialect = bind.dialect.name
    if dialect == "postgresql":
        lease_acquired = bool(
            bind.scalar(
                sa.text(
                    f"SELECT pg_try_advisory_xact_lock({RUNTIME_ADVISORY_LOCK_KEY})"
                )
            )
        )
        if not lease_acquired:
            raise RuntimeError(
                "migration 0006 blocked: a bot process still owns the PostgreSQL runtime lease"
            )
    _upgrade_with_runtime_fence(bind, dialect)


def _upgrade_with_runtime_fence(bind: sa.engine.Connection, dialect: str) -> None:
    invalid_hash_count = int(
        bind.scalar(
            sa.text(
                f"SELECT count(*) FROM operational_costs WHERE NOT ({SHA256_CHECK})"
            )
        )
        or 0
    )
    if invalid_hash_count:
        raise RuntimeError(
            "migration 0006 blocked: invalid legacy operational-cost source hashes exist; "
            "audit and quarantine those rows with an administrator before retrying"
        )
    if dialect == "postgresql":
        recent_active_count = int(
            bind.scalar(
                sa.text(
                    "SELECT count(*) FROM system_runs WHERE stopped_at IS NULL "
                    "AND last_heartbeat_at >= CURRENT_TIMESTAMP - INTERVAL '2 minutes'"
                )
            )
            or 0
        )
    else:
        recent_active_count = int(
            bind.scalar(
                sa.text(
                    "SELECT count(*) FROM system_runs WHERE stopped_at IS NULL "
                    "AND datetime(last_heartbeat_at) >= datetime('now', '-2 minutes')"
                )
            )
            or 0
        )
    if recent_active_count:
        raise RuntimeError(
            "migration 0006 blocked: stop every bot process and retry after active heartbeats expire"
        )
    bind.execute(
        sa.text(
            "UPDATE system_runs SET stopped_at = CURRENT_TIMESTAMP, "
            "last_heartbeat_at = CURRENT_TIMESTAMP, "
            "stop_reason = 'migration_0006_recovered_legacy_runtime' "
            "WHERE stopped_at IS NULL"
        )
    )
    if dialect == "postgresql":
        op.execute(
            f"""
            ALTER TABLE operational_costs
            ADD CONSTRAINT ck_operational_cost_source_sha256
            CHECK ({SHA256_CHECK}) NOT VALID
            """
        )
        op.execute(
            "ALTER TABLE operational_costs VALIDATE CONSTRAINT ck_operational_cost_source_sha256"
        )
        op.execute(
            """
            CREATE OR REPLACE FUNCTION reject_operational_cost_mutation()
            RETURNS trigger AS $$
            BEGIN
                RAISE EXCEPTION 'operational_costs is append-only';
            END;
            $$ LANGUAGE plpgsql
            """
        )
        op.execute("DROP TRIGGER IF EXISTS trg_operational_costs_no_mutation ON operational_costs")
        op.execute(
            """
            CREATE TRIGGER trg_operational_costs_no_mutation
            BEFORE UPDATE OR DELETE ON operational_costs
            FOR EACH ROW EXECUTE FUNCTION reject_operational_cost_mutation()
            """
        )
        op.execute(
            """
            CREATE OR REPLACE FUNCTION reject_strategy_version_mutation()
            RETURNS trigger AS $$
            BEGIN
                RAISE EXCEPTION 'strategy_versions is append-only';
            END;
            $$ LANGUAGE plpgsql
            """
        )
        op.execute("DROP TRIGGER IF EXISTS trg_strategy_versions_no_mutation ON strategy_versions")
        op.execute(
            """
            CREATE TRIGGER trg_strategy_versions_no_mutation
            BEFORE UPDATE OR DELETE ON strategy_versions
            FOR EACH ROW EXECUTE FUNCTION reject_strategy_version_mutation()
            """
        )
    else:
        op.execute("DROP TRIGGER IF EXISTS trg_operational_costs_no_update")
        op.execute("DROP TRIGGER IF EXISTS trg_operational_costs_no_delete")
        op.execute(
            """
            CREATE TRIGGER trg_operational_costs_no_update
            BEFORE UPDATE ON operational_costs
            BEGIN
                SELECT RAISE(ABORT, 'operational_costs is append-only');
            END
            """
        )
        op.execute(
            """
            CREATE TRIGGER trg_operational_costs_no_delete
            BEFORE DELETE ON operational_costs
            BEGIN
                SELECT RAISE(ABORT, 'operational_costs is append-only');
            END
            """
        )
        op.execute("DROP TRIGGER IF EXISTS trg_strategy_versions_no_update")
        op.execute("DROP TRIGGER IF EXISTS trg_strategy_versions_no_delete")
        op.execute(
            """
            CREATE TRIGGER trg_strategy_versions_no_update
            BEFORE UPDATE ON strategy_versions
            BEGIN
                SELECT RAISE(ABORT, 'strategy_versions is append-only');
            END
            """
        )
        op.execute(
            """
            CREATE TRIGGER trg_strategy_versions_no_delete
            BEFORE DELETE ON strategy_versions
            BEGIN
                SELECT RAISE(ABORT, 'strategy_versions is append-only');
            END
            """
        )
        op.execute(
            f"""
            CREATE TRIGGER trg_operational_costs_valid_source
            BEFORE INSERT ON operational_costs
            WHEN NOT ({SQLITE_SHA256_CHECK})
            BEGIN
                SELECT RAISE(ABORT, 'operational cost source must be lowercase SHA-256');
            END
            """
        )
    expression = "((1))" if dialect == "postgresql" else "(1)"
    op.execute(
        "CREATE UNIQUE INDEX uq_system_runs_one_active "
        f"ON system_runs {expression} WHERE stopped_at IS NULL"
    )


def downgrade() -> None:
    bind = op.get_bind()
    dialect = bind.dialect.name
    if dialect == "postgresql" and not bool(
        bind.scalar(
            sa.text(
                f"SELECT pg_try_advisory_xact_lock({RUNTIME_ADVISORY_LOCK_KEY})"
            )
        )
    ):
        raise RuntimeError(
            "migration 0006 downgrade blocked: a bot process owns the PostgreSQL runtime lease"
        )
    active_run_count = int(
        bind.scalar(sa.text("SELECT count(*) FROM system_runs WHERE stopped_at IS NULL"))
        or 0
    )
    if active_run_count:
        raise RuntimeError(
            "migration 0006 downgrade blocked: active system runs must be stopped first"
        )
    op.execute("DROP INDEX uq_system_runs_one_active")
    if dialect == "postgresql":
        op.execute(
            "ALTER TABLE operational_costs DROP CONSTRAINT ck_operational_cost_source_sha256"
        )
    else:
        op.execute("DROP TRIGGER trg_operational_costs_valid_source")
