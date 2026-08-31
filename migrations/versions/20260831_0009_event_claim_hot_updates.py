"""Keep durable claim completion on the PostgreSQL HOT-update path.

Revision ID: 20260831_0009
Revises: 20260831_0008
"""

import sqlalchemy as sa
from alembic import op

revision = "20260831_0009"
down_revision = "20260831_0008"
branch_labels = None
depends_on = None
RUNTIME_ADVISORY_LOCK_KEY = 0x534E49504552
STATUS_INDEX = "ix_event_dedup_processing_status"


def _assert_runtime_stopped() -> str:
    bind = op.get_bind()
    dialect = bind.dialect.name
    if dialect == "postgresql":
        owns_migration_lease = bool(
            bind.scalar(
                sa.text(
                    f"SELECT pg_try_advisory_xact_lock({RUNTIME_ADVISORY_LOCK_KEY})"
                )
            )
        )
        if not owns_migration_lease:
            raise RuntimeError(
                "migration 0009 blocked: a bot process owns the runtime lease"
            )
        return dialect
    active_runs = int(
        bind.scalar(
            sa.text("SELECT count(*) FROM system_runs WHERE stopped_at IS NULL")
        )
        or 0
    )
    if active_runs:
        raise RuntimeError(
            "migration 0009 blocked: active system runs must be stopped first"
        )
    return dialect


def upgrade() -> None:
    dialect = _assert_runtime_stopped()
    bind = op.get_bind()
    indexes = {
        str(index["name"])
        for index in sa.inspect(bind).get_indexes("event_dedup")
    }
    if STATUS_INDEX in indexes:
        op.drop_index(STATUS_INDEX, table_name="event_dedup")
    if dialect == "postgresql":
        op.execute(
            "ALTER TABLE public.event_dedup SET (fillfactor = 70)"
        )


def downgrade() -> None:
    dialect = _assert_runtime_stopped()
    if dialect == "postgresql":
        op.execute(
            "ALTER TABLE public.event_dedup RESET (fillfactor)"
        )
    bind = op.get_bind()
    indexes = {
        str(index["name"])
        for index in sa.inspect(bind).get_indexes("event_dedup")
    }
    if STATUS_INDEX not in indexes:
        op.create_index(
            STATUS_INDEX,
            "event_dedup",
            ["processing_status"],
            unique=False,
        )