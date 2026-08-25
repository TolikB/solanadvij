"""Persist the exact durable raw-event ingest order.

Revision ID: 20260824_0007
Revises: 20260824_0006
"""

import sqlalchemy as sa
from alembic import op

revision = "20260824_0007"
down_revision = "20260824_0006"
branch_labels = None
depends_on = None
RUNTIME_ADVISORY_LOCK_KEY = 0x534E49504552
SEQUENCE_NAME = "raw_chain_events_ingest_sequence_seq"
INDEX_NAME = "ix_raw_chain_events_ingest_sequence"


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
            raise RuntimeError("migration 0007 blocked: a bot process owns the runtime lease")
        return dialect
    active_runs = int(
        bind.scalar(sa.text("SELECT count(*) FROM system_runs WHERE stopped_at IS NULL")) or 0
    )
    if active_runs:
        raise RuntimeError("migration 0007 blocked: active system runs must be stopped first")
    return dialect


def upgrade() -> None:
    dialect = _assert_runtime_stopped()
    if dialect == "postgresql":
        op.execute(f"CREATE SEQUENCE {SEQUENCE_NAME}")
    op.add_column(
        "raw_chain_events",
        sa.Column("ingest_sequence", sa.BigInteger(), nullable=True),
    )
    bind = op.get_bind()
    if dialect == "postgresql":
        op.execute(
            """
            WITH ordered AS (
                SELECT id, block_date,
                       row_number() OVER (
                           ORDER BY created_at, slot, signature,
                                    instruction_index, inner_instruction_index, id
                       ) AS ingest_sequence
                FROM raw_chain_events
            )
            UPDATE raw_chain_events AS raw
            SET ingest_sequence = ordered.ingest_sequence
            FROM ordered
            WHERE raw.id = ordered.id AND raw.block_date = ordered.block_date
            """
        )
        op.execute(
            f"""
            SELECT setval(
                '{SEQUENCE_NAME}',
                COALESCE(max(ingest_sequence), 1),
                count(*) > 0
            )
            FROM raw_chain_events
            """
        )
    else:
        rows = bind.execute(
            sa.text(
                "SELECT id, block_date FROM raw_chain_events "
                "ORDER BY created_at, slot, signature, instruction_index, "
                "inner_instruction_index, id"
            )
        ).mappings()
        for ingest_sequence, row in enumerate(rows, start=1):
            bind.execute(
                sa.text(
                    "UPDATE raw_chain_events SET ingest_sequence = :ingest_sequence "
                    "WHERE id = :id AND block_date = :block_date"
                ),
                {
                    "ingest_sequence": ingest_sequence,
                    "id": row["id"],
                    "block_date": row["block_date"],
                },
            )
    if dialect == "sqlite":
        with op.batch_alter_table("raw_chain_events") as batch_op:
            batch_op.alter_column(
                "ingest_sequence",
                existing_type=sa.BigInteger(),
                nullable=False,
            )
    else:
        op.alter_column(
            "raw_chain_events",
            "ingest_sequence",
            existing_type=sa.BigInteger(),
            nullable=False,
        )
    op.create_index(INDEX_NAME, "raw_chain_events", ["ingest_sequence"], unique=False)


def downgrade() -> None:
    dialect = _assert_runtime_stopped()
    op.drop_index(INDEX_NAME, table_name="raw_chain_events")
    if dialect == "sqlite":
        with op.batch_alter_table("raw_chain_events") as batch_op:
            batch_op.drop_column("ingest_sequence")
    else:
        op.drop_column("raw_chain_events", "ingest_sequence")
    if dialect == "postgresql":
        op.execute(f"DROP SEQUENCE {SEQUENCE_NAME}")