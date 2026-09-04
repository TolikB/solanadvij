"""Add independent stream, recovery-gap, and raw-archive durability state."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260901_0010"
down_revision = "20260831_0009"
branch_labels = None
depends_on = None


def _index_names(table: str) -> set[str]:
    return {
        item["name"]
        for item in sa.inspect(op.get_bind()).get_indexes(table)
    }


def upgrade() -> None:
    event_indexes = _index_names("event_dedup")
    if "ix_event_dedup_block_date" in event_indexes:
        op.drop_index(
            "ix_event_dedup_block_date",
            table_name="event_dedup",
        )

    raw_indexes = _index_names("raw_chain_events")
    for name in (
        "ix_raw_chain_events_mint_time",
        "ix_raw_chain_events_pool_time",
    ):
        if name in raw_indexes:
            op.drop_index(name, table_name="raw_chain_events")
    if "ix_raw_chain_events_mint_sequence" not in raw_indexes:
        op.create_index(
            "ix_raw_chain_events_mint_sequence",
            "raw_chain_events",
            ["mint", "ingest_sequence"],
            postgresql_where=sa.text("mint IS NOT NULL"),
        )
    if "ix_raw_chain_events_pool_sequence" not in raw_indexes:
        op.create_index(
            "ix_raw_chain_events_pool_sequence",
            "raw_chain_events",
            ["pool_address", "ingest_sequence"],
            postgresql_where=sa.text("pool_address IS NOT NULL"),
        )

    op.create_table(
        "stream_protocol_checkpoints",
        sa.Column("protocol", sa.String(32), primary_key=True),
        sa.Column(
            "durable_ingest_sequence",
            sa.BigInteger(),
            nullable=False,
            server_default="0",
        ),
        sa.Column("durable_signature", sa.String(128)),
        sa.Column(
            "durable_slot",
            sa.BigInteger(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "state_sequence",
            sa.BigInteger(),
            nullable=False,
            server_default="0",
        ),
        sa.Column("state_signature", sa.String(128)),
        sa.Column(
            "state_slot",
            sa.BigInteger(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
    )
    op.create_table(
        "stream_recovery_gaps",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("protocol", sa.String(32), nullable=False),
        sa.Column("checkpoint_signature", sa.String(128)),
        sa.Column("reason", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column(
            "attempts",
            sa.Integer(),
            nullable=False,
            server_default="1",
        ),
        sa.Column(
            "discovered_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "last_attempt_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "completed_at",
            sa.DateTime(timezone=True),
        ),
    )
    op.create_index(
        "ix_stream_recovery_gaps_status_time",
        "stream_recovery_gaps",
        ["status", "discovered_at"],
    )
    op.create_table(
        "raw_archive_segments",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("protocol", sa.String(32), nullable=False),
        sa.Column("start_sequence", sa.BigInteger(), nullable=False),
        sa.Column("end_sequence", sa.BigInteger(), nullable=False),
        sa.Column("event_count", sa.Integer(), nullable=False),
        sa.Column("path", sa.Text(), nullable=False, unique=True),
        sa.Column(
            "checksum_sha256",
            sa.String(64),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.CheckConstraint(
            "start_sequence > 0 "
            "AND end_sequence >= start_sequence",
            name="ck_raw_archive_segment_range",
        ),
        sa.CheckConstraint(
            "event_count > 0",
            name="ck_raw_archive_segment_count",
        ),
        sa.UniqueConstraint(
            "protocol",
            "start_sequence",
            "end_sequence",
            name="uq_raw_archive_segment_range",
        ),
    )
    op.create_index(
        "ix_raw_archive_segments_end_sequence",
        "raw_archive_segments",
        ["end_sequence"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_raw_archive_segments_end_sequence",
        table_name="raw_archive_segments",
    )
    op.drop_table("raw_archive_segments")
    op.drop_index(
        "ix_stream_recovery_gaps_status_time",
        table_name="stream_recovery_gaps",
    )
    op.drop_table("stream_recovery_gaps")
    op.drop_table("stream_protocol_checkpoints")
    op.drop_index(
        "ix_raw_chain_events_pool_sequence",
        table_name="raw_chain_events",
    )
    op.drop_index(
        "ix_raw_chain_events_mint_sequence",
        table_name="raw_chain_events",
    )
    op.create_index(
        "ix_raw_chain_events_mint_time",
        "raw_chain_events",
        ["mint", "block_time"],
    )
    op.create_index(
        "ix_raw_chain_events_pool_time",
        "raw_chain_events",
        ["pool_address", "block_time"],
    )
    op.create_index(
        "ix_event_dedup_block_date",
        "event_dedup",
        ["block_date"],
    )
