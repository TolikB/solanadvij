"""Durable event processing and candidate runtime recovery.

Revision ID: 20260824_0002
Revises: 20260824_0001
"""

import sqlalchemy as sa
from alembic import op

revision = "20260824_0002"
down_revision = "20260824_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    event_columns = {column["name"] for column in inspector.get_columns("event_dedup")}
    event_additions = {
        "processing_status": sa.Column(
            "processing_status", sa.String(length=16), nullable=False,
            server_default="PROCESSED",
        ),
        "processing_attempts": sa.Column(
            "processing_attempts", sa.Integer(), nullable=False, server_default="1"
        ),
        "last_attempt_at": sa.Column("last_attempt_at", sa.DateTime(timezone=True)),
        "processed_at": sa.Column("processed_at", sa.DateTime(timezone=True)),
        "last_error": sa.Column("last_error", sa.String(length=128)),
        "processing_token": sa.Column("processing_token", sa.String(length=36)),
    }
    for name, column in event_additions.items():
        if name not in event_columns:
            op.add_column("event_dedup", column)
    event_indexes = {index["name"] for index in inspector.get_indexes("event_dedup")}
    if "ix_event_dedup_processing_status" not in event_indexes:
        op.create_index(
            "ix_event_dedup_processing_status", "event_dedup",
            ["processing_status"], unique=False,
        )

    candidate_columns = {column["name"] for column in inspector.get_columns("candidates")}
    if "runtime_state_json" not in candidate_columns:
        op.add_column("candidates", sa.Column("runtime_state_json", sa.JSON()))

    account_columns = {column["name"] for column in inspector.get_columns("paper_accounts")}
    account_additions = {
        "halt_reason": sa.Column("halt_reason", sa.String(length=128)),
        "pause_until": sa.Column("pause_until", sa.DateTime(timezone=True)),
        "daily_halt_date": sa.Column("daily_halt_date", sa.String(length=10)),
    }
    for name, column in account_additions.items():
        if name not in account_columns:
            op.add_column("paper_accounts", column)

    outbox_columns = {column["name"] for column in inspector.get_columns("outbox_events")}
    if "delivery_state" not in outbox_columns:
        op.add_column(
            "outbox_events",
            sa.Column(
                "delivery_state", sa.String(length=16), nullable=False,
                server_default="PENDING",
            ),
        )
    if "claimed_at" not in outbox_columns:
        op.add_column("outbox_events", sa.Column("claimed_at", sa.DateTime(timezone=True)))
    outbox_indexes = {index["name"] for index in inspector.get_indexes("outbox_events")}
    if "ix_outbox_events_delivery_state" not in outbox_indexes:
        op.create_index(
            "ix_outbox_events_delivery_state", "outbox_events",
            ["delivery_state"], unique=False,
        )

    if "runtime_checkpoints" not in inspector.get_table_names():
        op.create_table(
            "runtime_checkpoints",
            sa.Column("checkpoint_key", sa.String(length=64), primary_key=True),
            sa.Column("state_json", sa.JSON(), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        )


def downgrade() -> None:
    op.drop_table("runtime_checkpoints")
    op.drop_index("ix_outbox_events_delivery_state", table_name="outbox_events")
    op.drop_column("outbox_events", "claimed_at")
    op.drop_column("outbox_events", "delivery_state")
    op.drop_column("paper_accounts", "daily_halt_date")
    op.drop_column("paper_accounts", "pause_until")
    op.drop_column("paper_accounts", "halt_reason")
    op.drop_column("candidates", "runtime_state_json")
    op.drop_index("ix_event_dedup_processing_status", table_name="event_dedup")
    op.drop_column("event_dedup", "processing_token")
    op.drop_column("event_dedup", "last_error")
    op.drop_column("event_dedup", "processed_at")
    op.drop_column("event_dedup", "last_attempt_at")
    op.drop_column("event_dedup", "processing_attempts")
    op.drop_column("event_dedup", "processing_status")
