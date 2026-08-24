"""Fence Telegram outbox leases against late worker completion.

Revision ID: 20260824_0004
Revises: 20260824_0003
"""

import sqlalchemy as sa
from alembic import op

revision = "20260824_0004"
down_revision = "20260824_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    columns = {column["name"] for column in inspector.get_columns("outbox_events")}
    if "claim_token" not in columns:
        op.add_column(
            "outbox_events", sa.Column("claim_token", sa.String(length=36))
        )


def downgrade() -> None:
    op.drop_column("outbox_events", "claim_token")
