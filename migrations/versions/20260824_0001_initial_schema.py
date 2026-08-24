"""Initial complete MVP schema.

Revision ID: 20260824_0001
Revises: None
"""

from alembic import op

from migrations.schema_0001 import Base

revision = "20260824_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    Base.metadata.create_all(bind=bind)
    if bind.dialect.name == "postgresql":
        op.execute(
            "CREATE TABLE IF NOT EXISTS raw_chain_events_default "
            "PARTITION OF raw_chain_events DEFAULT"
        )
        op.execute(
            "CREATE TABLE IF NOT EXISTS market_snapshots_default "
            "PARTITION OF market_snapshots DEFAULT"
        )


def downgrade() -> None:
    bind = op.get_bind()
    Base.metadata.drop_all(bind=bind)
