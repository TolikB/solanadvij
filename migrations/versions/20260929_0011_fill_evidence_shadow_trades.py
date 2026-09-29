"""Fill evidence for offline re-pricing and a retention index for API calls.

Revision ID: 20260929_0011
Revises: 20260901_0010
"""

import sqlalchemy as sa
from alembic import op

revision = "20260929_0011"
down_revision = "20260901_0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    fill_columns = {column["name"] for column in inspector.get_columns("paper_fills")}
    if "evidence_json" not in fill_columns:
        op.add_column("paper_fills", sa.Column("evidence_json", sa.JSON(), nullable=True))
    call_indexes = {index["name"] for index in inspector.get_indexes("external_api_calls")}
    if "ix_external_api_calls_requested_at" not in call_indexes:
        op.create_index(
            "ix_external_api_calls_requested_at",
            "external_api_calls",
            ["requested_at"],
            unique=False,
        )


def downgrade() -> None:
    op.drop_index("ix_external_api_calls_requested_at", table_name="external_api_calls")
    op.drop_column("paper_fills", "evidence_json")
