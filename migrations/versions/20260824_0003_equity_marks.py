"""Durable paper-equity history for exact calendar-day reports.

Revision ID: 20260824_0003
Revises: 20260824_0002
"""

import sqlalchemy as sa
from alembic import op

revision = "20260824_0003"
down_revision = "20260824_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if "paper_equity_marks" in inspector.get_table_names():
        return
    op.create_table(
        "paper_equity_marks",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column(
            "account_id",
            sa.String(length=36),
            sa.ForeignKey("paper_accounts.id"),
            nullable=False,
        ),
        sa.Column("equity", sa.Numeric(38, 18), nullable=False),
        sa.Column("realized_pnl", sa.Numeric(38, 18), nullable=False),
        sa.Column("unrealized_pnl", sa.Numeric(38, 18), nullable=False),
        sa.Column("locked_capital", sa.Numeric(38, 18), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_paper_equity_marks_account_time",
        "paper_equity_marks",
        ["account_id", "observed_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_paper_equity_marks_account_time", table_name="paper_equity_marks"
    )
    op.drop_table("paper_equity_marks")
