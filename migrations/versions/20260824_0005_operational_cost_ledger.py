"""Add period-bounded operational cost ledger.

Revision ID: 20260824_0005
Revises: 20260824_0004
"""

import sqlalchemy as sa
from alembic import op

revision = "20260824_0005"
down_revision = "20260824_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "operational_costs",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("account_id", sa.String(length=36), nullable=False),
        sa.Column("category", sa.String(length=64), nullable=False),
        sa.Column("amount_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("incurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_reference_sha256", sa.String(length=64), nullable=False, unique=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["account_id"], ["paper_accounts.id"]),
    )
    op.create_index(
        "ix_operational_costs_account_time",
        "operational_costs",
        ["account_id", "incurred_at"],
    )
    if op.get_bind().dialect.name == "postgresql":
        op.execute(
            """
            CREATE FUNCTION reject_operational_cost_mutation()
            RETURNS trigger AS $$
            BEGIN
                RAISE EXCEPTION 'operational_costs is append-only';
            END;
            $$ LANGUAGE plpgsql
            """
        )
        op.execute(
            """
            CREATE TRIGGER trg_operational_costs_no_mutation
            BEFORE UPDATE OR DELETE ON operational_costs
            FOR EACH ROW EXECUTE FUNCTION reject_operational_cost_mutation()
            """
        )
        op.execute(
            """
            CREATE FUNCTION reject_strategy_version_mutation()
            RETURNS trigger AS $$
            BEGIN
                RAISE EXCEPTION 'strategy_versions is append-only';
            END;
            $$ LANGUAGE plpgsql
            """
        )
        op.execute(
            """
            CREATE TRIGGER trg_strategy_versions_no_mutation
            BEFORE UPDATE OR DELETE ON strategy_versions
            FOR EACH ROW EXECUTE FUNCTION reject_strategy_version_mutation()
            """
        )
    else:
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


def downgrade() -> None:
    if op.get_bind().dialect.name == "postgresql":
        op.execute("DROP TRIGGER trg_strategy_versions_no_mutation ON strategy_versions")
        op.execute("DROP FUNCTION reject_strategy_version_mutation()")
        op.execute("DROP TRIGGER trg_operational_costs_no_mutation ON operational_costs")
        op.execute("DROP FUNCTION reject_operational_cost_mutation()")
    else:
        op.execute("DROP TRIGGER trg_strategy_versions_no_update")
        op.execute("DROP TRIGGER trg_strategy_versions_no_delete")
        op.execute("DROP TRIGGER trg_operational_costs_no_update")
        op.execute("DROP TRIGGER trg_operational_costs_no_delete")
    op.drop_index("ix_operational_costs_account_time", table_name="operational_costs")
    op.drop_table("operational_costs")
