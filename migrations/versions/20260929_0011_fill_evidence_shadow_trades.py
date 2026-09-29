"""Fill evidence, shadow trades and a retention index for API calls.

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
    tables = set(inspector.get_table_names())
    money = sa.Numeric(38, 18)
    if "shadow_positions" not in tables:
        op.create_table(
            "shadow_positions",
            sa.Column("id", sa.String(length=36), primary_key=True),
            sa.Column(
                "candidate_id",
                sa.String(length=36),
                sa.ForeignKey("candidates.id"),
                nullable=False,
            ),
            sa.Column("mint", sa.String(length=64), nullable=False),
            sa.Column("pool_address", sa.String(length=64), nullable=False),
            sa.Column(
                "strategy_version_id",
                sa.String(length=36),
                sa.ForeignKey("strategy_versions.id"),
                nullable=False,
            ),
            sa.Column("config_hash", sa.String(length=64), nullable=False),
            sa.Column("block_reason", sa.String(length=64), nullable=False),
            sa.Column("status", sa.String(length=16), nullable=False),
            sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("notional_usd", money, nullable=False),
            sa.Column("entry_fee_usd", money, nullable=False),
            sa.Column("entry_cost_usd", money, nullable=False),
            sa.Column("entry_token_amount", money, nullable=False),
            sa.Column("remaining_token_amount", money, nullable=False),
            sa.Column("remaining_cost_usd", money, nullable=False),
            sa.Column("realized_pnl_usd", money, nullable=False),
            sa.Column("highest_executable_value", money, nullable=False),
            sa.Column("last_new_high_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("tp1_taken", sa.Boolean(), nullable=False),
            sa.Column("tp2_taken", sa.Boolean(), nullable=False),
            sa.Column("exit_reason", sa.String(length=64), nullable=True),
            sa.Column("adverse_fill_bps", sa.Integer(), nullable=False),
            sa.Column("evidence_json", sa.JSON(), nullable=True),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        )
        op.create_index(
            "ix_shadow_positions_candidate_id", "shadow_positions", ["candidate_id"]
        )
        op.create_index(
            "ix_shadow_positions_status_opened", "shadow_positions", ["status", "opened_at"]
        )
    if "shadow_fills" not in tables:
        op.create_table(
            "shadow_fills",
            sa.Column("id", sa.String(length=36), primary_key=True),
            sa.Column(
                "position_id",
                sa.String(length=36),
                sa.ForeignKey("shadow_positions.id"),
                nullable=False,
            ),
            sa.Column("token_amount", money, nullable=False),
            sa.Column("gross_usd", money, nullable=False),
            sa.Column("network_fee_usd", money, nullable=False),
            sa.Column("realized_pnl_usd", money, nullable=False),
            sa.Column("exit_reason", sa.String(length=64), nullable=False),
            sa.Column("filled_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("evidence_json", sa.JSON(), nullable=True),
        )
        op.create_index("ix_shadow_fills_position_id", "shadow_fills", ["position_id"])


def downgrade() -> None:
    op.drop_index("ix_shadow_fills_position_id", table_name="shadow_fills")
    op.drop_table("shadow_fills")
    op.drop_index("ix_shadow_positions_status_opened", table_name="shadow_positions")
    op.drop_index("ix_shadow_positions_candidate_id", table_name="shadow_positions")
    op.drop_table("shadow_positions")
    op.drop_index("ix_external_api_calls_requested_at", table_name="external_api_calls")
    op.drop_column("paper_fills", "evidence_json")
