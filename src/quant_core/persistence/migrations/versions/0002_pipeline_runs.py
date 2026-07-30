"""Add durable pipeline runs and stage checkpoints.

Revision ID: 0002_pipeline_runs
Revises: 0001_data_catalog
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_pipeline_runs"
down_revision: str | None = "0001_data_catalog"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "pipeline_run",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("mode", sa.String(16), nullable=False),
        sa.Column("provider", sa.String(64), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("requested_start", sa.String(10)),
        sa.Column("requested_end", sa.String(10)),
        sa.Column("resolved_start", sa.String(10), nullable=False),
        sa.Column("resolved_end", sa.String(10), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("created_at", sa.String(32), nullable=False),
        sa.Column("completed_at", sa.String(32)),
    )
    op.create_table(
        "pipeline_stage",
        sa.Column(
            "run_id",
            sa.String(36),
            sa.ForeignKey("pipeline_run.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("stage", sa.String(32), primary_key=True),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("input_hash", sa.String(64), nullable=False),
        sa.Column("output_hash", sa.String(64)),
        sa.Column("output_json", sa.String()),
        sa.Column("started_at", sa.String(32), nullable=False),
        sa.Column("completed_at", sa.String(32)),
        sa.Column("error_json", sa.String()),
    )
    op.execute(
        """
        CREATE TRIGGER pipeline_stage_succeeded_no_update
        BEFORE UPDATE ON pipeline_stage
        WHEN OLD.status = 'SUCCEEDED'
        BEGIN
          SELECT RAISE(ABORT, 'successful pipeline stages are immutable');
        END
        """
    )
    op.execute(
        """
        CREATE TRIGGER pipeline_stage_succeeded_no_delete
        BEFORE DELETE ON pipeline_stage
        WHEN OLD.status = 'SUCCEEDED'
        BEGIN
          SELECT RAISE(ABORT, 'successful pipeline stages are immutable');
        END
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS pipeline_stage_succeeded_no_delete")
    op.execute("DROP TRIGGER IF EXISTS pipeline_stage_succeeded_no_update")
    op.drop_table("pipeline_stage")
    op.drop_table("pipeline_run")
