"""Add pipeline component identity and stage claim leases.

Revision ID: 0003_pipeline_stage_leases
Revises: 0002_pipeline_runs
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_pipeline_stage_leases"
down_revision: str | None = "0002_pipeline_runs"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("pipeline_run") as batch:
        batch.add_column(
            sa.Column(
                "pipeline_fingerprint",
                sa.String(64),
                nullable=False,
                server_default="pipeline-v1",
            )
        )
    with op.batch_alter_table("pipeline_stage") as batch:
        batch.add_column(sa.Column("owner_id", sa.String(64)))
        batch.add_column(sa.Column("lease_expires_at", sa.String(32)))
        batch.add_column(
            sa.Column("attempt", sa.Integer(), nullable=False, server_default="0")
        )


def downgrade() -> None:
    with op.batch_alter_table("pipeline_stage") as batch:
        batch.drop_column("attempt")
        batch.drop_column("lease_expires_at")
        batch.drop_column("owner_id")
    with op.batch_alter_table("pipeline_run") as batch:
        batch.drop_column("pipeline_fingerprint")
