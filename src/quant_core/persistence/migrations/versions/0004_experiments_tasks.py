"""Add durable experiment registry and task queue storage.

Revision ID: 0004_experiments_tasks
Revises: 0003_pipeline_stage_leases
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004_experiments_tasks"
down_revision: str | None = "0003_pipeline_stage_leases"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

EXPERIMENT_STATUSES = (
    "CREATED",
    "QUEUED",
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    "CANCELLED",
)
RESEARCH_MARKS = ("UNREVIEWED", "BASELINE", "CANDIDATE", "DISCARDED")
TASK_STATUSES = (
    "QUEUED",
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    "CANCEL_REQUESTED",
    "CANCELLED",
    "ORPHANED",
)


def _allowed(column: str, values: tuple[str, ...]) -> str:
    rendered = ", ".join(f"'{value}'" for value in values)
    return f"{column} IN ({rendered})"


def upgrade() -> None:
    op.create_table(
        "experiment",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("strategy_id", sa.String(128), nullable=False),
        sa.Column("strategy_version", sa.String(64), nullable=False),
        sa.Column("config_json", sa.String(), nullable=False),
        sa.Column("config_hash", sa.String(64), nullable=False),
        sa.Column(
            "snapshot_id",
            sa.String(36),
            sa.ForeignKey("snapshot.id", ondelete="RESTRICT"),
        ),
        sa.Column("snapshot_manifest_hash", sa.String(64), nullable=False),
        sa.Column("source_tree_hash", sa.String(64)),
        sa.Column("git_commit_hash", sa.String(64)),
        sa.Column("lockfile_hash", sa.String(64), nullable=False),
        sa.Column("rulebook_version", sa.String(128), nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column(
            "research_mark",
            sa.String(16),
            nullable=False,
            server_default="UNREVIEWED",
        ),
        sa.Column("created_at", sa.String(32), nullable=False),
        sa.Column("queued_at", sa.String(32)),
        sa.Column("started_at", sa.String(32)),
        sa.Column("completed_at", sa.String(32)),
        sa.CheckConstraint(
            _allowed("status", EXPERIMENT_STATUSES),
            name="ck_experiment_status",
        ),
        sa.CheckConstraint(
            _allowed("research_mark", RESEARCH_MARKS),
            name="ck_experiment_research_mark",
        ),
        sa.CheckConstraint(
            "source_tree_hash IS NOT NULL OR git_commit_hash IS NOT NULL",
            name="ck_experiment_source_identity",
        ),
    )
    op.create_index("ix_experiment_fingerprint", "experiment", ["fingerprint"])
    op.create_index(
        "ix_experiment_strategy_created",
        "experiment",
        ["strategy_id", "created_at"],
    )
    op.create_table(
        "experiment_tag",
        sa.Column(
            "experiment_id",
            sa.String(36),
            sa.ForeignKey("experiment.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("tag", sa.String(64), primary_key=True),
    )
    op.create_table(
        "experiment_metric",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "experiment_id",
            sa.String(36),
            sa.ForeignKey("experiment.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("value", sa.Float(), nullable=False),
        sa.Column("unit", sa.String(32)),
        sa.Column("created_at", sa.String(32), nullable=False),
        sa.UniqueConstraint(
            "experiment_id", "name", name="uq_experiment_metric_name"
        ),
    )
    op.create_table(
        "experiment_artifact",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "experiment_id",
            sa.String(36),
            sa.ForeignKey("experiment.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("artifact_type", sa.String(64), nullable=False),
        sa.Column("path", sa.String(), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("metadata_json", sa.String(), nullable=False),
        sa.Column("created_at", sa.String(32), nullable=False),
        sa.UniqueConstraint(
            "experiment_id", "name", name="uq_experiment_artifact_name"
        ),
    )
    op.create_table(
        "task",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "experiment_id",
            sa.String(36),
            sa.ForeignKey("experiment.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("task_type", sa.String(64), nullable=False),
        sa.Column("payload_json", sa.String(), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("progress_json", sa.String(), nullable=False),
        sa.Column("created_at", sa.String(32), nullable=False),
        sa.Column("available_at", sa.String(32), nullable=False),
        sa.Column("updated_at", sa.String(32), nullable=False),
        sa.Column("heartbeat_at", sa.String(32)),
        sa.Column("completed_at", sa.String(32)),
        sa.CheckConstraint(_allowed("status", TASK_STATUSES), name="ck_task_status"),
    )
    op.create_index(
        "ix_task_queue",
        "task",
        ["status", "available_at", "priority", "created_at"],
    )
    op.create_index("ix_task_experiment", "task", ["experiment_id"])
    op.create_table(
        "task_attempt",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "task_id",
            sa.String(36),
            sa.ForeignKey("task.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("attempt_no", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("worker_id", sa.String(128)),
        sa.Column("started_at", sa.String(32), nullable=False),
        sa.Column("heartbeat_at", sa.String(32)),
        sa.Column("completed_at", sa.String(32)),
        sa.Column("log_path", sa.String()),
        sa.Column("progress_json", sa.String(), nullable=False),
        sa.Column("error_json", sa.String()),
        sa.CheckConstraint(
            _allowed("status", TASK_STATUSES), name="ck_task_attempt_status"
        ),
        sa.CheckConstraint("attempt_no > 0", name="ck_task_attempt_positive"),
        sa.UniqueConstraint("task_id", "attempt_no", name="uq_task_attempt_no"),
    )
    op.create_index("ix_task_attempt_task", "task_attempt", ["task_id"])
    op.create_table(
        "audit_event",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "experiment_id",
            sa.String(36),
            sa.ForeignKey("experiment.id", ondelete="SET NULL"),
        ),
        sa.Column(
            "task_id",
            sa.String(36),
            sa.ForeignKey("task.id", ondelete="SET NULL"),
        ),
        sa.Column("event_type", sa.String(128), nullable=False),
        sa.Column("actor", sa.String(128)),
        sa.Column("details_json", sa.String(), nullable=False),
        sa.Column("created_at", sa.String(32), nullable=False),
    )
    op.create_index(
        "ix_audit_event_experiment_created",
        "audit_event",
        ["experiment_id", "created_at"],
    )
    op.create_index(
        "ix_audit_event_task_created",
        "audit_event",
        ["task_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_audit_event_task_created", table_name="audit_event")
    op.drop_index("ix_audit_event_experiment_created", table_name="audit_event")
    op.drop_table("audit_event")
    op.drop_index("ix_task_attempt_task", table_name="task_attempt")
    op.drop_table("task_attempt")
    op.drop_index("ix_task_experiment", table_name="task")
    op.drop_index("ix_task_queue", table_name="task")
    op.drop_table("task")
    op.drop_table("experiment_artifact")
    op.drop_table("experiment_metric")
    op.drop_table("experiment_tag")
    op.drop_index("ix_experiment_strategy_created", table_name="experiment")
    op.drop_index("ix_experiment_fingerprint", table_name="experiment")
    op.drop_table("experiment")
