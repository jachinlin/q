"""Add durable task queue runtime ownership and idempotency.

Revision ID: 0005_task_queue_runtime
Revises: 0004_experiments_tasks
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005_task_queue_runtime"
down_revision: str | None = "0004_experiments_tasks"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

ACTIVE_TASK_STATUSES = ("QUEUED", "RUNNING", "CANCEL_REQUESTED")
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


def _backup_task_dependents() -> None:
    """Detach task children so SQLite can safely rebuild their parent table."""
    op.execute(
        "CREATE TABLE _task_attempt_0005_backup AS SELECT * FROM task_attempt"
    )
    op.execute("CREATE TABLE _audit_event_0005_backup AS SELECT * FROM audit_event")
    op.drop_index("ix_task_attempt_task", table_name="task_attempt")
    op.drop_table("task_attempt")
    op.drop_index("ix_audit_event_task_created", table_name="audit_event")
    op.drop_index("ix_audit_event_experiment_created", table_name="audit_event")
    op.drop_table("audit_event")


def _restore_task_dependents() -> None:
    """Recreate the exact 0004 child schemas and restore every history row."""
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
    op.execute(
        "INSERT INTO task_attempt ("
        "id, task_id, attempt_no, status, worker_id, started_at, heartbeat_at, "
        "completed_at, log_path, progress_json, error_json"
        ") SELECT "
        "id, task_id, attempt_no, status, worker_id, started_at, heartbeat_at, "
        "completed_at, log_path, progress_json, error_json "
        "FROM _task_attempt_0005_backup"
    )
    op.create_index("ix_task_attempt_task", "task_attempt", ["task_id"])
    op.drop_table("_task_attempt_0005_backup")

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
    op.execute(
        "INSERT INTO audit_event ("
        "id, experiment_id, task_id, event_type, actor, details_json, created_at"
        ") SELECT "
        "id, experiment_id, task_id, event_type, actor, details_json, created_at "
        "FROM _audit_event_0005_backup"
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
    op.drop_table("_audit_event_0005_backup")


def _create_head_indexes() -> None:
    op.execute(
        "CREATE INDEX ix_task_queue ON task "
        "(status, available_at, priority DESC, created_at, id)"
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_task_active_idempotency ON task "
        "(task_type, COALESCE(experiment_id, ''), idempotency_key) "
        "WHERE idempotency_key IS NOT NULL "
        "AND status IN ('QUEUED', 'RUNNING', 'CANCEL_REQUESTED')"
    )


def upgrade() -> None:
    _backup_task_dependents()
    op.drop_index("ix_task_queue", table_name="task")
    with op.batch_alter_table("task", recreate="always") as batch_op:
        batch_op.alter_column(
            "experiment_id",
            existing_type=sa.String(36),
            existing_nullable=False,
            nullable=True,
        )
        batch_op.add_column(sa.Column("idempotency_key", sa.String(128)))
        batch_op.add_column(sa.Column("worker_id", sa.String(128)))
        batch_op.add_column(sa.Column("locked_at", sa.String(32)))
        batch_op.add_column(sa.Column("error_json", sa.Text()))
    _create_head_indexes()
    _restore_task_dependents()


def downgrade() -> None:
    standalone_count = op.get_bind().scalar(
        sa.text("SELECT count(*) FROM task WHERE experiment_id IS NULL")
    )
    if standalone_count:
        raise RuntimeError(
            "cannot downgrade 0005 while standalone task rows exist: "
            "0004 requires task.experiment_id; remove or attach those tasks first"
        )
    _backup_task_dependents()
    op.drop_index("uq_task_active_idempotency", table_name="task")
    op.drop_index("ix_task_queue", table_name="task")
    with op.batch_alter_table("task", recreate="always") as batch_op:
        batch_op.drop_column("error_json")
        batch_op.drop_column("locked_at")
        batch_op.drop_column("worker_id")
        batch_op.drop_column("idempotency_key")
        batch_op.alter_column(
            "experiment_id",
            existing_type=sa.String(36),
            existing_nullable=True,
            nullable=False,
        )
    op.create_index(
        "ix_task_queue",
        "task",
        ["status", "available_at", "priority", "created_at"],
    )
    _restore_task_dependents()
