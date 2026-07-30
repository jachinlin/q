"""Create immutable data catalog and snapshot tables.

Revision ID: 0001_data_catalog
Revises:
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001_data_catalog"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "dataset_version",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("dataset", sa.String(64), nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("source", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("start_date", sa.String(10)),
        sa.Column("end_date", sa.String(10)),
        sa.Column("created_run_id", sa.String(128), nullable=False),
        sa.Column("created_at", sa.String(32), nullable=False),
        sa.UniqueConstraint("dataset", "fingerprint"),
    )
    op.create_table(
        "dataset_partition",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "dataset_version_id",
            sa.String(36),
            sa.ForeignKey("dataset_version.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("path", sa.String(), nullable=False),
        sa.Column("schema_fingerprint", sa.String(64), nullable=False),
        sa.Column("row_count", sa.Integer(), nullable=False),
        sa.UniqueConstraint("dataset_version_id", "ordinal"),
    )
    op.create_table(
        "quality_run",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("started_at", sa.String(32), nullable=False),
        sa.Column("completed_at", sa.String(32)),
        sa.Column("created_at", sa.String(32), nullable=False),
    )
    op.create_table(
        "quality_run_dataset",
        sa.Column(
            "quality_run_id",
            sa.String(36),
            sa.ForeignKey("quality_run.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("dataset", sa.String(64), primary_key=True),
        sa.Column(
            "dataset_version_id",
            sa.String(36),
            sa.ForeignKey("dataset_version.id", ondelete="RESTRICT"),
            nullable=False,
        ),
    )
    op.create_table(
        "quality_issue",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "quality_run_id",
            sa.String(36),
            sa.ForeignKey("quality_run.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("rule_id", sa.String(128), nullable=False),
        sa.Column("severity", sa.String(16), nullable=False),
        sa.Column("dataset", sa.String(64), nullable=False),
        sa.Column("scope_json", sa.String(), nullable=False),
        sa.Column("actual_json", sa.String(), nullable=False),
        sa.Column("threshold_json", sa.String(), nullable=False),
        sa.Column("message", sa.String(), nullable=False),
        sa.Column("remediation", sa.String(), nullable=False),
    )
    op.create_table(
        "snapshot",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "publication_fingerprint", sa.String(64), nullable=False, unique=True
        ),
        sa.Column("as_of", sa.String(32), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("manifest_path", sa.String(), nullable=False),
        sa.Column("manifest_hash", sa.String(64), nullable=False),
        sa.Column(
            "quality_run_id",
            sa.String(36),
            sa.ForeignKey("quality_run.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("created_at", sa.String(32), nullable=False),
        sa.Column("published_at", sa.String(32)),
    )
    op.create_table(
        "snapshot_dataset",
        sa.Column(
            "snapshot_id",
            sa.String(36),
            sa.ForeignKey("snapshot.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("dataset", sa.String(64), primary_key=True),
        sa.Column(
            "dataset_version_id",
            sa.String(36),
            sa.ForeignKey("dataset_version.id", ondelete="RESTRICT"),
            nullable=False,
        ),
    )
    op.create_table(
        "audit_log",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("action", sa.String(128), nullable=False),
        sa.Column("object_type", sa.String(64), nullable=False),
        sa.Column("object_id", sa.String(128), nullable=False),
        sa.Column("details_json", sa.String(), nullable=False),
        sa.Column("created_at", sa.String(32), nullable=False),
    )
    op.execute(
        """
        CREATE TRIGGER dataset_version_no_update
        BEFORE UPDATE ON dataset_version
        WHEN OLD.status = 'PUBLISHED'
        BEGIN
          SELECT RAISE(ABORT, 'dataset versions are immutable');
        END
        """
    )
    op.execute(
        """
        CREATE TRIGGER dataset_version_no_delete
        BEFORE DELETE ON dataset_version
        WHEN OLD.status = 'PUBLISHED'
        BEGIN
          SELECT RAISE(ABORT, 'dataset versions are immutable');
        END
        """
    )
    op.execute(
        """
        CREATE TRIGGER dataset_partition_no_update
        BEFORE UPDATE ON dataset_partition
        WHEN OLD.dataset_version_id != NEW.dataset_version_id
        OR EXISTS (
          SELECT 1 FROM dataset_version
          WHERE id = OLD.dataset_version_id AND status = 'PUBLISHED'
        )
        OR EXISTS (
          SELECT 1 FROM dataset_version
          WHERE id = NEW.dataset_version_id AND status = 'PUBLISHED'
        )
        BEGIN
          SELECT RAISE(ABORT, 'dataset partitions are immutable');
        END
        """
    )
    op.execute(
        """
        CREATE TRIGGER dataset_partition_no_delete
        BEFORE DELETE ON dataset_partition
        WHEN EXISTS (
          SELECT 1 FROM dataset_version
          WHERE id = OLD.dataset_version_id AND status = 'PUBLISHED'
        )
        BEGIN
          SELECT RAISE(ABORT, 'dataset partitions are immutable');
        END
        """
    )
    op.execute(
        """
        CREATE TRIGGER dataset_partition_no_insert
        BEFORE INSERT ON dataset_partition
        WHEN EXISTS (
          SELECT 1 FROM dataset_version
          WHERE id = NEW.dataset_version_id AND status = 'PUBLISHED'
        )
        BEGIN
          SELECT RAISE(ABORT, 'dataset partitions are immutable');
        END
        """
    )
    op.execute(
        """
        CREATE TRIGGER quality_run_completed_no_update
        BEFORE UPDATE ON quality_run
        WHEN OLD.status = 'COMPLETED'
        BEGIN
          SELECT RAISE(ABORT, 'completed quality runs are immutable');
        END
        """
    )
    op.execute(
        """
        CREATE TRIGGER quality_run_completed_no_delete
        BEFORE DELETE ON quality_run
        WHEN OLD.status = 'COMPLETED'
        BEGIN
          SELECT RAISE(ABORT, 'completed quality runs are immutable');
        END
        """
    )
    for table in ("quality_run_dataset", "quality_issue"):
        op.execute(
            f"""
            CREATE TRIGGER {table}_completed_no_insert
            BEFORE INSERT ON {table}
            WHEN EXISTS (
              SELECT 1 FROM quality_run
              WHERE id = NEW.quality_run_id AND status = 'COMPLETED'
            )
            BEGIN
              SELECT RAISE(ABORT, 'completed quality runs are immutable');
            END
            """
        )
        op.execute(
            f"""
            CREATE TRIGGER {table}_completed_no_update
            BEFORE UPDATE ON {table}
            WHEN OLD.quality_run_id != NEW.quality_run_id
            OR EXISTS (
              SELECT 1 FROM quality_run
              WHERE id = OLD.quality_run_id AND status = 'COMPLETED'
            )
            OR EXISTS (
              SELECT 1 FROM quality_run
              WHERE id = NEW.quality_run_id AND status = 'COMPLETED'
            )
            BEGIN
              SELECT RAISE(ABORT, 'completed quality runs are immutable');
            END
            """
        )
        op.execute(
            f"""
            CREATE TRIGGER {table}_completed_no_delete
            BEFORE DELETE ON {table}
            WHEN EXISTS (
              SELECT 1 FROM quality_run
              WHERE id = OLD.quality_run_id AND status = 'COMPLETED'
            )
            BEGIN
              SELECT RAISE(ABORT, 'completed quality runs are immutable');
            END
            """
        )
    op.execute(
        """
        CREATE TRIGGER snapshot_published_no_update
        BEFORE UPDATE ON snapshot
        WHEN OLD.status = 'PUBLISHED'
        BEGIN
          SELECT RAISE(ABORT, 'published snapshots are immutable');
        END
        """
    )
    op.execute(
        """
        CREATE TRIGGER snapshot_published_no_delete
        BEFORE DELETE ON snapshot
        WHEN OLD.status = 'PUBLISHED'
        BEGIN
          SELECT RAISE(ABORT, 'published snapshots are immutable');
        END
        """
    )
    op.execute(
        """
        CREATE TRIGGER snapshot_dataset_published_no_insert
        BEFORE INSERT ON snapshot_dataset
        WHEN EXISTS (
          SELECT 1 FROM snapshot
          WHERE id = NEW.snapshot_id AND status = 'PUBLISHED'
        )
        BEGIN
          SELECT RAISE(ABORT, 'published snapshot datasets are immutable');
        END
        """
    )
    op.execute(
        """
        CREATE TRIGGER snapshot_dataset_published_no_update
        BEFORE UPDATE ON snapshot_dataset
        WHEN OLD.snapshot_id != NEW.snapshot_id
        OR EXISTS (
          SELECT 1 FROM snapshot
          WHERE id = OLD.snapshot_id AND status = 'PUBLISHED'
        )
        OR EXISTS (
          SELECT 1 FROM snapshot
          WHERE id = NEW.snapshot_id AND status = 'PUBLISHED'
        )
        BEGIN
          SELECT RAISE(ABORT, 'published snapshot datasets are immutable');
        END
        """
    )
    op.execute(
        """
        CREATE TRIGGER snapshot_dataset_published_no_delete
        BEFORE DELETE ON snapshot_dataset
        WHEN EXISTS (
          SELECT 1 FROM snapshot
          WHERE id = OLD.snapshot_id AND status = 'PUBLISHED'
        )
        BEGIN
          SELECT RAISE(ABORT, 'published snapshot datasets are immutable');
        END
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS snapshot_dataset_published_no_delete")
    op.execute("DROP TRIGGER IF EXISTS snapshot_dataset_published_no_update")
    op.execute("DROP TRIGGER IF EXISTS snapshot_dataset_published_no_insert")
    op.execute("DROP TRIGGER IF EXISTS snapshot_published_no_delete")
    op.execute("DROP TRIGGER IF EXISTS snapshot_published_no_update")
    for table in ("quality_issue", "quality_run_dataset"):
        op.execute(f"DROP TRIGGER IF EXISTS {table}_completed_no_delete")
        op.execute(f"DROP TRIGGER IF EXISTS {table}_completed_no_update")
        op.execute(f"DROP TRIGGER IF EXISTS {table}_completed_no_insert")
    op.execute("DROP TRIGGER IF EXISTS quality_run_completed_no_delete")
    op.execute("DROP TRIGGER IF EXISTS quality_run_completed_no_update")
    op.execute("DROP TRIGGER IF EXISTS dataset_partition_no_delete")
    op.execute("DROP TRIGGER IF EXISTS dataset_partition_no_update")
    op.execute("DROP TRIGGER IF EXISTS dataset_partition_no_insert")
    op.execute("DROP TRIGGER IF EXISTS dataset_version_no_delete")
    op.execute("DROP TRIGGER IF EXISTS dataset_version_no_update")
    for table in (
        "audit_log",
        "snapshot_dataset",
        "snapshot",
        "quality_issue",
        "quality_run_dataset",
        "quality_run",
        "dataset_partition",
        "dataset_version",
    ):
        op.drop_table(table)
