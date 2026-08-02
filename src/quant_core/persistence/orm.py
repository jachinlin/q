"""Private SQLAlchemy mappings for metadata persistence."""

from __future__ import annotations

from sqlalchemy import (
    CheckConstraint,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base kept private to the persistence implementation."""


class DatasetVersionORM(Base):
    __tablename__ = "dataset_version"
    __table_args__ = (UniqueConstraint("dataset", "fingerprint"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    dataset: Mapped[str] = mapped_column(String(64), nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    start_date: Mapped[str | None] = mapped_column(String(10))
    end_date: Mapped[str | None] = mapped_column(String(10))
    created_run_id: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)


class DatasetPartitionORM(Base):
    __tablename__ = "dataset_partition"
    __table_args__ = (UniqueConstraint("dataset_version_id", "ordinal"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dataset_version_id: Mapped[str] = mapped_column(
        ForeignKey("dataset_version.id", ondelete="RESTRICT"), nullable=False
    )
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    path: Mapped[str] = mapped_column(String, nullable=False)
    schema_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    row_count: Mapped[int] = mapped_column(Integer, nullable=False)


class QualityRunORM(Base):
    __tablename__ = "quality_run"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    started_at: Mapped[str] = mapped_column(String(32), nullable=False)
    completed_at: Mapped[str | None] = mapped_column(String(32))
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)


class QualityRunDatasetORM(Base):
    __tablename__ = "quality_run_dataset"

    quality_run_id: Mapped[str] = mapped_column(
        ForeignKey("quality_run.id", ondelete="CASCADE"), primary_key=True
    )
    dataset: Mapped[str] = mapped_column(String(64), primary_key=True)
    dataset_version_id: Mapped[str] = mapped_column(
        ForeignKey("dataset_version.id", ondelete="RESTRICT"), nullable=False
    )


class QualityIssueORM(Base):
    __tablename__ = "quality_issue"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    quality_run_id: Mapped[str] = mapped_column(
        ForeignKey("quality_run.id", ondelete="CASCADE"), nullable=False
    )
    rule_id: Mapped[str] = mapped_column(String(128), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    dataset: Mapped[str] = mapped_column(String(64), nullable=False)
    scope_json: Mapped[str] = mapped_column(String, nullable=False)
    actual_json: Mapped[str] = mapped_column(String, nullable=False)
    threshold_json: Mapped[str] = mapped_column(String, nullable=False)
    message: Mapped[str] = mapped_column(String, nullable=False)
    remediation: Mapped[str] = mapped_column(String, nullable=False)


class SnapshotORM(Base):
    __tablename__ = "snapshot"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    publication_fingerprint: Mapped[str] = mapped_column(
        String(64), nullable=False, unique=True
    )
    as_of: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    manifest_path: Mapped[str] = mapped_column(String, nullable=False)
    manifest_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    quality_run_id: Mapped[str] = mapped_column(
        ForeignKey("quality_run.id", ondelete="RESTRICT"), nullable=False
    )
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)
    published_at: Mapped[str | None] = mapped_column(String(32))


class SnapshotDatasetORM(Base):
    __tablename__ = "snapshot_dataset"

    snapshot_id: Mapped[str] = mapped_column(
        ForeignKey("snapshot.id", ondelete="CASCADE"), primary_key=True
    )
    dataset: Mapped[str] = mapped_column(String(64), primary_key=True)
    dataset_version_id: Mapped[str] = mapped_column(
        ForeignKey("dataset_version.id", ondelete="RESTRICT"), nullable=False
    )


class AuditLogORM(Base):
    __tablename__ = "audit_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    action: Mapped[str] = mapped_column(String(128), nullable=False)
    object_type: Mapped[str] = mapped_column(String(64), nullable=False)
    object_id: Mapped[str] = mapped_column(String(128), nullable=False)
    details_json: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)


class PipelineRunORM(Base):
    __tablename__ = "pipeline_run"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    pipeline_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    requested_start: Mapped[str | None] = mapped_column(String(10))
    requested_end: Mapped[str | None] = mapped_column(String(10))
    resolved_start: Mapped[str] = mapped_column(String(10), nullable=False)
    resolved_end: Mapped[str] = mapped_column(String(10), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)
    completed_at: Mapped[str | None] = mapped_column(String(32))


class PipelineStageORM(Base):
    __tablename__ = "pipeline_stage"

    run_id: Mapped[str] = mapped_column(
        ForeignKey("pipeline_run.id", ondelete="CASCADE"), primary_key=True
    )
    stage: Mapped[str] = mapped_column(String(32), primary_key=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    input_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    output_hash: Mapped[str | None] = mapped_column(String(64))
    output_json: Mapped[str | None] = mapped_column(String)
    started_at: Mapped[str] = mapped_column(String(32), nullable=False)
    completed_at: Mapped[str | None] = mapped_column(String(32))
    error_json: Mapped[str | None] = mapped_column(String)
    owner_id: Mapped[str | None] = mapped_column(String(64))
    lease_expires_at: Mapped[str | None] = mapped_column(String(32))
    attempt: Mapped[int] = mapped_column(nullable=False, default=0)


class ExperimentORM(Base):
    __tablename__ = "experiment"
    __table_args__ = (
        CheckConstraint(
            "status IN ('CREATED', 'QUEUED', 'RUNNING', 'SUCCEEDED', "
            "'FAILED', 'CANCELLED')",
            name="ck_experiment_status",
        ),
        CheckConstraint(
            "research_mark IN ('UNREVIEWED', 'BASELINE', 'CANDIDATE', 'DISCARDED')",
            name="ck_experiment_research_mark",
        ),
        CheckConstraint(
            "source_tree_hash IS NOT NULL OR git_commit_hash IS NOT NULL",
            name="ck_experiment_source_identity",
        ),
        Index("ix_experiment_fingerprint", "fingerprint"),
        Index("ix_experiment_strategy_created", "strategy_id", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    strategy_id: Mapped[str] = mapped_column(String(128), nullable=False)
    strategy_version: Mapped[str] = mapped_column(String(64), nullable=False)
    config_json: Mapped[str] = mapped_column(String, nullable=False)
    config_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    snapshot_id: Mapped[str | None] = mapped_column(
        ForeignKey("snapshot.id", ondelete="RESTRICT")
    )
    snapshot_manifest_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    source_tree_hash: Mapped[str | None] = mapped_column(String(64))
    git_commit_hash: Mapped[str | None] = mapped_column(String(64))
    lockfile_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    rulebook_version: Mapped[str] = mapped_column(String(128), nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    research_mark: Mapped[str] = mapped_column(String(16), nullable=False)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)
    queued_at: Mapped[str | None] = mapped_column(String(32))
    started_at: Mapped[str | None] = mapped_column(String(32))
    completed_at: Mapped[str | None] = mapped_column(String(32))


class ExperimentTagORM(Base):
    __tablename__ = "experiment_tag"

    experiment_id: Mapped[str] = mapped_column(
        ForeignKey("experiment.id", ondelete="CASCADE"), primary_key=True
    )
    tag: Mapped[str] = mapped_column(String(64), primary_key=True)


class ExperimentMetricORM(Base):
    __tablename__ = "experiment_metric"
    __table_args__ = (
        UniqueConstraint(
            "experiment_id", "name", name="uq_experiment_metric_name"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    experiment_id: Mapped[str] = mapped_column(
        ForeignKey("experiment.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    unit: Mapped[str | None] = mapped_column(String(32))
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)


class ExperimentArtifactORM(Base):
    __tablename__ = "experiment_artifact"
    __table_args__ = (
        UniqueConstraint(
            "experiment_id", "name", name="uq_experiment_artifact_name"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    experiment_id: Mapped[str] = mapped_column(
        ForeignKey("experiment.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    artifact_type: Mapped[str] = mapped_column(String(64), nullable=False)
    path: Mapped[str] = mapped_column(String, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    metadata_json: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)


class TaskORM(Base):
    __tablename__ = "task"
    __table_args__ = (
        CheckConstraint(
            "status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', "
            "'CANCEL_REQUESTED', 'CANCELLED', 'ORPHANED')",
            name="ck_task_status",
        ),
        Index("ix_task_queue", "status", "available_at", "priority", "created_at"),
        Index("ix_task_experiment", "experiment_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    experiment_id: Mapped[str] = mapped_column(
        ForeignKey("experiment.id", ondelete="CASCADE"), nullable=False
    )
    task_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_json: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    progress_json: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)
    available_at: Mapped[str] = mapped_column(String(32), nullable=False)
    updated_at: Mapped[str] = mapped_column(String(32), nullable=False)
    heartbeat_at: Mapped[str | None] = mapped_column(String(32))
    completed_at: Mapped[str | None] = mapped_column(String(32))


class TaskAttemptORM(Base):
    __tablename__ = "task_attempt"
    __table_args__ = (
        CheckConstraint(
            "status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', "
            "'CANCEL_REQUESTED', 'CANCELLED', 'ORPHANED')",
            name="ck_task_attempt_status",
        ),
        CheckConstraint("attempt_no > 0", name="ck_task_attempt_positive"),
        UniqueConstraint("task_id", "attempt_no", name="uq_task_attempt_no"),
        Index("ix_task_attempt_task", "task_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    task_id: Mapped[str] = mapped_column(
        ForeignKey("task.id", ondelete="CASCADE"), nullable=False
    )
    attempt_no: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    worker_id: Mapped[str | None] = mapped_column(String(128))
    started_at: Mapped[str] = mapped_column(String(32), nullable=False)
    heartbeat_at: Mapped[str | None] = mapped_column(String(32))
    completed_at: Mapped[str | None] = mapped_column(String(32))
    log_path: Mapped[str | None] = mapped_column(String)
    progress_json: Mapped[str] = mapped_column(String, nullable=False)
    error_json: Mapped[str | None] = mapped_column(String)


class AuditEventORM(Base):
    __tablename__ = "audit_event"
    __table_args__ = (
        Index("ix_audit_event_experiment_created", "experiment_id", "created_at"),
        Index("ix_audit_event_task_created", "task_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    experiment_id: Mapped[str | None] = mapped_column(
        ForeignKey("experiment.id", ondelete="SET NULL")
    )
    task_id: Mapped[str | None] = mapped_column(
        ForeignKey("task.id", ondelete="SET NULL")
    )
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    actor: Mapped[str | None] = mapped_column(String(128))
    details_json: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)
