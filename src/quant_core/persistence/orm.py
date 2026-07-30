"""Private SQLAlchemy mappings for metadata persistence."""

from __future__ import annotations

from sqlalchemy import ForeignKey, Integer, String, UniqueConstraint
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
