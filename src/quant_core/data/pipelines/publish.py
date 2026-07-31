"""Hash-checkpointed orchestration from provider Raw to immutable snapshot."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Never, Protocol, cast
from uuid import uuid4

import polars as pl

from quant_core.data.contracts import (
    CanonicalMapper,
    JsonValue,
    PublishedPartition,
    RawBatch,
    canonical_json_bytes,
)
from quant_core.data.partitions import RawPartitionStore
from quant_core.data.pipelines.curate import CuratedPartitionStore, CuratedResult
from quant_core.data.pipelines.ingest import partition_from_json, partition_to_json
from quant_core.data.quality.models import QualityRunSpec, thaw_json
from quant_core.data.quality.rules import (
    QUALITY_RULE_SET_VERSION,
    required_dataset_issues,
)
from quant_core.data.quality.runner import QualityRunner
from quant_core.data.schemas import CANONICAL_SCHEMA_VERSION
from quant_core.data.snapshots import SNAPSHOT_MANIFEST_VERSION, SnapshotPublisher
from quant_core.domain.enums import DatasetKind, Severity
from quant_core.domain.identifiers import DatasetVersionId, QualityRunId, SnapshotId
from quant_core.errors import ErrorDetail, QuantError
from quant_core.persistence.repositories import (
    DatasetVersionRecord,
    MetadataRepository,
    PipelineRunSpec,
    PipelineStageName,
)


class PipelineSource(Protocol):
    @property
    def provider(self) -> str: ...

    def login(self) -> None: ...

    def close(self) -> None: ...

    def fetch_range(self, start: date, end: date) -> Iterable[RawBatch]: ...


class CalendarPolicy(Protocol):
    def bootstrap_window(self, years: int) -> tuple[date, date]: ...

    def explicit_window(self, start: date, end: date) -> tuple[date, date]: ...

    def update_window(
        self, watermark: date, overlap_days: int
    ) -> tuple[date, date]: ...


@dataclass(frozen=True, slots=True)
class PipelineResult:
    run_id: str
    dataset_versions: Mapping[str, DatasetVersionId]
    quality_run_id: QualityRunId
    snapshot_id: SnapshotId


@dataclass(frozen=True, slots=True)
class PipelineVersions:
    """Explicit versions for every stable component that affects pipeline output."""

    source_adapter: str = "pipeline-source-contract-v1"
    fetch_config: str = "pipeline-fetch-config-v1"
    mapper: str = "canonical-mapper-v1"
    canonical_schema: str = CANONICAL_SCHEMA_VERSION
    quality_rules: str = QUALITY_RULE_SET_VERSION
    snapshot_manifest: str = SNAPSHOT_MANIFEST_VERSION

    def __post_init__(self) -> None:
        if any(not value for value in self.as_json().values()):
            raise ValueError("pipeline version fingerprints must not be empty")

    def as_json(self) -> dict[str, JsonValue]:
        return {
            "source_adapter": self.source_adapter,
            "fetch_config": self.fetch_config,
            "mapper": self.mapper,
            "canonical_schema": self.canonical_schema,
            "quality_rules": self.quality_rules,
            "snapshot_manifest": self.snapshot_manifest,
        }


class DataPipeline:
    """Execute and recover the fixed four-stage data pipeline."""

    def __init__(
        self,
        *,
        source: PipelineSource,
        mapper: CanonicalMapper,
        calendar: CalendarPolicy,
        raw_store: RawPartitionStore,
        curated_store: CuratedPartitionStore,
        repository: MetadataRepository,
        quality_runner: QualityRunner,
        snapshot_publisher: SnapshotPublisher,
        versions: PipelineVersions | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        lease_duration: timedelta = timedelta(minutes=30),
    ) -> None:
        self._source = source
        self._mapper = mapper
        self._calendar = calendar
        self._raw_store = raw_store
        self._curated_store = curated_store
        self._repository = repository
        self._quality_runner = quality_runner
        self._snapshot_publisher = snapshot_publisher
        self._versions = versions or PipelineVersions()
        self._pipeline_fingerprint = _hash(self._versions.as_json())
        self._clock = clock
        if lease_duration <= timedelta(0):
            raise ValueError("pipeline lease duration must be positive")
        self._lease_duration = lease_duration

    def bootstrap(self) -> PipelineResult:
        try:
            start, end = self._calendar.bootstrap_window(20)
        except ValueError as error:
            self._raise_argument(str(error))
        return self._execute("BOOTSTRAP", None, None, start, end, None)

    def update(
        self, *, start: date | None = None, end: date | None = None
    ) -> PipelineResult:
        if (start is None) != (end is None):
            self._raise_argument("start and end must be supplied together")
        previous = self._repository.latest_snapshot()
        if previous is None:
            self._raise_argument("update requires an existing published snapshot")
        assert previous is not None
        previous_records = {
            dataset: self._repository.get_dataset_version(identifier)
            for dataset, identifier in previous.dataset_versions.items()
        }
        incompatible = sorted(
            dataset
            for dataset, record in previous_records.items()
            if record.source != self._source.provider
        )
        if incompatible:
            raise QuantError(
                ErrorDetail(
                    code="DATA_PIPELINE_PROVIDER_MISMATCH",
                    severity=Severity.FATAL,
                    message="update cannot merge history from a different provider",
                    context={
                        "current_provider": self._source.provider,
                        "datasets": incompatible,
                    },
                    remediation="run an explicit bootstrap or controlled migration",
                    retryable=False,
                )
            )
        try:
            if start is not None and end is not None:
                resolved_start, resolved_end = self._calendar.explicit_window(
                    start, end
                )
            else:
                daily_id = previous.dataset_versions.get(DatasetKind.DAILY_BAR.value)
                if daily_id is None:
                    self._raise_argument("latest snapshot has no daily_bar watermark")
                watermark = previous_records[DatasetKind.DAILY_BAR.value].end_date
                if watermark is None:
                    self._raise_argument("latest daily_bar version has no watermark")
                resolved_start, resolved_end = self._calendar.update_window(
                    watermark, 5
                )
        except ValueError as error:
            self._raise_argument(str(error))
        return self._execute(
            "UPDATE",
            start,
            end,
            resolved_start,
            resolved_end,
            previous.dataset_versions,
        )

    def validate_latest(self) -> Mapping[str, JsonValue]:
        """Validate the latest run whose Curated checkpoint is complete."""
        run = self._repository.latest_pipeline_run_ready_for(
            PipelineStageName.VALIDATE,
            provider=self._source.provider,
            pipeline_fingerprint=self._pipeline_fingerprint,
        )
        if run is None:
            self._raise_argument("no recoverable pipeline run exists")
        assert run is not None
        curated, curated_output_hash = self._resume_curated(run.id)
        quality_id = self._validate(
            run.id,
            _hash(
                {
                    "curated_output_hash": curated_output_hash,
                    "quality_rules": self._versions.quality_rules,
                }
            ),
            curated,
            uuid4().hex,
        )
        return {
            "run_id": run.id,
            "quality_run_id": str(quality_id),
            "status": "VALIDATED",
        }

    def publish_latest(self) -> PipelineResult:
        """Publish the latest run whose validation checkpoint is complete."""
        run = self._repository.latest_pipeline_run_ready_for(
            PipelineStageName.PUBLISH_SNAPSHOT,
            provider=self._source.provider,
            pipeline_fingerprint=self._pipeline_fingerprint,
        )
        if run is None:
            self._raise_argument("no recoverable pipeline run exists")
        assert run is not None
        owner_id = uuid4().hex
        curated, curated_output_hash = self._resume_curated(run.id)
        validate_input_hash = _hash(
            {
                "curated_output_hash": curated_output_hash,
                "quality_rules": self._versions.quality_rules,
            }
        )
        quality_checkpoint = self._checkpoint(
            run.id,
            PipelineStageName.VALIDATE,
            validate_input_hash,
            lambda value: self._restore_quality(value, curated.dataset_versions),
        )
        if not isinstance(quality_checkpoint, QualityRunId):
            self._raise_argument("latest pipeline run has no successful validation")
        quality_id = quality_checkpoint
        snapshot_id = self._publish(
            run.id,
            _hash(
                {
                    "quality_output_hash": _hash({"quality_run_id": str(quality_id)}),
                    "snapshot_manifest": self._versions.snapshot_manifest,
                }
            ),
            curated,
            quality_id,
            owner_id,
        )
        self._repository.complete_pipeline_run(run.id, self._now())
        return PipelineResult(run.id, curated.dataset_versions, quality_id, snapshot_id)

    def _execute(
        self,
        mode: str,
        requested_start: date | None,
        requested_end: date | None,
        start: date,
        end: date,
        previous_ids: Mapping[str, DatasetVersionId] | None,
    ) -> PipelineResult:
        owner_id = uuid4().hex
        request: JsonValue = {
            "mode": mode,
            "provider": self._source.provider,
            "requested_start": requested_start.isoformat() if requested_start else None,
            "requested_end": requested_end.isoformat() if requested_end else None,
            "resolved_start": start.isoformat(),
            "resolved_end": end.isoformat(),
            "previous_versions": {
                key: str(value) for key, value in sorted((previous_ids or {}).items())
            },
            "versions": self._versions.as_json(),
        }
        request_hash = _hash(request)
        run = self._repository.register_pipeline_run(
            PipelineRunSpec(
                mode=mode,
                provider=self._source.provider,
                request_hash=request_hash,
                requested_start=requested_start,
                requested_end=requested_end,
                resolved_start=start,
                resolved_end=end,
                created_at=self._now(),
                pipeline_fingerprint=self._pipeline_fingerprint,
            )
        )

        ingest_input_hash = _hash(
            {
                "request_hash": request_hash,
                "source_adapter": self._versions.source_adapter,
                "fetch_config": self._versions.fetch_config,
            }
        )
        raw = self._ingest(run.id, ingest_input_hash, start, end, owner_id)
        raw_output_hash = _hash(
            {"partitions": [partition_to_json(item) for item in raw]}
        )
        previous = {
            dataset: self._repository.get_dataset_version(identifier)
            for dataset, identifier in (previous_ids or {}).items()
        }
        curate_input_hash = _hash(
            {
                "raw_output_hash": raw_output_hash,
                "mapper": self._versions.mapper,
                "canonical_schema": self._versions.canonical_schema,
            }
        )
        curated = self._curate(
            run.id, curate_input_hash, raw, previous, start, end, owner_id
        )
        curated_hash = _hash(
            {
                "dataset_versions": {
                    key: str(value)
                    for key, value in sorted(curated.dataset_versions.items())
                }
            }
        )
        validate_input_hash = _hash(
            {
                "curated_output_hash": curated_hash,
                "quality_rules": self._versions.quality_rules,
            }
        )
        quality_id = self._validate(run.id, validate_input_hash, curated, owner_id)
        quality_output_hash = _hash({"quality_run_id": str(quality_id)})
        snapshot_id = self._publish(
            run.id,
            _hash(
                {
                    "quality_output_hash": quality_output_hash,
                    "snapshot_manifest": self._versions.snapshot_manifest,
                }
            ),
            curated,
            quality_id,
            owner_id,
        )
        self._repository.complete_pipeline_run(run.id, self._now())
        return PipelineResult(run.id, curated.dataset_versions, quality_id, snapshot_id)

    def _ingest(
        self,
        run_id: str,
        input_hash: str,
        start: date,
        end: date,
        owner_id: str,
    ) -> tuple[PublishedPartition, ...]:
        checkpoint = self._checkpoint(
            run_id,
            PipelineStageName.INGEST_RAW,
            input_hash,
            self._restore_raw,
        )
        if checkpoint is not None:
            return cast(tuple[PublishedPartition, ...], checkpoint)
        claim = self._repository.start_pipeline_stage(
            run_id,
            PipelineStageName.INGEST_RAW,
            input_hash=input_hash,
            started_at=self._now(),
            owner_id=owner_id,
            lease_expires_at=self._lease_expires_at(),
        )
        try:
            self._source.login()
            try:
                published: list[PublishedPartition] = []
                for batch in self._source.fetch_range(start, end):
                    self._heartbeat(
                        run_id, PipelineStageName.INGEST_RAW, owner_id, claim.attempt
                    )
                    published.append(self._raw_store.publish(batch, run_id=run_id))
                    self._heartbeat(
                        run_id, PipelineStageName.INGEST_RAW, owner_id, claim.attempt
                    )
                partitions = tuple(published)
            finally:
                self._source.close()
            output: JsonValue = {
                "partitions": [partition_to_json(item) for item in partitions]
            }
            self._repository.complete_pipeline_stage(
                run_id,
                PipelineStageName.INGEST_RAW,
                input_hash=input_hash,
                output_hash=_hash(output),
                output=output,
                completed_at=self._now(),
                owner_id=owner_id,
                attempt=claim.attempt,
            )
            return partitions
        except Exception as error:
            self._fail(
                run_id,
                PipelineStageName.INGEST_RAW,
                input_hash,
                error,
                owner_id,
                claim.attempt,
            )
            raise

    def _curate(
        self,
        run_id: str,
        input_hash: str,
        raw: tuple[PublishedPartition, ...],
        previous: Mapping[str, DatasetVersionRecord],
        start: date,
        end: date,
        owner_id: str,
    ) -> CuratedResult:
        checkpoint = self._checkpoint(
            run_id,
            PipelineStageName.CURATE,
            input_hash,
            lambda value: self._restore_curated(value, run_id),
        )
        if checkpoint is not None:
            return cast(CuratedResult, checkpoint)
        claim = self._repository.start_pipeline_stage(
            run_id,
            PipelineStageName.CURATE,
            input_hash=input_hash,
            started_at=self._now(),
            owner_id=owner_id,
            lease_expires_at=self._lease_expires_at(),
        )
        try:
            batches = (
                batch
                for partition in raw
                for batch in self._mapper.normalize(partition)
            )
            result = self._curated_store.publish(
                batches,
                previous_versions=previous,
                run_id=run_id,
                source=self._source.provider,
                start=start,
                end=end,
                repository=self._repository,
                heartbeat=lambda: self._heartbeat(
                    run_id, PipelineStageName.CURATE, owner_id, claim.attempt
                ),
            )
            output: JsonValue = {
                "dataset_versions": {
                    key: str(value)
                    for key, value in sorted(result.dataset_versions.items())
                }
            }
            self._repository.complete_pipeline_stage(
                run_id,
                PipelineStageName.CURATE,
                input_hash=input_hash,
                output_hash=_hash(output),
                output=output,
                completed_at=self._now(),
                owner_id=owner_id,
                attempt=claim.attempt,
            )
            return result
        except Exception as error:
            self._fail(
                run_id,
                PipelineStageName.CURATE,
                input_hash,
                error,
                owner_id,
                claim.attempt,
            )
            raise

    def _validate(
        self,
        run_id: str,
        input_hash: str,
        curated: CuratedResult,
        owner_id: str,
    ) -> QualityRunId:
        checkpoint = self._checkpoint(
            run_id,
            PipelineStageName.VALIDATE,
            input_hash,
            lambda value: self._restore_quality(value, curated.dataset_versions),
        )
        if checkpoint is not None:
            return cast(QualityRunId, checkpoint)
        claim = self._repository.start_pipeline_stage(
            run_id,
            PipelineStageName.VALIDATE,
            input_hash=input_hash,
            started_at=self._now(),
            owner_id=owner_id,
            lease_expires_at=self._lease_expires_at(),
        )
        try:
            started = self._now()
            heartbeat: Callable[[], None] = lambda: self._heartbeat(
                run_id, PipelineStageName.VALIDATE, owner_id, claim.attempt
            )
            issues = (
                *self._quality_runner.evaluate(
                    curated.frames,
                    heartbeat=heartbeat,
                ),
            )
            heartbeat()
            issues = (
                *issues,
                *required_dataset_issues(curated.frames),
            )
            heartbeat()
            quality = self._repository.register_quality_run(
                QualityRunSpec(
                    dataset_versions=curated.dataset_versions,
                    started_at=started,
                    completed_at=self._now(),
                    issues=issues,
                )
            )
            output: JsonValue = {"quality_run_id": str(quality.id)}
            self._repository.complete_pipeline_stage(
                run_id,
                PipelineStageName.VALIDATE,
                input_hash=input_hash,
                output_hash=_hash(output),
                output=output,
                completed_at=self._now(),
                owner_id=owner_id,
                attempt=claim.attempt,
            )
            return quality.id
        except Exception as error:
            self._fail(
                run_id,
                PipelineStageName.VALIDATE,
                input_hash,
                error,
                owner_id,
                claim.attempt,
            )
            raise

    def _publish(
        self,
        run_id: str,
        input_hash: str,
        curated: CuratedResult,
        quality_id: QualityRunId,
        owner_id: str,
    ) -> SnapshotId:
        checkpoint = self._checkpoint(
            run_id,
            PipelineStageName.PUBLISH_SNAPSHOT,
            input_hash,
            lambda value: self._restore_snapshot(
                value, curated.dataset_versions, quality_id
            ),
        )
        if checkpoint is not None:
            return cast(SnapshotId, checkpoint)
        claim = self._repository.start_pipeline_stage(
            run_id,
            PipelineStageName.PUBLISH_SNAPSHOT,
            input_hash=input_hash,
            started_at=self._now(),
            owner_id=owner_id,
            lease_expires_at=self._lease_expires_at(),
        )
        try:
            self._heartbeat(
                run_id, PipelineStageName.PUBLISH_SNAPSHOT, owner_id, claim.attempt
            )
            identifier = self._snapshot_publisher.publish(
                curated.dataset_versions, quality_id
            )
            self._heartbeat(
                run_id, PipelineStageName.PUBLISH_SNAPSHOT, owner_id, claim.attempt
            )
            output: JsonValue = {"snapshot_id": str(identifier)}
            self._repository.complete_pipeline_stage(
                run_id,
                PipelineStageName.PUBLISH_SNAPSHOT,
                input_hash=input_hash,
                output_hash=_hash(output),
                output=output,
                completed_at=self._now(),
                owner_id=owner_id,
                attempt=claim.attempt,
            )
            return identifier
        except Exception as error:
            self._fail(
                run_id,
                PipelineStageName.PUBLISH_SNAPSHOT,
                input_hash,
                error,
                owner_id,
                claim.attempt,
                blocked=isinstance(error, QuantError)
                and error.detail.code == "SNAP_QUALITY_BLOCKED",
            )
            raise

    def _checkpoint(
        self,
        run_id: str,
        stage: PipelineStageName,
        input_hash: str,
        verifier: Callable[[Mapping[str, object]], object] | None = None,
    ) -> object | None:
        try:
            checkpoint = self._repository.get_pipeline_stage(run_id, stage)
        except KeyError:
            return None
        try:
            if checkpoint.status != "SUCCEEDED":
                return None
            if checkpoint.input_hash != input_hash or checkpoint.output is None:
                raise ValueError(
                    "successful pipeline checkpoint does not match its input"
                )
            output = thaw_json(checkpoint.output)
            if checkpoint.output_hash != _hash(output):
                raise ValueError(
                    "successful pipeline checkpoint output hash is invalid"
                )
            if not isinstance(output, Mapping):
                raise TypeError("pipeline checkpoint output is not an object")
            return verifier(output) if verifier is not None else output
        except Exception as error:  # noqa: BLE001 - checkpoint trust boundary.
            self._raise_checkpoint(stage, error)

    def _restore_raw(
        self, values: Mapping[str, object]
    ) -> tuple[PublishedPartition, ...]:
        items = values.get("partitions")
        if not isinstance(items, list):
            raise TypeError("raw checkpoint partitions are invalid")
        return tuple(
            partition_from_json(cast(Mapping[str, object], item), self._raw_store.root)
            for item in items
        )

    def _restore_curated(
        self, values: Mapping[str, object], run_id: str
    ) -> CuratedResult:
        serialized = values.get("dataset_versions")
        if not isinstance(serialized, Mapping):
            raise TypeError("curated checkpoint versions are invalid")
        version_ids = {
            str(dataset): DatasetVersionId.parse(str(identifier))
            for dataset, identifier in serialized.items()
        }
        run = self._repository.get_pipeline_run(run_id)
        frames: dict[DatasetKind, tuple[pl.LazyFrame, ...]] = {}
        for dataset, identifier in version_ids.items():
            record = self._repository.get_dataset_version(identifier)
            range_dataset = record.dataset in {
                DatasetKind.TRADE_CALENDAR,
                DatasetKind.DAILY_BAR,
                DatasetKind.SECURITY_STATUS,
            }
            if (
                record.dataset.value != dataset
                or record.status != "PUBLISHED"
                or record.source != run.provider
                or (
                    range_dataset
                    and (
                        record.start_date is None
                        or record.end_date is None
                        or record.start_date > run.resolved_start
                        or record.end_date < run.resolved_end
                    )
                )
            ):
                raise ValueError("curated checkpoint dataset scope is invalid")
            self._curated_store.verify_version(record)
            frames[record.dataset] = self._curated_store.scan_version(record)
        return CuratedResult(version_ids, frames)

    def _restore_quality(
        self,
        values: Mapping[str, object],
        dataset_versions: Mapping[str, DatasetVersionId],
    ) -> QualityRunId:
        identifier = QualityRunId.parse(str(values["quality_run_id"]))
        quality = self._repository.get_quality_run(identifier)
        if quality.status != "COMPLETED" or dict(quality.dataset_versions) != dict(
            dataset_versions
        ):
            raise ValueError("quality checkpoint scope or status is invalid")
        return identifier

    def _restore_snapshot(
        self,
        values: Mapping[str, object],
        dataset_versions: Mapping[str, DatasetVersionId],
        quality_id: QualityRunId,
    ) -> SnapshotId:
        identifier = SnapshotId.parse(str(values["snapshot_id"]))
        self._snapshot_publisher.verify_published(
            identifier, dataset_versions, quality_id
        )
        return identifier

    def _resume_curated(self, run_id: str) -> tuple[CuratedResult, str]:
        run = self._repository.get_pipeline_run(run_id)
        ingest_input_hash = _hash(
            {
                "request_hash": run.request_hash,
                "source_adapter": self._versions.source_adapter,
                "fetch_config": self._versions.fetch_config,
            }
        )
        raw = self._checkpoint(
            run_id,
            PipelineStageName.INGEST_RAW,
            ingest_input_hash,
            self._restore_raw,
        )
        if raw is None:
            self._raise_argument("pipeline run has no successful Raw checkpoint")
        raw_stage = self._repository.get_pipeline_stage(
            run_id, PipelineStageName.INGEST_RAW
        )
        assert raw_stage.output_hash is not None
        curate_input_hash = _hash(
            {
                "raw_output_hash": raw_stage.output_hash,
                "mapper": self._versions.mapper,
                "canonical_schema": self._versions.canonical_schema,
            }
        )
        curated = self._checkpoint(
            run_id,
            PipelineStageName.CURATE,
            curate_input_hash,
            lambda value: self._restore_curated(value, run_id),
        )
        if not isinstance(curated, CuratedResult):
            self._raise_argument("pipeline run has no successful Curated checkpoint")
        curated_stage = self._repository.get_pipeline_stage(
            run_id, PipelineStageName.CURATE
        )
        assert curated_stage.output_hash is not None
        return curated, curated_stage.output_hash

    def _fail(
        self,
        run_id: str,
        stage: PipelineStageName,
        input_hash: str,
        error: Exception,
        owner_id: str,
        attempt: int,
        *,
        blocked: bool = False,
    ) -> None:
        detail: JsonValue
        if isinstance(error, QuantError):
            detail = {
                "code": error.detail.code,
                "severity": error.detail.severity.value,
                "message": error.detail.message,
                "context": dict(cast(Mapping[str, JsonValue], error.detail.context)),
                "remediation": error.detail.remediation,
                "retryable": error.detail.retryable,
            }
        else:
            detail = {
                "code": "DATA_PIPELINE_STAGE_FAILED",
                "severity": Severity.SEVERE.value,
                "message": str(error),
                "context": {"error_type": type(error).__name__},
                "remediation": "fix the stage error and resume the same run",
                "retryable": False,
            }
        try:
            self._repository.fail_pipeline_stage(
                run_id,
                stage,
                input_hash=input_hash,
                error=detail,
                completed_at=self._now(),
                owner_id=owner_id,
                attempt=attempt,
                blocked=blocked,
            )
        except QuantError as failure:
            if failure.detail.code != "DATA_PIPELINE_BUSY":
                raise

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("pipeline clock must return timezone-aware timestamps")
        return value.astimezone(UTC)

    def _lease_expires_at(self) -> datetime:
        return self._now() + self._lease_duration

    def _heartbeat(
        self,
        run_id: str,
        stage: PipelineStageName,
        owner_id: str,
        attempt: int,
    ) -> None:
        now = self._now()
        self._repository.renew_pipeline_stage_lease(
            run_id,
            stage,
            owner_id=owner_id,
            attempt=attempt,
            renewed_at=now,
            lease_expires_at=now + self._lease_duration,
        )

    @staticmethod
    def _raise_checkpoint(stage: PipelineStageName, cause: Exception) -> Never:
        raise QuantError(
            ErrorDetail(
                code="DATA_CHECKPOINT_INVALID",
                severity=Severity.FATAL,
                message="pipeline checkpoint output failed content verification",
                context={"stage": stage.value, "error_type": type(cause).__name__},
                remediation="restore immutable output or start a distinct pipeline request",
                retryable=False,
            )
        ) from cause

    @staticmethod
    def _raise_argument(message: str) -> Never:
        raise QuantError(
            ErrorDetail(
                code="DATA_PIPELINE_ARGUMENT",
                severity=Severity.SEVERE,
                message=message,
                context={},
                remediation="provide a complete valid trading-date range",
                retryable=False,
            )
        )


def _hash(value: JsonValue) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()
