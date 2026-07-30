"""Hash-checkpointed orchestration from provider Raw to immutable snapshot."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Never, Protocol, cast

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
from quant_core.data.quality.runner import QualityRunner
from quant_core.data.snapshots import SnapshotPublisher
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
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._source = source
        self._mapper = mapper
        self._calendar = calendar
        self._raw_store = raw_store
        self._curated_store = curated_store
        self._repository = repository
        self._quality_runner = quality_runner
        self._snapshot_publisher = snapshot_publisher
        self._clock = clock

    def bootstrap(self) -> PipelineResult:
        start, end = self._calendar.bootstrap_window(20)
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
        if start is not None and end is not None:
            resolved_start, resolved_end = self._calendar.explicit_window(start, end)
        else:
            daily_id = previous.dataset_versions.get(DatasetKind.DAILY_BAR.value)
            if daily_id is None:
                self._raise_argument("latest snapshot has no daily_bar watermark")
            watermark = self._repository.get_dataset_version(daily_id).end_date
            if watermark is None:
                self._raise_argument("latest daily_bar version has no watermark")
            resolved_start, resolved_end = self._calendar.update_window(watermark, 5)
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
        run = self._repository.latest_recoverable_pipeline_run(self._source.provider)
        if run is None:
            self._raise_argument("no recoverable pipeline run exists")
        assert run is not None
        curated = self._curated_checkpoint(run.id)
        quality_id = self._validate(
            run.id, _versions_hash(curated.dataset_versions), curated
        )
        return {
            "run_id": run.id,
            "quality_run_id": str(quality_id),
            "status": "VALIDATED",
        }

    def publish_latest(self) -> PipelineResult:
        """Publish the latest run whose validation checkpoint is complete."""
        run = self._repository.latest_recoverable_pipeline_run(self._source.provider)
        if run is None:
            self._raise_argument("no recoverable pipeline run exists")
        assert run is not None
        curated = self._curated_checkpoint(run.id)
        curated_hash = _versions_hash(curated.dataset_versions)
        quality_checkpoint = self._checkpoint(
            run.id, PipelineStageName.VALIDATE, curated_hash
        )
        if not isinstance(quality_checkpoint, Mapping):
            self._raise_argument("latest pipeline run has no successful validation")
        quality_id = QualityRunId.parse(str(quality_checkpoint["quality_run_id"]))
        snapshot_id = self._publish(
            run.id,
            _hash({"versions": curated_hash, "quality": str(quality_id)}),
            curated,
            quality_id,
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
            )
        )

        raw = self._ingest(run.id, request_hash, start, end)
        raw_output_hash = _hash([partition_to_json(item) for item in raw])
        previous = {
            dataset: self._repository.get_dataset_version(identifier)
            for dataset, identifier in (previous_ids or {}).items()
        }
        curated = self._curate(run.id, raw_output_hash, raw, previous, start, end)
        curated_hash = _versions_hash(curated.dataset_versions)
        quality_id = self._validate(run.id, curated_hash, curated)
        snapshot_id = self._publish(
            run.id,
            _hash({"versions": curated_hash, "quality": str(quality_id)}),
            curated,
            quality_id,
        )
        self._repository.complete_pipeline_run(run.id, self._now())
        return PipelineResult(run.id, curated.dataset_versions, quality_id, snapshot_id)

    def _ingest(
        self, run_id: str, input_hash: str, start: date, end: date
    ) -> tuple[PublishedPartition, ...]:
        checkpoint = self._checkpoint(run_id, PipelineStageName.INGEST_RAW, input_hash)
        if checkpoint is not None:
            try:
                values = cast(Mapping[str, object], checkpoint)
                items = values.get("partitions")
                if not isinstance(items, tuple):
                    raise TypeError("raw checkpoint partitions are invalid")
                return tuple(
                    partition_from_json(cast(Mapping[str, object], item))
                    for item in items
                )
            except Exception as error:  # noqa: BLE001 - integrity boundary.
                self._raise_checkpoint(PipelineStageName.INGEST_RAW, error)
        self._repository.start_pipeline_stage(
            run_id,
            PipelineStageName.INGEST_RAW,
            input_hash=input_hash,
            started_at=self._now(),
        )
        try:
            self._source.login()
            try:
                partitions = tuple(
                    self._raw_store.publish(batch, run_id=run_id)
                    for batch in self._source.fetch_range(start, end)
                )
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
            )
            return partitions
        except Exception as error:
            self._fail(run_id, PipelineStageName.INGEST_RAW, input_hash, error)
            raise

    def _curate(
        self,
        run_id: str,
        input_hash: str,
        raw: tuple[PublishedPartition, ...],
        previous: Mapping[str, DatasetVersionRecord],
        start: date,
        end: date,
    ) -> CuratedResult:
        checkpoint = self._checkpoint(run_id, PipelineStageName.CURATE, input_hash)
        if checkpoint is not None:
            values = cast(Mapping[str, object], checkpoint)
            serialized = values.get("dataset_versions")
            if not isinstance(serialized, Mapping):
                raise ValueError("curated checkpoint versions are invalid")
            version_ids = {
                str(dataset): DatasetVersionId.parse(str(identifier))
                for dataset, identifier in serialized.items()
            }
            frames = {
                record.dataset: self._curated_store.read_version(record)
                for record in (
                    self._repository.get_dataset_version(identifier)
                    for identifier in version_ids.values()
                )
            }
            return CuratedResult(version_ids, frames)
        self._repository.start_pipeline_stage(
            run_id,
            PipelineStageName.CURATE,
            input_hash=input_hash,
            started_at=self._now(),
        )
        try:
            batches = tuple(
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
            )
            return result
        except Exception as error:
            self._fail(run_id, PipelineStageName.CURATE, input_hash, error)
            raise

    def _validate(
        self, run_id: str, input_hash: str, curated: CuratedResult
    ) -> QualityRunId:
        checkpoint = self._checkpoint(run_id, PipelineStageName.VALIDATE, input_hash)
        if checkpoint is not None:
            values = cast(Mapping[str, object], checkpoint)
            identifier = QualityRunId.parse(str(values["quality_run_id"]))
            self._repository.get_quality_run(identifier)
            return identifier
        self._repository.start_pipeline_stage(
            run_id,
            PipelineStageName.VALIDATE,
            input_hash=input_hash,
            started_at=self._now(),
        )
        try:
            started = self._now()
            issues = self._quality_runner.evaluate(curated.frames)
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
            )
            return quality.id
        except Exception as error:
            self._fail(run_id, PipelineStageName.VALIDATE, input_hash, error)
            raise

    def _publish(
        self,
        run_id: str,
        input_hash: str,
        curated: CuratedResult,
        quality_id: QualityRunId,
    ) -> SnapshotId:
        checkpoint = self._checkpoint(
            run_id, PipelineStageName.PUBLISH_SNAPSHOT, input_hash
        )
        if checkpoint is not None:
            values = cast(Mapping[str, object], checkpoint)
            identifier = SnapshotId.parse(str(values["snapshot_id"]))
            self._repository.get_snapshot(identifier)
            return identifier
        self._repository.start_pipeline_stage(
            run_id,
            PipelineStageName.PUBLISH_SNAPSHOT,
            input_hash=input_hash,
            started_at=self._now(),
        )
        try:
            identifier = self._snapshot_publisher.publish(
                curated.dataset_versions, quality_id
            )
            output: JsonValue = {"snapshot_id": str(identifier)}
            self._repository.complete_pipeline_stage(
                run_id,
                PipelineStageName.PUBLISH_SNAPSHOT,
                input_hash=input_hash,
                output_hash=_hash(output),
                output=output,
                completed_at=self._now(),
            )
            return identifier
        except Exception as error:
            self._fail(
                run_id,
                PipelineStageName.PUBLISH_SNAPSHOT,
                input_hash,
                error,
                blocked=isinstance(error, QuantError)
                and error.detail.code == "SNAP_QUALITY_BLOCKED",
            )
            raise

    def _checkpoint(
        self, run_id: str, stage: PipelineStageName, input_hash: str
    ) -> object | None:
        try:
            checkpoint = self._repository.get_pipeline_stage(run_id, stage)
        except KeyError:
            return None
        if checkpoint.status != "SUCCEEDED":
            return None
        if checkpoint.input_hash != input_hash or checkpoint.output is None:
            raise ValueError("successful pipeline checkpoint does not match its input")
        if checkpoint.output_hash != _hash(thaw_json(checkpoint.output)):
            raise ValueError("successful pipeline checkpoint output hash is invalid")
        return checkpoint.output

    def _curated_checkpoint(self, run_id: str) -> CuratedResult:
        try:
            checkpoint = self._repository.get_pipeline_stage(
                run_id, PipelineStageName.CURATE
            )
        except KeyError:
            self._raise_argument("latest pipeline run has no Curated checkpoint")
        if checkpoint.status != "SUCCEEDED" or checkpoint.output is None:
            self._raise_argument(
                "latest pipeline run has no successful Curated checkpoint"
            )
        values = cast(Mapping[str, object], checkpoint.output)
        serialized = values.get("dataset_versions")
        if not isinstance(serialized, Mapping):
            raise TypeError("curated checkpoint versions are invalid")
        version_ids = {
            str(dataset): DatasetVersionId.parse(str(identifier))
            for dataset, identifier in serialized.items()
        }
        frames: dict[DatasetKind, tuple[pl.DataFrame, ...]] = {}
        for identifier in version_ids.values():
            record = self._repository.get_dataset_version(identifier)
            frames[record.dataset] = self._curated_store.read_version(record)
        return CuratedResult(version_ids, frames)

    def _fail(
        self,
        run_id: str,
        stage: PipelineStageName,
        input_hash: str,
        error: Exception,
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
        self._repository.fail_pipeline_stage(
            run_id,
            stage,
            input_hash=input_hash,
            error=detail,
            completed_at=self._now(),
            blocked=blocked,
        )

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("pipeline clock must return timezone-aware timestamps")
        return value.astimezone(UTC)

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


def _versions_hash(versions: Mapping[str, DatasetVersionId]) -> str:
    return _hash({key: str(value) for key, value in sorted(versions.items())})
