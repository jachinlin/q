"""JSON CLI for data bootstrap, updates, validation and snapshot inspection."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Never, cast

import typer

from quant_core.data.contracts import JsonValue, canonical_json_bytes
from quant_core.data.mappers.baostock import BAOSTOCK_MAPPER_VERSION, BaoStockMapper
from quant_core.data.partitions import RawPartitionStore
from quant_core.data.pipelines.curate import CuratedPartitionStore
from quant_core.data.pipelines.publish import (
    DataPipeline,
    PipelineResult,
    PipelineVersions,
)
from quant_core.data.quality.rules import QUALITY_RULE_SET_VERSION
from quant_core.data.quality.runner import QualityRunner
from quant_core.data.schemas import CANONICAL_SCHEMA_VERSION
from quant_core.data.snapshots import SNAPSHOT_MANIFEST_VERSION, SnapshotPublisher
from quant_core.data.sources.baostock import (
    BAOSTOCK_SOURCE_ADAPTER_VERSION,
    BaoStockCalendarPolicy,
    BaoStockClient,
    BaoStockConfig,
    BaoStockSdkGateway,
)
from quant_core.domain.enums import Severity, SnapshotStatus
from quant_core.errors import ErrorDetail, QuantError
from quant_core.persistence.database import create_sqlite_engine, upgrade_database
from quant_core.persistence.repositories import MetadataRepository
from quant_core.settings import Settings


@dataclass(frozen=True, slots=True)
class ApplicationServices:
    pipeline: DataPipeline
    repository: MetadataRepository


def create_app(
    services_factory: Callable[[], ApplicationServices],
) -> typer.Typer:
    """Build an injectable command tree without constructing services at import."""
    application = typer.Typer(no_args_is_help=True)
    data = typer.Typer(no_args_is_help=True)
    application.add_typer(data, name="data")

    @data.command("bootstrap")
    def bootstrap() -> None:
        _invoke(lambda services: services.pipeline.bootstrap(), services_factory)

    @data.command("update")
    def update(
        start: str | None = typer.Option(None),
        end: str | None = typer.Option(None),
    ) -> None:
        if (start is None) != (end is None):
            _emit_error(
                QuantError(
                    ErrorDetail(
                        code="DATA_PIPELINE_ARGUMENT",
                        severity=Severity.SEVERE,
                        message="start and end must be supplied together",
                        context={},
                        remediation="provide both --start and --end or neither",
                        retryable=False,
                    )
                )
            )
        parsed_start = _parse_cli_date(start, "start") if start is not None else None
        parsed_end = _parse_cli_date(end, "end") if end is not None else None
        _invoke(
            lambda services: services.pipeline.update(
                start=parsed_start, end=parsed_end
            ),
            services_factory,
        )

    @data.command("validate")
    def validate() -> None:
        _invoke(lambda services: services.pipeline.validate_latest(), services_factory)

    @data.command("publish")
    def publish() -> None:
        _invoke(lambda services: services.pipeline.publish_latest(), services_factory)

    @data.command("snapshots")
    def snapshots() -> None:
        def list_records(services: ApplicationServices) -> Mapping[str, object]:
            return {
                "snapshots": [
                    {
                        "snapshot_id": str(record.id),
                        "as_of": record.as_of.isoformat(),
                        "status": record.status.value,
                        "quality_run_id": str(record.quality_run_id),
                        "datasets": {
                            key: str(value)
                            for key, value in sorted(record.dataset_versions.items())
                        },
                    }
                    for record in services.repository.list_snapshots()
                    if record.status is SnapshotStatus.PUBLISHED
                ]
            }

        _invoke(list_records, services_factory, add_status=False)

    return application


def _invoke(
    operation: Callable[[ApplicationServices], object],
    services_factory: Callable[[], ApplicationServices],
    *,
    add_status: bool = True,
) -> None:
    try:
        result = operation(services_factory())
    except QuantError as error:
        _emit_error(error)
    except Exception as error:  # noqa: BLE001 - CLI is the structured error boundary.
        _emit_error(
            QuantError(
                ErrorDetail(
                    code="DATA_PIPELINE_UNEXPECTED",
                    severity=Severity.FATAL,
                    message=str(error),
                    context={"error_type": type(error).__name__},
                    remediation="inspect local logs and pipeline checkpoints",
                    retryable=False,
                )
            )
        )
    payload = _result_payload(result)
    if add_status and "status" not in payload:
        payload["status"] = "SUCCEEDED"
    typer.echo(json.dumps(payload, ensure_ascii=False, sort_keys=True))


def _result_payload(result: object) -> dict[str, object]:
    if isinstance(result, Mapping):
        return dict(result)
    if isinstance(result, PipelineResult):
        return {
            "run_id": result.run_id,
            "snapshot_id": str(result.snapshot_id),
            "quality_run_id": str(result.quality_run_id),
            "dataset_versions": {
                str(key): str(identifier)
                for key, identifier in sorted(result.dataset_versions.items())
            },
        }
    raise TypeError("command returned an unsupported result")


def _emit_error(error: QuantError) -> Never:
    payload = {
        "error": {
            "code": error.detail.code,
            "severity": error.detail.severity.value,
            "message": error.detail.message,
            "context": dict(error.detail.context),
            "remediation": error.detail.remediation,
            "retryable": error.detail.retryable,
        }
    }
    typer.echo(json.dumps(payload, ensure_ascii=False, sort_keys=True), err=True)
    raise typer.Exit(code=2)


def _parse_cli_date(value: str, field: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        _emit_error(
            QuantError(
                ErrorDetail(
                    code="DATA_PIPELINE_ARGUMENT",
                    severity=Severity.SEVERE,
                    message=f"{field} must be YYYY-MM-DD",
                    context={"field": field},
                    remediation="provide an ISO calendar date",
                    retryable=False,
                )
            )
        )
    return parsed


def _baostock_fetch_config_fingerprint(config: BaoStockConfig) -> str:
    retryable_error_codes = [
        cast(JsonValue, code) for code in sorted(config.retryable_error_codes)
    ]
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "all_market_route": ("query_daily_history_k_AStock-per-open-date-v1"),
                "selected_route": "query_history_k_data_plus-v1",
                "selected_max_days_per_batch": config.max_days_per_batch,
                "selected_max_instruments_per_batch": (
                    config.max_instruments_per_batch
                ),
                "max_attempts": config.max_attempts,
                "retry_backoff_seconds": list(config.retry_backoff_seconds),
                "retryable_error_codes": retryable_error_codes,
            }
        )
    ).hexdigest()


def build_default_services() -> ApplicationServices:
    """Wire the real local BaoStock-backed application from typed settings."""
    source_root = Path(__file__).resolve().parents[2]
    data_root_text = os.environ.get("QUANT_DATA_ROOT")
    if not data_root_text:
        raise QuantError(
            ErrorDetail(
                code="CFG_DATA_ROOT_REQUIRED",
                severity=Severity.FATAL,
                message="QUANT_DATA_ROOT is required",
                context={},
                remediation="set QUANT_DATA_ROOT outside the source tree",
                retryable=False,
            )
        )
    config_path = Path(
        os.environ.get("QUANT_CONFIG", source_root / "configs" / "base.yaml")
    )
    settings = Settings.load(
        config_path, data_root=Path(data_root_text), source_root=source_root
    )
    upgrade_database(settings.state_db)
    repository = MetadataRepository(create_sqlite_engine(settings.state_db))
    source_gateway = BaoStockSdkGateway()
    calendar_gateway = BaoStockSdkGateway()
    source_config = BaoStockConfig(
        max_instruments_per_batch=100,
        max_days_per_batch=366,
        max_attempts=5,
        retry_backoff_seconds=(1.0, 2.0, 4.0, 8.0),
        retryable_error_codes=frozenset({"-1", "10002007"}),
    )
    source = BaoStockClient(source_gateway, None, source_config)
    calendar_client = BaoStockClient(calendar_gateway, None, source_config)
    fetch_config_fingerprint = _baostock_fetch_config_fingerprint(source_config)
    pipeline = DataPipeline(
        source=source,
        mapper=BaoStockMapper(),
        calendar=BaoStockCalendarPolicy(calendar_client),
        raw_store=RawPartitionStore(settings.raw_root),
        curated_store=CuratedPartitionStore(settings.curated_root),
        repository=repository,
        quality_runner=QualityRunner(),
        snapshot_publisher=SnapshotPublisher(
            repository, settings.data_root / "data" / "snapshots"
        ),
        versions=PipelineVersions(
            source_adapter=BAOSTOCK_SOURCE_ADAPTER_VERSION,
            fetch_config=fetch_config_fingerprint,
            mapper=BAOSTOCK_MAPPER_VERSION,
            canonical_schema=CANONICAL_SCHEMA_VERSION,
            quality_rules=QUALITY_RULE_SET_VERSION,
            snapshot_manifest=SNAPSHOT_MANIFEST_VERSION,
        ),
    )
    return ApplicationServices(pipeline, repository)


app = create_app(build_default_services)
