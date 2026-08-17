"""Provider session ownership for dataset localization."""

import json
from datetime import UTC, date, datetime
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch
from uuid import uuid4

from quant_research.data.contracts import RawBatch
from quant_research.data.pipelines.dataset import (
    DataPipeline,
    DataUpdatePlan,
    DataUpdateWindow,
    DataUpdateWindowBasis,
    LocalizeResult,
)
from quant_research.domain.enums import DatasetKind
from quant_research.domain.identifiers import QualityRunId
from quant_research.infrastructure.baostock.routing import BAOSTOCK_ROUTES
from quant_research.logging import StructuredLogger


class _Source:
    provider = "baostock"

    def __init__(self) -> None:
        self.login_calls = 0
        self.close_calls = 0

    def login(self) -> None:
        self.login_calls += 1

    def close(self) -> None:
        self.close_calls += 1

    def fetch_instruments(self) -> tuple[RawBatch, ...]:
        return ()


class _Calendar:
    def explicit_window(self, start: date, end: date) -> tuple[date, date]:
        return start, end


class _Repository:
    def record_dataset_stage(self, *_: object, **__: object) -> None:
        """接收测试不关心的运营阶段证据。"""

    def catalog_state(self) -> Any:
        """返回执行结果所需的稳定目录身份。"""
        return SimpleNamespace(catalog_hash="a" * 64)


class _Observer:
    def __init__(self) -> None:
        self.stages: list[tuple[str, int]] = []

    def stage_started(self, stage: str, total: int) -> None:
        self.stages.append((stage, total))

    def dataset_completed(self, *_: object, **__: object) -> None:
        """接收测试不关心的单数据集进度。"""

    def boundary(self, *_: object, **__: object) -> None:
        """接收测试不关心的安全取消边界。"""

    def is_cancelled(self) -> bool:
        return False


def _partial_plan() -> DataUpdatePlan:
    windows = tuple(
        DataUpdateWindow(
            dataset=dataset,
            basis=DataUpdateWindowBasis.INCREMENTAL,
            start=date(2026, 8, 10),
            end=date(2026, 8, 14),
            overlap_days=4,
            current_watermark=date(2026, 8, 13),
        )
        for dataset in (DatasetKind.DAILY_BAR, DatasetKind.DAILY_BASIC)
    )
    return DataUpdatePlan(
        window_mode="AUTO_INCREMENTAL",
        planned_at=datetime(2026, 8, 15, tzinfo=UTC),
        start=date(2026, 8, 10),
        end=date(2026, 8, 14),
        dataset_windows=windows,
    )


def _pipeline(
    source: _Source,
    data_root: Path,
    logger: StructuredLogger | None = None,
) -> DataPipeline:
    return DataPipeline(
        source=source,  # type: ignore[arg-type]
        mapper=object(),  # type: ignore[arg-type]
        calendar=_Calendar(),  # type: ignore[arg-type]
        raw_store=SimpleNamespace(root=data_root / "raw"),  # type: ignore[arg-type]
        curated_store=SimpleNamespace(  # type: ignore[arg-type]
            root=data_root / "canonical"
        ),
        repository=_Repository(),  # type: ignore[arg-type]
        quality_runner=object(),  # type: ignore[arg-type]
        routes=BAOSTOCK_ROUTES,
        logger=logger,
    )


def test_localize_reuses_an_active_outer_source_session(tmp_path: Path) -> None:
    source = _Source()
    pipeline = _pipeline(source, tmp_path)
    pipeline._source_session_active = True

    result = pipeline.localize(
        DatasetKind.INSTRUMENT,
        start=date(2026, 8, 11),
        end=date(2026, 8, 11),
    )

    assert result == LocalizeResult(DatasetKind.INSTRUMENT, 0, 0, 0)
    assert source.login_calls == 0
    assert source.close_calls == 0


def test_localize_all_opens_one_session_around_every_dataset(tmp_path: Path) -> None:
    source = _Source()
    pipeline = _pipeline(source, tmp_path)
    observed: list[DatasetKind] = []

    def localized(
        instance: DataPipeline,
        dataset: DatasetKind,
        *,
        start: date | None = None,
        end: date | None = None,
        full: bool = False,
        observer: object | None = None,
    ) -> LocalizeResult:
        assert observer is not None
        assert instance._source_session_active
        assert start is None and end is None and full is False
        observed.append(dataset)
        return LocalizeResult(dataset, 0, 1, 1)

    with patch.object(DataPipeline, "localize", autospec=True, side_effect=localized):
        results = pipeline.localize_all()

    assert len(results) == len(observed) == 8
    assert source.login_calls == 1
    assert source.close_calls == 1
    assert pipeline._source_session_active is False


def test_localize_plan_only_visits_the_frozen_dataset_subset(tmp_path: Path) -> None:
    source = _Source()
    pipeline = _pipeline(source, tmp_path)
    plan = _partial_plan()
    observed: list[tuple[DatasetKind, tuple[date, date] | None]] = []

    def localized(
        instance: DataPipeline,
        dataset: DatasetKind,
        *,
        start: date | None = None,
        end: date | None = None,
        full: bool = False,
        planned_window: tuple[date, date] | None = None,
        observer: object | None = None,
    ) -> LocalizeResult:
        assert observer is not None
        assert instance._source_session_active
        assert start is None and end is None and full is False
        observed.append((dataset, planned_window))
        return LocalizeResult(dataset, 0, 1, 1)

    with patch.object(DataPipeline, "localize", autospec=True, side_effect=localized):
        results = pipeline.localize_all(plan=plan)

    assert tuple(item.dataset for item in results) == (
        DatasetKind.DAILY_BAR,
        DatasetKind.DAILY_BASIC,
    )
    assert observed == [
        (DatasetKind.DAILY_BAR, (date(2026, 8, 10), date(2026, 8, 14))),
        (DatasetKind.DAILY_BASIC, (date(2026, 8, 10), date(2026, 8, 14))),
    ]
    assert source.login_calls == source.close_calls == 1


def test_execute_partial_plan_curates_selection_then_validates_full_catalog(
    tmp_path: Path,
) -> None:
    pipeline = _pipeline(_Source(), tmp_path)
    plan = _partial_plan()
    observer = _Observer()
    quality_run_id = QualityRunId(uuid4())

    with (
        patch.object(
            DataPipeline, "localize_all", autospec=True, return_value=()
        ) as localize_all,
        patch.object(
            DataPipeline, "_curate_many", autospec=True, return_value=()
        ) as curate_many,
        patch.object(
            DataPipeline,
            "validate",
            autospec=True,
            return_value=quality_run_id,
        ) as validate,
    ):
        result = pipeline.execute_update_plan(plan, observer=observer)

    localize_all.assert_called_once_with(pipeline, plan=plan, observer=observer)
    curate_many.assert_called_once_with(
        pipeline,
        (DatasetKind.DAILY_BAR, DatasetKind.DAILY_BASIC),
        full=False,
        observer=observer,
    )
    validate.assert_called_once_with(pipeline)
    assert ("VALIDATE", len(tuple(BAOSTOCK_ROUTES))) in observer.stages
    assert result.quality_run_id == quality_run_id


def test_pipeline_stages_use_independent_business_log_contexts(
    tmp_path: Path,
) -> None:
    stream = StringIO()
    pipeline = _pipeline(_Source(), tmp_path, StructuredLogger(stream))

    pipeline._localize_log(
        "localize.raw_completed",
        request={"api": "query_trade_dates", "from": "2026-08-11"},
        source="baostock",
        endpoint="query_trade_dates",
        disposition="fetched",
        request_hash="a" * 64,
        content_hash="b" * 64,
        row_count=1,
    )
    pipeline._curate_log(
        "curate.partition_completed",
        dataset="trade_calendar",
        partition={
            "partition_key": "all",
            "content_hash": "c" * 64,
            "schema_fingerprint": "d" * 64,
            "row_count": 1,
            "path": "canonical/dataset=trade_calendar/all/c.parquet",
        },
    )
    pipeline._validate_log(
        "validate.issue_detected",
        scope="ALL",
        catalog_hash="e" * 64,
        issue={
            "rule_id": "primary_key",
            "dataset": "trade_calendar",
            "actual": 1,
            "threshold": 0,
        },
    )

    localize, curate, validate = [
        json.loads(line) for line in stream.getvalue().splitlines()
    ]
    assert localize["stage"] == "LOCALIZE"
    assert "task_id" not in localize
    assert "worker_id" not in localize
    assert localize["context"]["request"]["api"] == "query_trade_dates"
    assert curate["stage"] == "CURATE"
    assert "request" not in curate["context"]
    assert curate["context"]["partition"]["partition_key"] == "all"
    assert validate["stage"] == "VALIDATE"
    assert "request" not in validate["context"]
    assert validate["context"]["scope"] == "ALL"
    assert validate["context"]["issue"]["rule_id"] == "primary_key"
