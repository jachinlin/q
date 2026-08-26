"""数据更新计划的确定性解析与持久化契约。"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest

from quant_research.data.catalog import DATASET_CATALOG
from quant_research.data.contracts import CanonicalBatch
from quant_research.data.pipeline.publish import (
    DataPipeline,
    DataUpdatePlan,
    DataUpdatePlanner,
    DataUpdateWindowBasis,
)
from quant_research.domain.enums import DatasetKind
from quant_research.domain.errors import QuantError
from quant_research.infrastructure.persistence.repositories import (
    CanonicalDatasetRecord,
)
from quant_research.infrastructure.tushare.routing import TUSHARE_ROUTES


class _Calendar:
    def bootstrap_window(self, years: int) -> tuple[date, date]:
        assert years == 20
        return date(2006, 8, 14), date(2026, 8, 14)

    def latest_complete_day(self) -> date:
        return date(2026, 8, 14)

    def explicit_window(self, start: date, end: date) -> tuple[date, date]:
        assert start <= end
        return start + timedelta(days=1), end - timedelta(days=1)


class _Repository:
    def __init__(self, missing: frozenset[DatasetKind] = frozenset()) -> None:
        self._missing = missing
        self.requested: list[DatasetKind] = []

    def find_canonical_dataset(
        self, dataset: DatasetKind
    ) -> CanonicalDatasetRecord | None:
        self.requested.append(dataset)
        if dataset in self._missing:
            return None
        return CanonicalDatasetRecord(
            dataset=dataset,
            content_hash="a" * 64,
            source="baostock",
            partitions=(),
            start_date=date(2020, 1, 1),
            end_date=date(2026, 8, 10),
            updated_at=datetime(2026, 8, 11, tzinfo=UTC),
        )

    def find_data_initialization(self) -> None:
        return None


def _planner(
    repository: _Repository, planned_at: datetime = datetime(2026, 8, 15, tzinfo=UTC)
) -> DataUpdatePlanner:
    return DataUpdatePlanner(
        calendar=_Calendar(),
        repository=repository,
        routes=TUSHARE_ROUTES,
        clock=lambda: planned_at,
    )


def test_auto_plan_freezes_each_dataset_window_without_null_parameters() -> None:
    plan = _planner(_Repository()).plan(start=None, end=None)

    assert plan.window_mode == "AUTO_INCREMENTAL"
    assert plan.start == date(2026, 7, 11)
    assert plan.end == date(2026, 11, 12)
    expected = tuple(
        sorted(
            item.value
            for item in DATASET_CATALOG
            if TUSHARE_ROUTES[item]
        )
    )
    assert tuple(item.dataset.value for item in plan.dataset_windows) == expected
    assert tuple(item.dataset for item in plan.skipped_datasets) == (
        DatasetKind.STOCK_FINANCIAL_INDICATOR,
    )
    assert plan.skipped_datasets[0].trigger_date == date(2026, 8, 31)
    instrument = next(
        item for item in plan.dataset_windows if item.dataset is DatasetKind.STOCK_MASTER
    )
    calendar = next(
        item
        for item in plan.dataset_windows
        if item.dataset is DatasetKind.TRADE_CALENDAR
    )
    assert instrument.basis is DataUpdateWindowBasis.SNAPSHOT_REFRESH
    assert instrument.start == instrument.end == date(2026, 8, 15)
    assert instrument.current_watermark is None
    assert calendar.basis is DataUpdateWindowBasis.INCREMENTAL
    assert calendar.end == date(2026, 8, 14) + timedelta(days=90)

    payload = plan.to_payload()
    assert payload["start"] == "2026-07-11"
    assert payload["end"] == "2026-11-12"
    assert "requested_start" not in payload
    assert "requested_end" not in payload
    assert "null" not in str(payload).lower()
    assert DataUpdatePlan.from_payload(payload) == plan


def test_plan_hash_excludes_generation_time_but_covers_resolved_windows() -> None:
    first = _planner(_Repository()).plan(start=None, end=None)
    second = _planner(_Repository(), datetime(2026, 8, 15, 1, tzinfo=UTC)).plan(
        start=None, end=None
    )

    assert first.planned_at != second.planned_at
    assert first.plan_hash == second.plan_hash


def test_partial_auto_plan_rejects_an_incomplete_global_baseline() -> None:
    repository = _Repository(frozenset({DatasetKind.STOCK_MASTER}))
    with pytest.raises(QuantError) as captured:
        _planner(repository).plan(
            start=None,
            end=None,
            datasets=(DatasetKind.STOCK_DAILY_BASIC, DatasetKind.STOCK_DAILY_BAR),
        )

    assert captured.value.detail.code == "DATA_UPDATE_REQUIRES_BOOTSTRAP"
    assert DatasetKind.STOCK_MASTER in repository.requested


def test_bootstrap_plan_uses_required_years_and_freezes_base_window() -> None:
    plan = _planner(_Repository()).plan_bootstrap(20)

    assert plan.window_mode == "BOOTSTRAP"
    assert plan.requested_start == date(2006, 8, 14)
    assert plan.requested_end == date(2026, 8, 14)
    assert all(
        item.basis is DataUpdateWindowBasis.BOOTSTRAP
        for item in plan.dataset_windows
        if item.dataset is not DatasetKind.STOCK_MASTER
    )
    assert DataUpdatePlan.from_payload(plan.to_payload()) == plan


def test_partial_plan_rejects_empty_and_duplicate_dataset_selections() -> None:
    planner = _planner(_Repository())

    for datasets in ((), (DatasetKind.STOCK_DAILY_BAR, DatasetKind.STOCK_DAILY_BAR)):
        try:
            planner.plan(start=None, end=None, datasets=datasets)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid dataset selection was accepted")


def test_explicit_plan_records_request_and_normalized_dataset_windows() -> None:
    plan = _planner(_Repository()).plan(start=date(2026, 8, 1), end=date(2026, 8, 15))

    assert plan.window_mode == "EXPLICIT"
    assert plan.requested_start == date(2026, 8, 1)
    assert plan.requested_end == date(2026, 8, 15)
    assert plan.start == date(2026, 8, 2)
    assert plan.end == date(2026, 11, 12)
    assert all(
        item.basis is DataUpdateWindowBasis.EXPLICIT
        for item in plan.dataset_windows
        if item.dataset is not DatasetKind.STOCK_MASTER
    )
    instrument = next(
        item for item in plan.dataset_windows if item.dataset is DatasetKind.STOCK_MASTER
    )
    assert instrument.basis is DataUpdateWindowBasis.SNAPSHOT_REFRESH
    assert instrument.current_watermark is None
    assert DataUpdatePlan.from_payload(plan.to_payload()).plan_hash == plan.plan_hash


def test_financial_auto_plan_uses_disclosure_batch_without_watermark() -> None:
    plan = _planner(
        _Repository(),
        datetime(2026, 9, 1, tzinfo=UTC),
    ).plan(
        start=None,
        end=None,
        datasets=(DatasetKind.STOCK_FINANCIAL_INDICATOR,),
    )

    assert plan.skipped_datasets == ()
    assert len(plan.dataset_windows) == 1
    window = plan.dataset_windows[0]
    assert window.basis is DataUpdateWindowBasis.DISCLOSURE_TRIGGER
    assert window.start == window.end == date(2026, 6, 30)
    assert window.trigger_date == date(2026, 8, 31)
    assert window.current_watermark is None
    assert window.overlap_days == 0


def test_financial_auto_plan_is_no_op_until_deadline_is_strictly_passed() -> None:
    plan = _planner(
        _Repository(),
        datetime(2026, 8, 31, tzinfo=UTC),
    ).plan(
        start=None,
        end=None,
        datasets=(DatasetKind.STOCK_FINANCIAL_INDICATOR,),
    )

    assert plan.dataset_windows == ()
    assert plan.start == plan.end == date(2026, 8, 31)
    assert plan.skipped_datasets[0].reason == "DISCLOSURE_DEADLINE_PENDING"
    assert DataUpdatePlan.from_payload(plan.to_payload()) == plan


def test_instrument_snapshot_ignores_future_canonical_list_date_watermark() -> None:
    plan = _planner(
        _Repository(),
        datetime(2026, 8, 21, tzinfo=UTC),
    ).plan(
        start=None,
        end=None,
        datasets=(DatasetKind.STOCK_MASTER,),
    )

    window = plan.dataset_windows[0]
    assert window.basis is DataUpdateWindowBasis.SNAPSHOT_REFRESH
    assert window.start == window.end == date(2026, 8, 21)
    assert window.current_watermark is None
    assert window.overlap_days == 0


def test_instrument_canonical_window_uses_snapshot_time_not_listing_lifecycle() -> None:
    frame = pl.DataFrame(
        {
            "list_date": [date(1990, 12, 10), date(2026, 8, 24)],
            "ingested_at": [
                datetime(2026, 8, 20, 12, 36, tzinfo=UTC),
                datetime(2026, 8, 20, 12, 36, tzinfo=UTC),
            ],
        }
    )
    batch = CanonicalBatch(
        DatasetKind.STOCK_MASTER,
        frame,
        ("a" * 64,),
    )

    assert DataPipeline._batch_window((batch,), None, None) == (
        date(2026, 8, 20),
        date(2026, 8, 20),
    )
