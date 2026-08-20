"""数据更新计划的确定性解析与持久化契约。"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from quant_research.data.catalog import DATASET_CATALOG
from quant_research.data.pipeline.publish import (
    DataUpdatePlan,
    DataUpdatePlanner,
    DataUpdateWindowBasis,
)
from quant_research.domain.enums import DatasetKind
from quant_research.infrastructure.baostock.routing import BAOSTOCK_ROUTES
from quant_research.infrastructure.persistence.repositories import (
    CanonicalDatasetRecord,
)


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


def _planner(
    repository: _Repository, planned_at: datetime = datetime(2026, 8, 15, tzinfo=UTC)
) -> DataUpdatePlanner:
    return DataUpdatePlanner(
        calendar=_Calendar(),
        repository=repository,
        routes=BAOSTOCK_ROUTES,
        clock=lambda: planned_at,
    )


def test_auto_plan_freezes_each_dataset_window_without_null_parameters() -> None:
    plan = _planner(_Repository(frozenset({DatasetKind.INSTRUMENT}))).plan(
        start=None, end=None
    )

    assert plan.window_mode == "AUTO_INCREMENTAL"
    assert plan.start == date(2006, 8, 14)
    assert plan.end == date(2026, 11, 12)
    assert tuple(item.dataset.value for item in plan.dataset_windows) == tuple(
        sorted(item.value for item in DATASET_CATALOG if BAOSTOCK_ROUTES[item])
    )
    instrument = next(
        item for item in plan.dataset_windows if item.dataset is DatasetKind.INSTRUMENT
    )
    calendar = next(
        item
        for item in plan.dataset_windows
        if item.dataset is DatasetKind.TRADE_CALENDAR
    )
    assert instrument.basis is DataUpdateWindowBasis.BOOTSTRAP
    assert instrument.current_watermark is None
    assert calendar.basis is DataUpdateWindowBasis.INCREMENTAL
    assert calendar.end == date(2026, 8, 14) + timedelta(days=90)

    payload = plan.to_payload()
    assert payload["start"] == "2006-08-14"
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


def test_partial_auto_plan_is_sorted_and_ignores_unselected_missing_watermarks() -> (
    None
):
    repository = _Repository(frozenset({DatasetKind.INSTRUMENT}))
    plan = _planner(repository).plan(
        start=None,
        end=None,
        datasets=(DatasetKind.DAILY_BASIC, DatasetKind.DAILY_BAR),
    )

    assert tuple(item.dataset for item in plan.dataset_windows) == (
        DatasetKind.DAILY_BAR,
        DatasetKind.DAILY_BASIC,
    )
    assert all(
        item.basis is DataUpdateWindowBasis.INCREMENTAL for item in plan.dataset_windows
    )
    assert repository.requested == [DatasetKind.DAILY_BAR, DatasetKind.DAILY_BASIC]
    full = _planner(_Repository()).plan(start=None, end=None)
    assert plan.plan_hash != full.plan_hash


def test_partial_plan_rejects_empty_and_duplicate_dataset_selections() -> None:
    planner = _planner(_Repository())

    for datasets in ((), (DatasetKind.DAILY_BAR, DatasetKind.DAILY_BAR)):
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
        item.basis is DataUpdateWindowBasis.EXPLICIT for item in plan.dataset_windows
    )
    assert DataUpdatePlan.from_payload(plan.to_payload()).plan_hash == plan.plan_hash
