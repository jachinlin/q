"""Behavioral coverage for immutable snapshot-bound research reads."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from quant_core.data.repository import (
    SnapshotDatasetMissing,
    SnapshotResearchRepository,
)
from quant_core.data.schemas import CANONICAL_SCHEMAS
from quant_core.domain.enums import DatasetKind
from quant_core.domain.identifiers import InstrumentId
from tests.fixtures.point_in_time import point_in_time_fixture


def test_financials_do_not_cross_snapshot_membership(tmp_path: Path) -> None:
    """Changing the late financial version must not change an earlier snapshot read."""
    fixture = point_in_time_fixture(tmp_path)
    repository = SnapshotResearchRepository(fixture.repository)

    result = repository.financials_as_of(
        fixture.early_snapshot_id,
        ["revenue"],
        date(2024, 4, 29),
    ).collect()

    assert result["value"].to_list() == [100.0]
    assert result["revision"].to_list() == [0]


def test_financials_choose_latest_available_revision_before_shanghai_close(
    tmp_path: Path,
) -> None:
    """Dropping the availability cutoff would incorrectly expose revision 2 early."""
    fixture = point_in_time_fixture(tmp_path)
    repository = SnapshotResearchRepository(fixture.repository)

    result = repository.financials_as_of(
        fixture.late_snapshot_id,
        ["revenue"],
        date(2024, 4, 29),
    ).collect()

    assert result["value"].to_list() == [120.0]
    assert result["revision"].to_list() == [1]
    assert result.schema == CANONICAL_SCHEMAS[DatasetKind.FINANCIAL_OBSERVATION].columns


def test_bars_are_snapshot_bound_range_reads_with_canonical_sort(
    tmp_path: Path,
) -> None:
    """Removing the ordered range filter would reorder or leak bar observations."""
    fixture = point_in_time_fixture(tmp_path)
    repository = SnapshotResearchRepository(fixture.repository)

    result = repository.bars(
        fixture.early_snapshot_id,
        [InstrumentId.parse("SSE:600000")],
        date(2024, 4, 28),
        date(2024, 4, 29),
    ).collect()

    assert result["trade_date"].to_list() == [date(2024, 4, 28), date(2024, 4, 29)]
    assert result.schema == CANONICAL_SCHEMAS[DatasetKind.DAILY_BAR].columns


def test_security_status_filters_the_requested_as_of_date(tmp_path: Path) -> None:
    """Treating status as unbounded history would return observations from other dates."""
    fixture = point_in_time_fixture(tmp_path)
    repository = SnapshotResearchRepository(fixture.repository)

    result = repository.security_status(
        fixture.early_snapshot_id,
        date(2024, 4, 29),
        [InstrumentId.parse("SSE:600000")],
    ).collect()

    assert result.select("is_listed", "is_suspended").rows() == [(True, False)]
    assert result.schema == CANONICAL_SCHEMAS[DatasetKind.SECURITY_STATUS].columns


def test_missing_snapshot_dataset_has_a_stable_structured_contract(
    tmp_path: Path,
) -> None:
    """A missing dataset must not be mistaken for an empty, mutable latest dataset."""
    fixture = point_in_time_fixture(tmp_path)
    repository = SnapshotResearchRepository(fixture.repository)
    missing_snapshot_id = fixture.repository.snapshot_without_dataset(
        fixture.early_snapshot_id, "daily_bar"
    )

    with pytest.raises(SnapshotDatasetMissing) as captured:
        repository.bars(
            missing_snapshot_id,
            [InstrumentId.parse("SSE:600000")],
            date(2024, 4, 28),
            date(2024, 4, 29),
        )

    assert captured.value.detail.code == "SNAPSHOT_DATASET_MISSING"
    assert captured.value.detail.context == {
        "dataset": "daily_bar",
        "snapshot_id": str(missing_snapshot_id),
    }
