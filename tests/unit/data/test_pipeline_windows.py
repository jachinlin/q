from datetime import date

from quant_research.data.pipeline.dataset import _DatasetPipelineSupport
from quant_research.domain.enums import DatasetKind


def test_trade_calendar_window_extends_ninety_calendar_days() -> None:
    assert _DatasetPipelineSupport._calendar_horizon(
        DatasetKind.TRADE_CALENDAR,
        (date(2026, 8, 11), date(2026, 8, 11)),
    ) == (date(2026, 8, 11), date(2026, 11, 9))


def test_non_calendar_window_is_not_extended() -> None:
    window = date(2026, 8, 1), date(2026, 8, 11)

    assert (
        _DatasetPipelineSupport._calendar_horizon(DatasetKind.STOCK_DAILY_BAR, window)
        == window
    )


def test_event_partition_from_an_older_announcement_year_is_selected() -> None:
    assert _DatasetPipelineSupport._partition_selected(
        DatasetKind.STOCK_DIVIDEND,
        "announcement_year=2025",
        date(2026, 1, 1),
        date(2026, 1, 8),
    )


def test_non_event_partition_outside_the_window_is_not_selected() -> None:
    assert not _DatasetPipelineSupport._partition_selected(
        DatasetKind.STOCK_DAILY_BAR,
        "year=2025",
        date(2026, 1, 1),
        date(2026, 1, 8),
    )
