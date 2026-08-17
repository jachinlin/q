from datetime import date

from quant_research.data.pipelines.dataset import _DatasetPipelineSupport
from quant_research.domain.enums import DatasetKind


def test_trade_calendar_window_extends_ninety_calendar_days() -> None:
    assert _DatasetPipelineSupport._calendar_horizon(
        DatasetKind.TRADE_CALENDAR,
        (date(2026, 8, 11), date(2026, 8, 11)),
    ) == (date(2026, 8, 11), date(2026, 11, 9))


def test_non_calendar_window_is_not_extended() -> None:
    window = date(2026, 8, 1), date(2026, 8, 11)

    assert (
        _DatasetPipelineSupport._calendar_horizon(DatasetKind.DAILY_BAR, window)
        == window
    )
