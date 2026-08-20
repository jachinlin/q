"""验证三类信号行使用互斥语义和稳定主键。"""

from datetime import UTC, date, datetime

import pytest

from quant_research.signals.models import (
    AllocationSignalRow,
    CrossSectionalScoreRow,
    Direction,
    DirectionalSignalRow,
)


def test_signal_rows_accept_their_declared_semantics() -> None:
    available = datetime(2024, 1, 2, 8, tzinfo=UTC)
    cross = CrossSectionalScoreRow(date(2024, 1, 2), "600000.SH", "alpha", 1.2, 0.8, available, True, None)
    directional = DirectionalSignalRow(date(2024, 1, 2), "510300.SH", "ma", Direction.LONG, 1.0, True, available, True, None)
    allocation = AllocationSignalRow(date(2024, 1, 2), "510300.SH", "rotation", 0.5, available, True, None)
    assert cross.score == 1.2
    assert directional.direction is Direction.LONG
    assert allocation.desired_exposure == 0.5


def test_invalid_signal_requires_reason_and_no_value() -> None:
    with pytest.raises(ValueError, match="valid cross-sectional"):
        CrossSectionalScoreRow(
            date(2024, 1, 2),
            "600000.SH",
            "alpha",
            1.0,
            0.0,
            datetime(2024, 1, 2, tzinfo=UTC),
            False,
            "MISSING_INPUT",
        )
