from __future__ import annotations

from datetime import date
from math import log

import polars as pl
import pytest

from quant_research.data.canonical.adjustments import (
    FORWARD_LOG_RETURN_COLUMN,
    _PriceAdjustmentSupport,
)


def test_ex_right_return_uses_previous_session_adjustment_factor() -> None:
    bars = pl.DataFrame(
        {
            "instrument_id": ["600000.SH", "600000.SH"],
            "trade_date": [date(2026, 8, 24), date(2026, 8, 25)],
            "open": [10.0, 5.0],
            "high": [10.0, 5.0],
            "low": [10.0, 5.0],
            "close": [10.0, 5.0],
            "preclose": [10.0, 10.0],
            "change": [0.0, -5.0],
            "pct_change": [0.0, -0.5],
            "volume": [100, 100],
            "amount": [1000.0, 500.0],
        }
    )
    factors = pl.DataFrame(
        {
            "instrument_id": ["600000.SH", "600000.SH"],
            "trade_date": [date(2026, 8, 24), date(2026, 8, 25)],
            "adjustment_factor": [1.0, 2.0],
        }
    )
    adjusted, _ = _PriceAdjustmentSupport._factor_adjust(
        bars, factors, date(2026, 8, 25)
    )
    ex_right = adjusted.row(1, named=True)
    assert ex_right["close"] == pytest.approx(5.0)
    assert ex_right["preclose"] == pytest.approx(5.0)
    assert ex_right["pct_change"] == pytest.approx(0.0)
    assert ex_right[FORWARD_LOG_RETURN_COLUMN] == pytest.approx(log(5.0) - log(5.0))
