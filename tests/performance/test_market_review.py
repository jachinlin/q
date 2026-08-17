"""市场全景十万行级横截面聚合性能门槛。"""

from __future__ import annotations

from datetime import date, timedelta
from time import perf_counter

import pytest

from quant_research.dashboard.market_review import MarketReviewService


@pytest.mark.performance
def test_market_review_core_aggregation_finishes_within_three_seconds() -> None:
    sessions = tuple(date(2026, 1, 1) + timedelta(days=index) for index in range(21))
    rows_per_session = 5_000
    market_rows = {
        session: tuple(
            {
                "instrument_id": f"{index:06d}.SH",
                "amount": float(1_000_000 + index),
            }
            for index in range(rows_per_session)
        )
        for session in sessions
    }
    returns = tuple(((index % 201) - 100) / 10_000 for index in range(rows_per_session))

    started = perf_counter()
    liquidity = MarketReviewService._liquidity(sessions, market_rows)
    breadth = MarketReviewService._breadth(returns)
    elapsed = perf_counter() - started

    assert sum(item.count for item in breadth.buckets) == rows_per_session
    assert liquidity.amount > 0.0
    assert elapsed < 3.0
