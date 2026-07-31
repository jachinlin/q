"""Backward-adjusted ETF market risk factors."""

from __future__ import annotations

from collections.abc import Sequence
from itertools import pairwise
from math import isfinite, log, sqrt

from quant_core.data.adjustments import AdjustmentMode
from quant_core.domain.identifiers import InstrumentId
from quant_core.factors.base import FactorSpec
from quant_core.factors.builtin.momentum import AdjustedBarService, _MarketFactor

_VERSION = "1.0.0"


class Volatility60dFactor(_MarketFactor):
    """Annualized sample volatility of the latest 60 daily log returns."""

    def __init__(
        self,
        price_service: AdjustedBarService,
        instruments: Sequence[InstrumentId],
    ) -> None:
        super().__init__(
            price_service,
            instruments,
            FactorSpec(
                factor_id="volatility_60d_v1",
                version=_VERSION,
                frequency="daily",
                lookback_sessions=60,
                dependencies=(),
                direction=-1,
                parameters={
                    "adjustment_mode": AdjustmentMode.BACKWARD.value,
                    "annualization_sessions": 252,
                    "ddof": 1,
                    "formula": "std(log(close[t]/close[t-1]),ddof=1)*sqrt(252)",
                    "price_field": "close",
                    "window_prices": 61,
                    "window_returns": 60,
                },
            ),
            required_prices=61,
            evaluator=_volatility_value,
        )


def _volatility_value(closes: Sequence[float]) -> float | None:
    returns = [log(current / previous) for previous, current in pairwise(closes)]
    count = len(returns)
    mean = sum(returns) / count
    variance = sum((value - mean) ** 2 for value in returns) / (count - 1)
    result = sqrt(variance) * sqrt(252.0)
    return result if isfinite(result) else None
