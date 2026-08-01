"""Forward-adjusted ETF market risk factors."""

from __future__ import annotations

from collections.abc import Sequence
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
                    "adjustment_mode": AdjustmentMode.FORWARD.value,
                    "annualization_sessions": 252,
                    "ddof": 1,
                    "formula": "std(log(close[t])-log(close[t-1]),ddof=1)*sqrt(252)",
                    "price_field": "close",
                    "window_prices": 61,
                    "window_returns": 60,
                },
            ),
            required_prices=61,
            evaluator=_volatility_value,
        )


class DownsideVolatility60dFactor(_MarketFactor):
    """Annualized root-mean-square of negative log returns."""

    def __init__(
        self, price_service: AdjustedBarService, instruments: Sequence[InstrumentId]
    ) -> None:
        super().__init__(
            price_service,
            instruments,
            FactorSpec(
                factor_id="downside_volatility_60d_v1",
                version=_VERSION,
                frequency="daily",
                lookback_sessions=60,
                dependencies=(),
                direction=-1,
                parameters={
                    "adjustment_mode": AdjustmentMode.FORWARD.value,
                    "annualization_sessions": 252,
                    "eligible_for_alpha": True,
                    "formula": "sqrt(mean(min(log_return,0)^2))*sqrt(252)",
                    "window_prices": 61,
                },
            ),
            required_prices=61,
            evaluator=_downside_volatility_value,
        )


class MaxDrawdown120dFactor(_MarketFactor):
    """Largest peak-to-later-close loss in the latest 120 prices."""

    def __init__(
        self, price_service: AdjustedBarService, instruments: Sequence[InstrumentId]
    ) -> None:
        super().__init__(
            price_service,
            instruments,
            FactorSpec(
                factor_id="max_drawdown_120d_v1",
                version=_VERSION,
                frequency="daily",
                lookback_sessions=119,
                dependencies=(),
                direction=-1,
                parameters={
                    "adjustment_mode": AdjustmentMode.FORWARD.value,
                    "eligible_for_alpha": True,
                    "formula": "max(1-close/running_peak)",
                    "window_prices": 120,
                },
            ),
            required_prices=120,
            evaluator=_max_drawdown_value,
        )


def _volatility_value(closes: Sequence[float]) -> float | None:
    log_prices = [log(close) for close in closes]
    returns = [
        log_prices[index] - log_prices[index - 1] for index in range(1, len(log_prices))
    ]
    count = len(returns)
    mean = sum(returns) / count
    variance = sum((value - mean) ** 2 for value in returns) / (count - 1)
    result = sqrt(variance) * sqrt(252.0)
    return result if isfinite(result) else None


def _downside_volatility_value(closes: Sequence[float]) -> float | None:
    log_prices = [log(close) for close in closes]
    returns = [
        log_prices[index] - log_prices[index - 1] for index in range(1, len(log_prices))
    ]
    result = sqrt(sum(min(value, 0.0) ** 2 for value in returns) / len(returns)) * sqrt(
        252.0
    )
    return result if isfinite(result) else None


def _max_drawdown_value(closes: Sequence[float]) -> float | None:
    peak = closes[0]
    drawdown = 0.0
    for close in closes:
        peak = max(peak, close)
        drawdown = max(drawdown, 1.0 - close / peak)
    return drawdown if isfinite(drawdown) else None
