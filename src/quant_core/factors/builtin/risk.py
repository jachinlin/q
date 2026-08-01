"""Row-log-return market risk factors."""

from __future__ import annotations

from collections.abc import Sequence
from math import expm1, isfinite, sqrt

from quant_core.data.adjustments import FORWARD_LOG_RETURN_COLUMN, AdjustmentMode
from quant_core.domain.identifiers import InstrumentId
from quant_core.factors.base import FactorSpec
from quant_core.factors.builtin.momentum import AdjustedBarService, _MarketFactor

_VERSION = "2.0.0"
_PRICE_BASIS = "baostock_forward_log_return_v1"
_PATH_CONSTRUCTION = "window_forward_cumsum_v1"


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
                    "formula": "std(forward_log_return[1:61],ddof=1)*sqrt(252)",
                    "path_construction": _PATH_CONSTRUCTION,
                    "price_basis": _PRICE_BASIS,
                    "price_field": FORWARD_LOG_RETURN_COLUMN,
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
                    "formula": (
                        "sqrt(mean(min(forward_log_return[1:61],0)^2))*sqrt(252)"
                    ),
                    "path_construction": _PATH_CONSTRUCTION,
                    "price_basis": _PRICE_BASIS,
                    "price_field": FORWARD_LOG_RETURN_COLUMN,
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
                    "formula": ("max(1-exp(relative_log_path-running_peak_log))"),
                    "path_construction": _PATH_CONSTRUCTION,
                    "price_basis": _PRICE_BASIS,
                    "price_field": FORWARD_LOG_RETURN_COLUMN,
                    "window_prices": 120,
                },
            ),
            required_prices=120,
            evaluator=_max_drawdown_value,
        )


def _volatility_value(
    relative_log_path: Sequence[float], log_returns: Sequence[float]
) -> float | None:
    del relative_log_path
    count = len(log_returns)
    mean = sum(log_returns) / count
    variance = sum((value - mean) ** 2 for value in log_returns) / (count - 1)
    result = sqrt(variance) * sqrt(252.0)
    return result if isfinite(result) else None


def _downside_volatility_value(
    relative_log_path: Sequence[float], log_returns: Sequence[float]
) -> float | None:
    del relative_log_path
    result = sqrt(
        sum(min(value, 0.0) ** 2 for value in log_returns) / len(log_returns)
    ) * sqrt(252.0)
    return result if isfinite(result) else None


def _max_drawdown_value(
    relative_log_path: Sequence[float], log_returns: Sequence[float]
) -> float | None:
    del log_returns
    peak = relative_log_path[0]
    drawdown = 0.0
    for log_price in relative_log_path:
        peak = max(peak, log_price)
        drawdown = max(drawdown, -expm1(log_price - peak))
    return drawdown if isfinite(drawdown) else None
