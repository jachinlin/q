"""验证目标架构策略契约、双均线状态和组件目录。"""

from datetime import date

import polars as pl
import pytest

from quant_research.domain.identifiers import InstrumentId
from quant_research.portfolio.rebalance import RebalancePlanner
from quant_research.strategies.base import AccountView, DecisionContext, OrderSide
from quant_research.strategies.components import (
    StrategyComponentCatalog,
    StrategyPipelineConfig,
)
from quant_research.strategies.cross_sectional import CrossSectionalPortfolioAssembler
from quant_research.strategies.dual_ma import DualMAConfig, DualMATrendStrategy
from quant_research.strategies.multifactor import MultifactorConfig, MultifactorStrategy
from quant_research.strategies.registry import StrategyRegistry


class _Data:
    def __init__(self, signal_date: date, prices: list[float]) -> None:
        self.signal_date = signal_date
        self._prices = prices

    def adjusted_bars(
        self, instruments: object, lookback_sessions: int
    ) -> pl.LazyFrame:
        del instruments
        assert lookback_sessions == len(self._prices)
        days = [date(2024, 1, index + 2) for index in range(len(self._prices))]
        days[-1] = self.signal_date
        return pl.DataFrame(
            {
                "trade_date": days,
                "instrument_id": ["510300.SH"] * len(days),
                "adjusted_close": self._prices,
            }
        ).lazy()

    def bars(self, instruments: object, lookback_sessions: int) -> pl.LazyFrame:
        del instruments, lookback_sessions
        return pl.DataFrame(
            {
                "trade_date": [self.signal_date],
                "instrument_id": ["510300.SH"],
                "close": [self._prices[-1]],
            }
        ).lazy()


class _TurnoverData:
    def __init__(self, signal_date: date) -> None:
        self.signal_date = signal_date

    def bars(self, instruments: object, lookback_sessions: int) -> pl.LazyFrame:
        del instruments, lookback_sessions
        return pl.DataFrame(
            {
                "trade_date": [self.signal_date, self.signal_date],
                "instrument_id": ["600001.SH", "600002.SH"],
                "close": [10.0, 10.0],
            }
        ).lazy()


class _SuspendedData:
    def bars(self, instruments: object, lookback_sessions: int) -> pl.LazyFrame:
        del instruments
        assert lookback_sessions == 1
        return pl.DataFrame(
            schema={
                "trade_date": pl.Date,
                "instrument_id": pl.String,
                "close": pl.Float64,
            }
        ).lazy()

def _context(prices: list[float]) -> DecisionContext:
    signal_date, execute_date = date(2024, 1, 4), date(2024, 1, 5)
    instrument = InstrumentId.parse("510300.SH")
    return DecisionContext(
        signal_date,
        execute_date,
        _Data(signal_date, prices),  # type: ignore[arg-type]
        AccountView(100_000, {instrument: 0}, {instrument: 0}, 100_000),
    )


def test_dual_ma_first_long_and_equal_flat_are_literal() -> None:
    strategy = DualMATrendStrategy(
        DualMAConfig(InstrumentId.parse("510300.SH"), short_window=2, long_window=3),
        RebalancePlanner(),
    )
    orders = strategy.on_event(_context([1.0, 1.0, 2.0]))
    assert [(item.side, item.quantity) for item in orders] == [(OrderSide.BUY, 500)]
    assert strategy.signal_frame().select("state", "state_changed").row(0) == (
        "LONG",
        True,
    )

    flat = DualMATrendStrategy(
        DualMAConfig(InstrumentId.parse("510300.SH"), short_window=2, long_window=3),
        RebalancePlanner(),
    )
    assert flat.on_event(_context([1.0, 1.0, 1.0])) == ()
    assert flat.signal_frame()["state"].to_list() == ["FLAT"]


def test_reference_prices_fall_back_to_account_mark_during_suspension() -> None:
    instrument = InstrumentId.parse("000932.SZ")
    strategy = DualMATrendStrategy(
        DualMAConfig(instrument, short_window=2, long_window=3),
        RebalancePlanner(),
    )
    context = DecisionContext(
        date(2018, 12, 7),
        date(2018, 12, 10),
        _SuspendedData(),  # type: ignore[arg-type]
        AccountView(
            cash_fen=0,
            positions={instrument: 100},
            sellable={instrument: 100},
            equity_fen=65_000,
            mark_prices={instrument: 6.5},
        ),
    )

    assert strategy.reference_prices(context, (instrument,)) == {instrument: 6.5}


def test_registry_and_five_module_catalog_are_stable() -> None:
    assert StrategyRegistry.builtins(
        commission_bps=3.0, commission_minimum_fen=500
    ).strategy_ids() == (
        "dual_ma_trend",
        "etf_rotation",
        "stock_multifactor",
    )
    assert StrategyComponentCatalog().list() == {
        "alpha": ("multi_factor_composite", "single_factor"),
        "constraint": ("long_only",),
        "construction": ("mean_variance", "top_n_equal_weight"),
        "cost": ("fixed_bps", "linear_impact", "sqrt_impact"),
        "risk": ("none", "sample_cov", "shrinkage"),
    }


def test_pipeline_rejects_mean_variance_without_non_degenerate_risk() -> None:
    parameters = {
        "pipeline": {
            "frequency": "WEEKLY",
            "alpha": {
                "model_id": "single_factor",
                "params": {"factor_id": "book_to_price_mrq"},
            },
            "risk": {"model_id": "none"},
            "cost": {"model_id": "fixed_bps"},
            "construction": {"model_id": "mean_variance"},
            "constraints": {
                "model_id": "long_only",
                "params": {"min_positions": 1, "max_positions": 5},
            },
        }
    }
    try:
        StrategyPipelineConfig.from_parameters(parameters)  # type: ignore[arg-type]
    except ValueError as error:
        assert "PIPELINE_MODEL_UNAVAILABLE" in str(error)
    else:
        raise AssertionError("MVO accepted a degenerate risk component")


def test_pipeline_rejects_user_supplied_commission_rate() -> None:
    """确认事前基础费率不能绕过唯一交易规则文件。"""
    parameters = {
        "pipeline": {
            "frequency": "WEEKLY",
            "alpha": {
                "model_id": "single_factor",
                "params": {"factor_id": "book_to_price_mrq"},
            },
            "risk": {"model_id": "none"},
            "cost": {"model_id": "fixed_bps", "params": {"fixed_bps": 1.0}},
            "construction": {
                "model_id": "top_n_equal_weight",
                "params": {"top_n": 1},
            },
            "constraints": {
                "model_id": "long_only",
                "params": {"min_positions": 1, "max_positions": 1},
            },
        }
    }
    with pytest.raises(ValueError, match="unknown fixed_bps parameter: fixed_bps"):
        StrategyPipelineConfig.from_parameters(parameters)  # type: ignore[arg-type]


def test_turnover_projection_replaces_holdings_without_exceeding_position_cap() -> None:
    parameters = {
        "pipeline": {
            "frequency": "WEEKLY",
            "alpha": {
                "model_id": "single_factor",
                "params": {"factor_id": "book_to_price_mrq"},
            },
            "risk": {"model_id": "none"},
            "cost": {"model_id": "fixed_bps"},
            "construction": {
                "model_id": "top_n_equal_weight",
                "params": {"top_n": 2},
            },
            "constraints": {
                "model_id": "long_only",
                "params": {
                    "min_positions": 2,
                    "max_positions": 2,
                    "max_position_weight": 0.5,
                    "max_turnover": 0.5,
                    "max_industry_weight": 1.0,
                    "min_adv_amount": 0.0,
                    "long_exposure": 1.0,
                },
            },
        }
    }
    pipeline = StrategyPipelineConfig.from_parameters(parameters)  # type: ignore[arg-type]
    assembler = CrossSectionalPortfolioAssembler(
        pipeline, commission_bps=3.0, commission_minimum_fen=500
    )
    first = InstrumentId.parse("600001.SH")
    second = InstrumentId.parse("600002.SH")
    entrant = InstrumentId.parse("600003.SH")
    another_entrant = InstrumentId.parse("600004.SH")
    signal_date = date(2024, 1, 4)
    context = DecisionContext(
        signal_date,
        date(2024, 1, 5),
        _TurnoverData(signal_date),  # type: ignore[arg-type]
        AccountView(
            0,
            {first: 50, second: 50},
            {first: 50, second: 50},
            100_000,
        ),
    )

    projected = assembler._apply_turnover(
        context,
        {entrant: 0.5, another_entrant: 0.5},
    )

    assert projected == {second: 0.5, entrant: 0.5}
    assert len([value for value in projected.values() if value > 1e-12]) == 2
    assert assembler._turnover(
        {first: 0.5, second: 0.5}, projected
    ) == pytest.approx(0.5)

    current = {
        InstrumentId.parse(f"6000{index:02d}.SH"): 0.1 for index in range(10)
    }
    target = {
        InstrumentId.parse(f"6010{index:02d}.SH"): 0.1 for index in range(10)
    }
    ten_position_pipeline = StrategyPipelineConfig.from_parameters(  # type: ignore[arg-type]
        {
            "pipeline": {
                **parameters["pipeline"],
                "construction": {
                    "model_id": "top_n_equal_weight",
                    "params": {"top_n": 10},
                },
                "constraints": {
                    "model_id": "long_only",
                    "params": {
                        **parameters["pipeline"]["constraints"]["params"],
                        "min_positions": 5,
                        "max_positions": 10,
                        "max_position_weight": 0.1,
                        "max_turnover": 0.4,
                    },
                },
            }
        }
    )
    ten_position_assembler = CrossSectionalPortfolioAssembler(
        ten_position_pipeline, commission_bps=3.0, commission_minimum_fen=500
    )

    ten_position_target = ten_position_assembler._turnover_limited_target(
        current, target
    )

    assert len(ten_position_target) == 10
    assert len(set(ten_position_target) & set(target)) == 4
    assert ten_position_assembler._turnover(
        current, ten_position_target
    ) == pytest.approx(0.4)

    overweight_current = dict(current)
    overweight_key = next(iter(overweight_current))
    overweight_current[overweight_key] = 0.11
    capped_target = ten_position_assembler._turnover_limited_target(
        overweight_current, target
    )
    assert max(capped_target.values()) <= 0.1
    assert len(capped_target) == 10
    assert ten_position_assembler._turnover(
        overweight_current, capped_target
    ) <= 0.4 + 1e-12


def test_multifactor_signal_frame_uses_declared_schema_for_late_invalid_reason() -> None:
    pipeline = StrategyPipelineConfig.from_parameters(  # type: ignore[arg-type]
        {
            "pipeline": {
                "frequency": "WEEKLY",
                "alpha": {
                    "model_id": "single_factor",
                    "params": {"factor_id": "book_to_price_mrq"},
                },
                "risk": {"model_id": "none"},
                "cost": {"model_id": "fixed_bps"},
                "construction": {
                    "model_id": "top_n_equal_weight",
                    "params": {"top_n": 1},
                },
                "constraints": {
                    "model_id": "long_only",
                    "params": {"min_positions": 1, "max_positions": 1},
                },
            }
        }
    )
    strategy = MultifactorStrategy(
        MultifactorConfig(pipeline),
        RebalancePlanner(),
        commission_bps=3.0,
        commission_minimum_fen=500,
    )
    strategy._signals.extend(
        {
            "signal_date": date(2024, 1, 2),
            "instrument_id": f"{600000 + index}.SH",
            "state": "SCORE",
            "score": 1.0,
            "state_changed": False,
            "invalid_reason": None,
        }
        for index in range(101)
    )
    strategy._signals.append(
        {
            "signal_date": date(2024, 1, 3),
            "instrument_id": "600999.SH",
            "state": "INVALID",
            "score": None,
            "state_changed": False,
            "invalid_reason": "INSUFFICIENT_VALID_FACTORS",
        }
    )

    frame = strategy.signal_frame()

    assert frame.schema["invalid_reason"] == pl.String
    assert frame.filter(pl.col("state") == "INVALID")["invalid_reason"].item() == (
        "INSUFFICIENT_VALID_FACTORS"
    )
