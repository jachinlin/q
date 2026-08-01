"""Behavioral tests for the constrained stock multifactor strategy."""

from datetime import UTC, date, datetime
from pathlib import Path
from uuid import UUID

import polars as pl
import pytest
import yaml

from quant_core.domain import InstrumentId
from quant_core.portfolio import PortfolioConstraints, PortfolioConstructor
from quant_core.strategies.base import PortfolioState, StrategyContext
from quant_core.strategies.multifactor import (
    MultifactorConfig,
    MultifactorDecision,
    MultifactorStrategy,
)

_SNAPSHOT = UUID("00000000-0000-0000-0000-000000000602")
_SIGNAL = date(2026, 7, 31)
_EXECUTE = date(2026, 8, 3)
_ALPHA_REFS = (
    "earnings_yield_ttm_v1@1.0.0",
    "book_to_price_mrq_v1@1.0.0",
    "roe_avg_pit_v1@1.0.0",
    "cfo_to_np_pit_v1@1.0.0",
    "momentum_120_20_v1@1.0.0",
    "volatility_60d_v1@1.0.0",
    "downside_volatility_60d_v1@1.0.0",
    "max_drawdown_120d_v1@1.0.0",
)
_IDS = tuple(f"SSE:{600001 + index:06d}" for index in range(8))


class _Data:
    def __init__(self, factors: pl.DataFrame, universe: pl.DataFrame) -> None:
        self.factors = factors
        self.universe = universe
        self.factor_calls: list[tuple[InstrumentId, ...] | None] = []

    def factor_values(
        self,
        snapshot_id: UUID,
        signal_date: date,
        instruments: tuple[InstrumentId, ...] | None,
        factor_refs: tuple[str, ...],
    ) -> pl.DataFrame:
        assert factor_refs == _ALPHA_REFS
        self.factor_calls.append(instruments)
        if instruments is None:
            return self.factors
        return self.factors.filter(
            pl.col("instrument_id").is_in(
                [instrument.canonical() for instrument in instruments]
            )
        )

    def stock_universe(self, snapshot_id: UUID, signal_date: date) -> pl.DataFrame:
        return self.universe


def _constraints() -> PortfolioConstraints:
    return PortfolioConstraints(0.6, 0.8, 2, 3, 100.0, 1.0)


def _config(**overrides: object) -> MultifactorConfig:
    values: dict[str, object] = {"constraints": _constraints()}
    values.update(overrides)
    return MultifactorConfig(**values)  # type: ignore[arg-type]


def _frames() -> tuple[pl.DataFrame, pl.DataFrame]:
    visible = datetime(2026, 7, 31, 7, tzinfo=UTC)
    factor_rows: list[dict[str, object]] = []
    for index, instrument in enumerate(_IDS):
        # The alternating residual avoids a pure size/industry fit; risk factors are
        # deliberately inverse so their configured -1 direction rewards lower risk.
        residual = (-1.0, 1.0, -2.0, 2.0)[index % 4]
        for factor_index, factor_ref in enumerate(_ALPHA_REFS):
            risk = factor_index >= 5
            factor_rows.append(
                {
                    "trade_date": _SIGNAL,
                    "instrument_id": instrument,
                    "factor_ref": factor_ref,
                    "value": (index + 1) * (-1.0 if risk else 1.0) + residual,
                    "available_at": visible,
                    "is_valid": True,
                }
            )
    universe = pl.DataFrame(
        {
            "instrument_id": list(_IDS),
            "as_of": [_SIGNAL] * len(_IDS),
            "eligible": [True] * 7 + [False],
            "reason_codes": [[] for _ in _IDS[:-1]] + [["NOT_ELIGIBLE"]],
            "industry": ["BANK"] * 4 + ["TECH"] * 4,
            "adv_amount": [1_000.0] * len(_IDS),
            "log_market_cap": [10.0, 11.0, 12.0, 13.0] * 2,
        },
        schema={
            "instrument_id": pl.String,
            "as_of": pl.Date,
            "eligible": pl.Boolean,
            "reason_codes": pl.List(pl.String),
            "industry": pl.String,
            "adv_amount": pl.Float64,
            "log_market_cap": pl.Float64,
        },
    )
    factors = pl.DataFrame(
        factor_rows,
        schema={
            "trade_date": pl.Date,
            "instrument_id": pl.String,
            "factor_ref": pl.String,
            "value": pl.Float64,
            "available_at": pl.Datetime("us", "UTC"),
            "is_valid": pl.Boolean,
        },
    )
    return factors, universe


def _context(data: _Data) -> StrategyContext:
    return StrategyContext(
        _SNAPSHOT,
        _SIGNAL,
        _EXECUTE,
        (date(2026, 7, 30), _SIGNAL, _EXECUTE),
        data,
        PortfolioConstructor(),
    )


def test_multifactor_uses_eligible_current_snapshot_and_constructor_constraints() -> (
    None
):
    factors, universe = _frames()
    data = _Data(factors, universe)

    target = MultifactorStrategy(_config()).generate_targets(
        _context(data),
        _SIGNAL,
        PortfolioState(_SIGNAL, 1_000_000, 1_000_000, 0, (), 1.0),
    )

    assert 2 <= len(target.positions) <= 3
    assert all(
        position.instrument_id.canonical() in _IDS[:7] for position in target.positions
    )
    assert all(position.target_weight <= 0.6 for position in target.positions)
    assert sum(
        position.target_weight for position in target.positions
    ) + target.cash_weight == pytest.approx(1.0)
    assert data.factor_calls == [tuple(InstrumentId.parse(value) for value in _IDS[:7])]


def test_multifactor_excludes_instruments_without_required_factor_coverage() -> None:
    factors, universe = _frames()
    factors = factors.filter(
        ~(
            (pl.col("instrument_id") == _IDS[0])
            & pl.col("factor_ref").is_in(_ALPHA_REFS[:3])
        )
    )
    data = _Data(factors, universe)

    target = MultifactorStrategy(_config()).generate_targets(
        _context(data),
        _SIGNAL,
        PortfolioState(_SIGNAL, 1_000_000, 1_000_000, 0, (), 1.0),
    )

    assert _IDS[0] not in [
        position.instrument_id.canonical() for position in target.positions
    ]


def test_multifactor_frequency_is_weekly_and_uses_next_session_boundary() -> None:
    factors, universe = _frames()
    strategy = MultifactorStrategy(_config())
    context = StrategyContext(
        _SNAPSHOT,
        date(2026, 7, 31),
        date(2026, 8, 3),
        (date(2026, 7, 30), date(2026, 7, 31), date(2026, 8, 3)),
        _Data(factors, universe),
        PortfolioConstructor(),
    )

    assert strategy.should_rebalance(context, context.signal_date)


def test_multifactor_does_not_rebalance_inside_an_iso_week() -> None:
    factors, universe = _frames()
    context = StrategyContext(
        _SNAPSHOT,
        date(2026, 8, 3),
        date(2026, 8, 4),
        (date(2026, 7, 31), date(2026, 8, 3), date(2026, 8, 4)),
        _Data(factors, universe),
        PortfolioConstructor(),
    )
    assert not MultifactorStrategy(_config()).should_rebalance(
        context, context.signal_date
    )


@pytest.mark.parametrize(
    "override",
    [
        {"min_valid_factors": 5},
        {"category_weights": {"VALUE": 1.0}},
    ],
)
def test_multifactor_config_fails_closed_for_invalid_inputs(
    override: dict[str, object],
) -> None:
    with pytest.raises((TypeError, ValueError)):
        _config(**override)


def test_example_multifactor_yaml_is_safe_loadable_and_rejects_unknown_keys() -> None:
    mapping = yaml.safe_load(
        (Path("configs/experiments/examples/multifactor.yaml")).read_text(
            encoding="utf-8"
        )
    )

    config = MultifactorConfig.from_mapping(mapping)

    assert config.min_valid_factors == 6
    with pytest.raises(ValueError, match="unknown"):
        MultifactorConfig.from_mapping({**mapping, "unknown": True})


@pytest.mark.parametrize(
    "definition",
    [
        {"category": "VALUE"},
        {"category": "VALUE", "direction": "1"},
        {"category": "AUXILIARY", "direction": 1},
    ],
)
def test_multifactor_mapping_rejects_bad_nested_factor_definitions(
    definition: dict[str, object],
) -> None:
    with pytest.raises((TypeError, ValueError)):
        MultifactorConfig.from_mapping(
            {
                "constraints": {
                    "max_position_weight": 0.6,
                    "max_industry_weight": 0.8,
                    "min_positions": 1,
                    "max_positions": 2,
                    "min_adv_amount": 1.0,
                    "max_turnover": 1.0,
                },
                "factor_definitions": {
                    **{
                        ref: {"category": category, "direction": direction}
                        for ref, (category, direction) in {
                            "earnings_yield_ttm_v1@1.0.0": ("VALUE", 1),
                            "book_to_price_mrq_v1@1.0.0": ("VALUE", 1),
                            "roe_avg_pit_v1@1.0.0": ("QUALITY", 1),
                            "cfo_to_np_pit_v1@1.0.0": ("QUALITY", 1),
                            "momentum_120_20_v1@1.0.0": ("MOMENTUM", 1),
                            "volatility_60d_v1@1.0.0": ("RISK", -1),
                            "downside_volatility_60d_v1@1.0.0": ("RISK", -1),
                            "max_drawdown_120d_v1@1.0.0": ("RISK", -1),
                        }.items()
                    },
                    "earnings_yield_ttm_v1@1.0.0": definition,
                },
            }
        )


def test_multifactor_audits_insufficient_factor_coverage_with_source_reason() -> None:
    factors, universe = _frames()
    factors = factors.with_columns(
        pl.when(
            (pl.col("instrument_id") == _IDS[0])
            & pl.col("factor_ref").is_in(_ALPHA_REFS[:3])
        )
        .then(pl.lit(False))
        .otherwise(pl.col("is_valid"))
        .alias("is_valid"),
        pl.lit("SOURCE_INVALID", dtype=pl.String).alias("invalid_reason"),
    )
    decisions: list[MultifactorDecision] = []

    MultifactorStrategy(_config(), audit_sink=decisions.extend).generate_targets(
        _context(_Data(factors, universe)),
        _SIGNAL,
        PortfolioState(_SIGNAL, 1_000_000, 1_000_000, 0, (), 1.0),
    )

    decision = next(
        item for item in decisions if item.instrument_id.canonical() == _IDS[0]
    )
    assert decision.reason_code == "INSUFFICIENT_FACTOR_COVERAGE"
    assert decision.factor_reasons[_ALPHA_REFS[0]] == "SOURCE_INVALID"
