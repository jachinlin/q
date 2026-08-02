"""Behavioral tests for the constrained stock multifactor strategy."""

from collections.abc import Callable
from copy import deepcopy
from dataclasses import FrozenInstanceError
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import UUID

import polars as pl
import pytest
import yaml

import quant_core.strategies.multifactor as multifactor_module
from quant_core.domain import InstrumentId
from quant_core.portfolio import (
    ConstraintViolation,
    PortfolioConstraints,
    PortfolioConstructor,
    TargetPortfolio,
)
from quant_core.strategies.base import (
    PortfolioPosition,
    PortfolioState,
    StrategyContext,
)
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
_RAW_BY_FACTOR = {
    "earnings_yield_ttm_v1@1.0.0": (131.0, 131.0, 137.0, 138.0, 138.0, 1000.0),
    "book_to_price_mrq_v1@1.0.0": (131.0, 137.0, 131.0, 138.0, 1000.0, 138.0),
    "roe_avg_pit_v1@1.0.0": (137.0, 131.0, 131.0, 1000.0, 138.0, 138.0),
    "cfo_to_np_pit_v1@1.0.0": (131.0, 137.0, 131.0, 1000.0, 138.0, 138.0),
    "momentum_120_20_v1@1.0.0": (137.0, 131.0, 131.0, 138.0, 1000.0, 138.0),
    "volatility_60d_v1@1.0.0": (-137.0, -131.0, -131.0, -138.0, -1000.0, -138.0),
    "downside_volatility_60d_v1@1.0.0": (
        -131.0,
        -131.0,
        -137.0,
        -138.0,
        -138.0,
        -1000.0,
    ),
    "max_drawdown_120d_v1@1.0.0": (-137.0, -131.0, -131.0, -1000.0, -138.0, -138.0),
}
_EXPECTED_FACTOR_Z = {
    "earnings_yield_ttm_v1@1.0.0": (
        1.6212439465260766,
        -0.5428499119637041,
        -0.26933216074961036,
        0.5401778951611508,
        -1.623915963328636,
        0.2746761943547232,
    ),
    "book_to_price_mrq_v1@1.0.0": (
        -1.084622120900817,
        0.8583152809660731,
        0.10694234157347496,
        -1.3044136043554668,
        1.5366272445979174,
        -0.11284914188118134,
    ),
    "roe_avg_pit_v1@1.0.0": (
        0.3472186562479454,
        -1.027667633232938,
        0.07713404910014317,
        1.85151215119962,
        -1.1764994528239259,
        -0.07169777049084472,
    ),
    "cfo_to_np_pit_v1@1.0.0": (
        -1.6304437390445652,
        0.8851908910748366,
        -0.08269133670107048,
        1.4373923369936963,
        -0.6916621767473222,
        0.0822140244244251,
    ),
    "momentum_120_20_v1@1.0.0": (
        0.42565458364508985,
        -0.4484675152379841,
        0.248118407336624,
        -1.6443033152990194,
        1.6701293097050858,
        -0.2511314701497959,
    ),
    "volatility_60d_v1@1.0.0": (
        -0.42565458364508985,
        0.4484675152379841,
        -0.248118407336624,
        1.6443033152990194,
        -1.6701293097050858,
        0.2511314701497959,
    ),
    "downside_volatility_60d_v1@1.0.0": (
        -1.6212439465260766,
        0.5428499119637041,
        0.26933216074961036,
        -0.5401778951611508,
        1.623915963328636,
        -0.2746761943547232,
    ),
    "max_drawdown_120d_v1@1.0.0": (
        -0.3472186562479454,
        1.027667633232938,
        -0.07713404910014317,
        -1.85151215119962,
        1.1764994528239259,
        0.07169777049084472,
    ),
}
_EXPECTED_CATEGORY_MEANS = {
    "VALUE": (
        0.26831091281262975,
        0.1577326845011845,
        -0.0811949095880677,
        -0.382117854597158,
        -0.043644359365359264,
        0.08091352623677092,
    ),
    "QUALITY": (
        -0.6416125413983099,
        -0.07123837107905068,
        -0.002778643800463658,
        1.6444522440966582,
        -0.9340808147856241,
        0.005258126966790191,
    ),
    "MOMENTUM": (
        0.42565458364508985,
        -0.4484675152379841,
        0.248118407336624,
        -1.6443033152990194,
        1.6701293097050858,
        -0.2511314701497959,
    ),
    "RISK": (
        0.7980390621397039,
        -0.6729950201448753,
        0.0186400985623856,
        0.24912891035391713,
        -0.37676203548249204,
        -0.016051015428639154,
    ),
}
_EXPECTED_CATEGORY_WEIGHTS = {
    "VALUE": 0.25,
    "QUALITY": 0.25,
    "MOMENTUM": 0.30,
    "RISK": 0.20,
}
_EXPECTED_FINAL = {
    "SSE:600001": 0.19397878037504768,
    "SSE:600002": -0.24751568024483683,
    "SSE:600003": 0.05717015356633147,
    "SSE:600004": -0.12788161514404733,
    "SSE:600005": 0.18125509227728148,
    "SSE:600006": -0.057006730829776316,
}


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


class _RecordingConstructor(PortfolioConstructor):
    def __init__(self) -> None:
        self.calls: list[tuple[pl.DataFrame, PortfolioConstraints, date, date]] = []

    def construct(
        self,
        candidates: pl.DataFrame,
        constraints: PortfolioConstraints,
        signal_date: date,
        execute_date: date,
    ) -> TargetPortfolio:
        self.calls.append((candidates.clone(), constraints, signal_date, execute_date))
        return super().construct(candidates, constraints, signal_date, execute_date)


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


def _literal_oracle_frames() -> tuple[pl.DataFrame, pl.DataFrame]:
    visible = datetime(2026, 7, 31, 7, tzinfo=UTC)
    factor_rows = [
        {
            "trade_date": _SIGNAL,
            "instrument_id": instrument,
            "factor_ref": factor_ref,
            "value": values[index],
            "available_at": visible,
            "is_valid": True,
        }
        for factor_ref, values in _RAW_BY_FACTOR.items()
        for index, instrument in enumerate(_IDS[:6])
    ]
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
    universe = pl.DataFrame(
        {
            "instrument_id": list(_IDS[:6]),
            "as_of": [_SIGNAL] * 6,
            "eligible": [True] * 6,
            "reason_codes": [[] for _ in range(6)],
            "industry": ["AAA", "AAA", "AAA", "BBB", "BBB", "BBB"],
            "adv_amount": [1_000.0] * 6,
            "log_market_cap": [10.0, 11.0, 12.0, 10.0, 11.0, 12.0],
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
    return factors, universe


def _empty_multifactor_state() -> PortfolioState:
    return PortfolioState(_SIGNAL, 1_000_000, 1_000_000, 0, (), 1.0)


def _context(
    data: _Data, constructor: PortfolioConstructor | None = None
) -> StrategyContext:
    return StrategyContext(
        _SNAPSHOT,
        _SIGNAL,
        _EXECUTE,
        (date(2026, 7, 30), _SIGNAL, _EXECUTE),
        data,
        PortfolioConstructor() if constructor is None else constructor,
    )


def _run_with_constraints(
    constraints: PortfolioConstraints,
    *,
    factors: pl.DataFrame | None = None,
    universe: pl.DataFrame | None = None,
    current: PortfolioState | None = None,
) -> TargetPortfolio:
    default_factors, default_universe = _frames()
    selected_factors = default_factors if factors is None else factors
    selected_universe = default_universe if universe is None else universe
    selected_current = _empty_multifactor_state() if current is None else current
    return MultifactorStrategy(_config(constraints=constraints)).generate_targets(
        _context(_Data(selected_factors, selected_universe)),
        _SIGNAL,
        selected_current,
    )


def _multifactor_mapping() -> dict[str, object]:
    envelope = yaml.safe_load(
        Path("configs/experiments/examples/multifactor.yaml").read_text(
            encoding="utf-8"
        )
    )
    mapping = envelope["strategy_config"]
    assert isinstance(mapping, dict)
    return mapping


def _mapping_section(mapping: dict[str, object], name: str) -> dict[object, object]:
    section = mapping[name]
    assert isinstance(section, dict)
    return section


def _add_unknown_factor(mapping: dict[str, object]) -> None:
    _mapping_section(mapping, "factor_definitions")["unknown_alpha_v1@1.0.0"] = {
        "category": "VALUE",
        "direction": 1,
    }


def _replace_alpha_ref(mapping: dict[str, object], replacement: str) -> None:
    definitions = _mapping_section(mapping, "factor_definitions")
    definitions[replacement] = definitions.pop(_ALPHA_REFS[0])


def _reverse_risk_direction(mapping: dict[str, object]) -> None:
    definition = _mapping_section(mapping, "factor_definitions")[
        "volatility_60d_v1@1.0.0"
    ]
    assert isinstance(definition, dict)
    definition["direction"] = 1


def _set_factor_definition_field(
    mapping: dict[str, object], field: str, value: object
) -> None:
    definition = _mapping_section(mapping, "factor_definitions")[_ALPHA_REFS[0]]
    assert isinstance(definition, dict)
    definition[field] = value


def _remove_factor_definition_field(mapping: dict[str, object], field: str) -> None:
    definition = _mapping_section(mapping, "factor_definitions")[_ALPHA_REFS[0]]
    assert isinstance(definition, dict)
    definition.pop(field)


def _set_category_weight(
    mapping: dict[str, object], category: str, value: object
) -> None:
    _mapping_section(mapping, "category_weights")[category] = value


def _set_constraint(mapping: dict[str, object], name: str, value: object) -> None:
    _mapping_section(mapping, "constraints")[name] = value


def _remove_constraint(mapping: dict[str, object], name: str) -> None:
    _mapping_section(mapping, "constraints").pop(name)


def _add_unknown_constraint(mapping: dict[str, object]) -> None:
    _mapping_section(mapping, "constraints")["unknown"] = 1


def _set_mad_multiplier(mapping: dict[str, object], value: object) -> None:
    mapping["mad_multiplier"] = value


def _set_nonstring_factor_ref(mapping: dict[str, object]) -> None:
    definitions = _mapping_section(mapping, "factor_definitions")
    definitions[1] = definitions.pop(_ALPHA_REFS[0])


def _set_integer_factor_category(mapping: dict[str, object]) -> None:
    definition = _mapping_section(mapping, "factor_definitions")[_ALPHA_REFS[0]]
    assert isinstance(definition, dict)
    definition["category"] = 1


_MappingMutation = Callable[[dict[str, object]], None]
MULTIFACTOR_MAPPING_MUTATIONS: list[object] = [
    pytest.param(_add_unknown_factor, id="unknown-factor"),
    *[
        pytest.param(
            lambda mapping, replacement=replacement: _replace_alpha_ref(
                mapping, replacement
            ),
            id=f"auxiliary-factor-{replacement.split('_v1')[0]}",
        )
        for replacement in (
            "avg_amount_20d_v1@1.0.0",
            "log_market_cap_v1@1.0.0",
            "industry_code_v1@1.0.0",
        )
    ],
    pytest.param(_reverse_risk_direction, id="risk-direction"),
    pytest.param(
        lambda mapping: _remove_factor_definition_field(mapping, "direction"),
        id="missing-factor-direction",
    ),
    pytest.param(
        lambda mapping: _set_factor_definition_field(mapping, "direction", "1"),
        id="string-factor-direction",
    ),
    pytest.param(
        lambda mapping: _set_factor_definition_field(mapping, "category", "AUXILIARY"),
        id="unknown-factor-category",
    ),
    pytest.param(
        lambda mapping: _set_category_weight(mapping, "VALUE", 0.15),
        id="category-weight-sum",
    ),
    pytest.param(
        lambda mapping: _set_category_weight(mapping, "VALUE", -0.1),
        id="category-weight-negative",
    ),
    pytest.param(
        lambda mapping: _set_category_weight(mapping, "VALUE", float("nan")),
        id="category-weight-nan",
    ),
    pytest.param(
        lambda mapping: _set_category_weight(mapping, "VALUE", float("inf")),
        id="category-weight-inf",
    ),
    pytest.param(
        lambda mapping: _set_constraint(mapping, "min_positions", 0),
        id="min-positions-zero",
    ),
    pytest.param(
        lambda mapping: _set_constraint(mapping, "max_positions", 0),
        id="max-positions-zero",
    ),
    pytest.param(
        lambda mapping: _set_constraint(mapping, "max_positions", 19),
        id="max-positions-below-min",
    ),
    *[
        pytest.param(
            lambda mapping, value=value: _set_mad_multiplier(mapping, value),
            id=f"mad-multiplier-{label}",
        )
        for label, value in (
            ("zero", 0.0),
            ("negative", -1.0),
            ("nan", float("nan")),
            ("inf", float("inf")),
        )
    ],
    pytest.param(_add_unknown_constraint, id="unknown-constraint"),
    *[
        pytest.param(
            lambda mapping, name=name: _remove_constraint(mapping, name),
            id=f"missing-constraint-{name}",
        )
        for name in (
            "max_position_weight",
            "max_industry_weight",
            "min_positions",
            "max_positions",
            "min_adv_amount",
            "max_turnover",
        )
    ],
    *[
        pytest.param(
            lambda mapping, name=name, value=value: _set_constraint(
                mapping, name, value
            ),
            id=f"invalid-constraint-{name}",
        )
        for name, value in (
            ("max_position_weight", 0.0),
            ("max_industry_weight", 1.1),
            ("min_positions", True),
            ("max_positions", "50"),
            ("min_adv_amount", -1.0),
            ("max_turnover", 1.1),
        )
    ],
    pytest.param(_set_nonstring_factor_ref, id="nonstring-factor-ref"),
    pytest.param(_set_integer_factor_category, id="integer-factor-category"),
]


def test_multifactor_literal_score_oracle_covers_full_transform_chain() -> None:
    factors, universe = _literal_oracle_frames()
    decisions: list[MultifactorDecision] = []
    strategy = MultifactorStrategy(
        _config(constraints=PortfolioConstraints(1.0, 1.0, 1, 6, 0.0, 1.0)),
        audit_sink=decisions.extend,
    )

    strategy.generate_targets(
        _context(_Data(factors, universe)), _SIGNAL, _empty_multifactor_state()
    )

    actual = {
        item.instrument_id.canonical(): item.score
        for item in decisions
        if item.reason_code == "MULTIFACTOR_SELECTED"
    }
    assert actual == pytest.approx(_EXPECTED_FINAL, abs=1e-12)


def test_factor_definition_mapping_order_is_publicly_canonical_and_score_neutral() -> (
    None
):
    factors, universe = _literal_oracle_frames()
    constraints = PortfolioConstraints(1.0, 1.0, 1, 6, 0.0, 1.0)
    default_config = _config(constraints=constraints)
    reversed_config = _config(
        constraints=constraints,
        factor_definitions=dict(reversed(default_config.factor_definitions.items())),
    )

    def scores(config: MultifactorConfig) -> dict[str, float | None]:
        decisions: list[MultifactorDecision] = []
        MultifactorStrategy(config, audit_sink=decisions.extend).generate_targets(
            _context(_Data(factors, universe)),
            _SIGNAL,
            _empty_multifactor_state(),
        )
        return {
            decision.instrument_id.canonical(): decision.score for decision in decisions
        }

    assert tuple(default_config.factor_definitions) == tuple(sorted(_ALPHA_REFS))
    assert tuple(reversed_config.factor_definitions) == tuple(sorted(_ALPHA_REFS))
    assert scores(default_config) == pytest.approx(_EXPECTED_FINAL, abs=1e-12)
    assert scores(reversed_config) == pytest.approx(_EXPECTED_FINAL, abs=1e-12)


def test_transform_order_and_factor_components_have_keyed_literal_oracle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factors, universe = _literal_oracle_frames()
    raw_signatures = {
        values: factor_ref for factor_ref, values in _RAW_BY_FACTOR.items()
    }
    frame_refs: dict[int, str] = {}
    calls: list[tuple[str, str]] = []
    zscore_outputs: dict[str, tuple[float | None, ...]] = {}
    original_winsorize = multifactor_module.winsorize_mad
    original_neutralize = multifactor_module.neutralize_wls
    original_zscore = multifactor_module.zscore

    def recording_winsorize(
        frame: pl.DataFrame,
        value_col: str,
        group_cols: tuple[str, ...],
        n_mad: float,
    ) -> pl.DataFrame:
        factor_ref = raw_signatures[tuple(frame[value_col].to_list())]
        calls.append((factor_ref, "winsorize_mad"))
        result = original_winsorize(frame, value_col, group_cols, n_mad)
        frame_refs[id(result)] = factor_ref
        return result

    def recording_neutralize(
        frame: pl.DataFrame,
        value_col: str,
        industry_col: str,
        size_col: str,
    ) -> pl.DataFrame:
        factor_ref = frame_refs[id(frame)]
        calls.append((factor_ref, "neutralize_wls"))
        result = original_neutralize(frame, value_col, industry_col, size_col)
        frame_refs[id(result)] = factor_ref
        return result

    def recording_zscore(
        frame: pl.DataFrame,
        value_col: str,
        group_cols: tuple[str, ...],
    ) -> pl.DataFrame:
        factor_ref = frame_refs[id(frame)]
        calls.append((factor_ref, "zscore"))
        result = original_zscore(frame, value_col, group_cols)
        zscore_outputs[factor_ref] = tuple(result[value_col].to_list())
        return result

    monkeypatch.setattr(multifactor_module, "winsorize_mad", recording_winsorize)
    monkeypatch.setattr(multifactor_module, "neutralize_wls", recording_neutralize)
    monkeypatch.setattr(multifactor_module, "zscore", recording_zscore)
    strategy = MultifactorStrategy(
        _config(constraints=PortfolioConstraints(1.0, 1.0, 1, 6, 0.0, 1.0))
    )

    strategy.generate_targets(
        _context(_Data(factors, universe)), _SIGNAL, _empty_multifactor_state()
    )

    assert calls == [
        (factor_ref, transform)
        for factor_ref in _ALPHA_REFS
        for transform in ("winsorize_mad", "neutralize_wls", "zscore")
    ]
    assert set(zscore_outputs) == set(_ALPHA_REFS)
    for factor_ref, expected in _EXPECTED_FACTOR_Z.items():
        assert zscore_outputs[factor_ref] == pytest.approx(expected, abs=1e-12)
    assert all(
        strategy.config.factor_definitions[ref][1] == -1 for ref in _ALPHA_REFS[5:]
    )


def test_category_means_and_weights_have_literal_final_oracle() -> None:
    factors, universe = _literal_oracle_frames()
    decisions: list[MultifactorDecision] = []
    MultifactorStrategy(
        _config(constraints=PortfolioConstraints(1.0, 1.0, 1, 6, 0.0, 1.0)),
        audit_sink=decisions.extend,
    ).generate_targets(
        _context(_Data(factors, universe)), _SIGNAL, _empty_multifactor_state()
    )
    decision_by_id = {
        decision.instrument_id.canonical(): decision for decision in decisions
    }

    for index, instrument_id in enumerate(_IDS[:6]):
        component_means = {
            "VALUE": (
                _EXPECTED_FACTOR_Z[_ALPHA_REFS[0]][index]
                + _EXPECTED_FACTOR_Z[_ALPHA_REFS[1]][index]
            )
            / 2.0,
            "QUALITY": (
                _EXPECTED_FACTOR_Z[_ALPHA_REFS[2]][index]
                + _EXPECTED_FACTOR_Z[_ALPHA_REFS[3]][index]
            )
            / 2.0,
            "MOMENTUM": _EXPECTED_FACTOR_Z[_ALPHA_REFS[4]][index],
            "RISK": -(
                _EXPECTED_FACTOR_Z[_ALPHA_REFS[5]][index]
                + _EXPECTED_FACTOR_Z[_ALPHA_REFS[6]][index]
                + _EXPECTED_FACTOR_Z[_ALPHA_REFS[7]][index]
            )
            / 3.0,
        }
        expected_means = {
            category: values[index]
            for category, values in _EXPECTED_CATEGORY_MEANS.items()
        }
        assert component_means == pytest.approx(expected_means, abs=1e-12)
        weighted_score = sum(
            _EXPECTED_CATEGORY_WEIGHTS[category] * expected_means[category]
            for category in ("VALUE", "QUALITY", "MOMENTUM", "RISK")
        )
        assert weighted_score == pytest.approx(
            _EXPECTED_FINAL[instrument_id], abs=1e-12
        )
        assert decision_by_id[instrument_id].score == pytest.approx(
            _EXPECTED_FINAL[instrument_id], abs=1e-12
        )


def test_auxiliary_fields_do_not_change_literal_scores() -> None:
    factors, universe = _literal_oracle_frames()

    def selected_scores(candidate_universe: pl.DataFrame) -> dict[str, float | None]:
        decisions: list[MultifactorDecision] = []
        MultifactorStrategy(
            _config(constraints=PortfolioConstraints(1.0, 1.0, 1, 6, 0.0, 1.0)),
            audit_sink=decisions.extend,
        ).generate_targets(
            _context(_Data(factors, candidate_universe)),
            _SIGNAL,
            _empty_multifactor_state(),
        )
        return {
            item.instrument_id.canonical(): item.score
            for item in decisions
            if item.reason_code == "MULTIFACTOR_SELECTED"
        }

    baseline = selected_scores(universe)
    higher_adv = selected_scores(
        universe.with_columns((pl.col("adv_amount") + 1_000.0).alias("adv_amount"))
    )
    shifted_size = selected_scores(
        universe.with_columns((pl.col("log_market_cap") + 7.0).alias("log_market_cap"))
    )
    renamed_industries = selected_scores(
        universe.with_columns(
            pl.col("industry").replace({"AAA": "X", "BBB": "Y"}).alias("industry")
        )
    )

    assert baseline == pytest.approx(_EXPECTED_FINAL, abs=1e-12)
    assert higher_adv == pytest.approx(baseline, abs=1e-12)
    assert shifted_size == pytest.approx(baseline, abs=1e-12)
    assert renamed_industries == pytest.approx(baseline, abs=1e-12)


def test_transform_zero_variance_reason_and_decision_audit_are_immutable() -> None:
    factors, universe = _literal_oracle_frames()
    factors = factors.with_columns(
        pl.when(pl.col("factor_ref") == _ALPHA_REFS[0])
        .then(pl.lit(0.0))
        .otherwise(pl.col("value"))
        .alias("value")
    )
    decisions: list[MultifactorDecision] = []

    MultifactorStrategy(
        _config(constraints=PortfolioConstraints(1.0, 1.0, 1, 6, 0.0, 1.0)),
        audit_sink=decisions.extend,
    ).generate_targets(
        _context(_Data(factors, universe)), _SIGNAL, _empty_multifactor_state()
    )

    assert len(decisions) == 6
    assert all(decision.reason_code == "MULTIFACTOR_SELECTED" for decision in decisions)
    assert all(
        decision.factor_reasons[_ALPHA_REFS[0]] == "ZERO_VARIANCE"
        for decision in decisions
    )
    with pytest.raises(FrozenInstanceError):
        decisions[0].score = 0.0  # type: ignore[misc]
    with pytest.raises(TypeError):
        decisions[0].factor_reasons["x"] = "mutated"  # type: ignore[index]


def test_multifactor_score_tie_selects_smaller_canonical_id() -> None:
    factors, universe = _literal_oracle_frames()
    duplicate_rows = factors.filter(
        pl.col("instrument_id") == "SSE:600001"
    ).with_columns(pl.lit("SSE:600002").alias("instrument_id"))
    tied_factors = pl.concat(
        [
            factors.filter(pl.col("instrument_id") != "SSE:600002"),
            duplicate_rows,
        ]
    )
    tied_universe = universe.with_columns(
        pl.when(pl.col("instrument_id") == "SSE:600002")
        .then(pl.lit(10.0))
        .otherwise(pl.col("log_market_cap"))
        .alias("log_market_cap")
    )
    decisions: list[MultifactorDecision] = []

    target = MultifactorStrategy(
        _config(constraints=PortfolioConstraints(1.0, 1.0, 1, 1, 0.0, 1.0)),
        audit_sink=decisions.extend,
    ).generate_targets(
        _context(_Data(tied_factors, tied_universe)),
        _SIGNAL,
        _empty_multifactor_state(),
    )

    decision_by_id = {item.instrument_id.canonical(): item for item in decisions}
    assert decision_by_id["SSE:600001"].reason_code == "MULTIFACTOR_SELECTED"
    assert decision_by_id["SSE:600002"].reason_code == "MULTIFACTOR_SELECTED"
    assert decision_by_id["SSE:600001"].score is not None
    assert decision_by_id["SSE:600001"].score == decision_by_id["SSE:600002"].score
    assert tuple(
        position.instrument_id.canonical() for position in target.positions
    ) == ("SSE:600001",)


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


def test_multifactor_passes_all_current_holdings_and_schedule_to_real_constructor() -> (
    None
):
    factors, universe = _frames()
    constructor = _RecordingConstructor()
    current = PortfolioState(
        _SIGNAL,
        700_000,
        1_000_000,
        300_000,
        (
            PortfolioPosition(InstrumentId.parse(_IDS[0]), 20, 200_000, 0.2),
            PortfolioPosition(InstrumentId.parse(_IDS[7]), 10, 100_000, 0.1),
        ),
        0.7,
    )

    target = MultifactorStrategy(_config()).generate_targets(
        _context(_Data(factors, universe), constructor), _SIGNAL, current
    )

    assert len(constructor.calls) == 1
    candidates, constraints, signal_date, execute_date = constructor.calls[0]
    assert constraints == _constraints()
    assert (signal_date, execute_date) == (_SIGNAL, _EXECUTE)
    eligible = candidates.filter(pl.col("instrument_id") == _IDS[0]).row(0, named=True)
    noneligible = candidates.filter(pl.col("instrument_id") == _IDS[7]).row(
        0, named=True
    )
    assert eligible["current_weight"] == 0.2
    assert noneligible == {
        "instrument_id": _IDS[7],
        "score": None,
        "industry": None,
        "adv_amount": 0.0,
        "current_weight": 0.1,
    }
    assert (target.signal_date, target.execute_date) == (_SIGNAL, _EXECUTE)


def test_multifactor_max_position_weight_is_binding_and_leaves_cash() -> None:
    target = _run_with_constraints(PortfolioConstraints(0.20, 1.0, 1, 3, 0.0, 1.0))

    assert all(position.target_weight <= 0.20 for position in target.positions)
    assert target.cash_weight == pytest.approx(0.40)


def test_multifactor_max_industry_weight_is_binding_per_industry() -> None:
    factors, universe = _frames()
    baseline = _run_with_constraints(
        PortfolioConstraints(1.0, 1.0, 1, 6, 0.0, 1.0),
        factors=factors,
        universe=universe,
    )
    target = _run_with_constraints(
        PortfolioConstraints(1.0, 0.25, 1, 6, 0.0, 1.0),
        factors=factors,
        universe=universe,
    )
    industry_by_id = dict(universe.select("instrument_id", "industry").iter_rows())

    def industry_weights(portfolio: TargetPortfolio) -> dict[str, float]:
        result: dict[str, float] = {}
        for position in portfolio.positions:
            industry = industry_by_id[position.instrument_id.canonical()]
            result[industry] = result.get(industry, 0.0) + position.target_weight
        return result

    baseline_weights = industry_weights(baseline)
    capped_weights = industry_weights(target)
    assert any(weight > 0.25 for weight in baseline_weights.values())
    assert all(weight <= 0.25 for weight in capped_weights.values())
    assert target.cash_weight == pytest.approx(0.50)


def test_multifactor_min_positions_is_binding_after_factor_coverage() -> None:
    factors, universe = _frames()
    three_covered = factors.filter(pl.col("instrument_id").is_in(_IDS[:3]))

    with pytest.raises(ConstraintViolation) as caught:
        _run_with_constraints(
            PortfolioConstraints(1.0, 1.0, 4, 6, 0.0, 1.0),
            factors=three_covered,
            universe=universe,
        )

    assert caught.value.constraint_name == "min_positions"


def test_multifactor_max_positions_selects_literal_top_two_scores() -> None:
    factors, universe = _literal_oracle_frames()

    target = _run_with_constraints(
        PortfolioConstraints(1.0, 1.0, 1, 2, 0.0, 1.0),
        factors=factors,
        universe=universe,
    )

    assert {position.instrument_id.canonical() for position in target.positions} == {
        "SSE:600001",
        "SSE:600005",
    }


def test_multifactor_min_adv_amount_filters_a_selected_high_score() -> None:
    factors, universe = _frames()
    high_score_id = _IDS[5]
    liquid_universe = universe.with_columns(
        pl.when(pl.col("instrument_id") == high_score_id)
        .then(1_000.0)
        .otherwise(2_000.0)
        .alias("adv_amount")
    )
    baseline = _run_with_constraints(
        PortfolioConstraints(1.0, 1.0, 1, 3, 0.0, 1.0),
        factors=factors,
        universe=liquid_universe,
    )
    target = _run_with_constraints(
        PortfolioConstraints(1.0, 1.0, 1, 3, 1_500.0, 1.0),
        factors=factors,
        universe=liquid_universe,
    )

    assert high_score_id in {
        position.instrument_id.canonical() for position in baseline.positions
    }
    assert high_score_id not in {
        position.instrument_id.canonical() for position in target.positions
    }


def test_multifactor_max_turnover_is_binding_from_all_cash() -> None:
    with pytest.raises(ConstraintViolation) as caught:
        _run_with_constraints(PortfolioConstraints(1.0, 1.0, 1, 6, 0.0, 0.10))

    assert caught.value.constraint_name == "max_turnover"


def test_multifactor_noneligible_current_holding_contributes_to_turnover() -> None:
    constraints = PortfolioConstraints(0.10, 1.0, 1, 3, 0.0, 0.30)
    without_holding = _run_with_constraints(constraints)
    current = PortfolioState(
        _SIGNAL,
        600_000,
        1_000_000,
        400_000,
        (PortfolioPosition(InstrumentId.parse(_IDS[7]), 40, 400_000, 0.4),),
        0.6,
    )

    with pytest.raises(ConstraintViolation) as caught:
        _run_with_constraints(constraints, current=current)

    assert without_holding.cash_weight == pytest.approx(0.70)
    assert caught.value.constraint_name == "max_turnover"
    assert caught.value.actual_value == pytest.approx(0.40)


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
    mapping = _multifactor_mapping()

    config = MultifactorConfig.from_mapping(mapping)

    assert config.min_valid_factors == 6
    assert sum(config.category_weights.values()) == pytest.approx(1.0)
    invalid = deepcopy(mapping)
    invalid["unknown"] = True
    with pytest.raises(ValueError, match="unknown"):
        MultifactorConfig.from_mapping(invalid)


@pytest.mark.parametrize("mutate", MULTIFACTOR_MAPPING_MUTATIONS)
def test_multifactor_mapping_rejects_each_invalid_assembly(
    mutate: _MappingMutation,
) -> None:
    mapping = deepcopy(_multifactor_mapping())
    mutate(mapping)

    with pytest.raises((TypeError, ValueError)):
        MultifactorConfig.from_mapping(mapping)


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

    target = MultifactorStrategy(
        _config(), audit_sink=decisions.extend
    ).generate_targets(
        _context(_Data(factors, universe)),
        _SIGNAL,
        PortfolioState(_SIGNAL, 1_000_000, 1_000_000, 0, (), 1.0),
    )

    selected = {position.instrument_id.canonical() for position in target.positions}
    assert _IDS[0] not in selected
    assert target.cash_weight == 0.0
    decision = next(
        item for item in decisions if item.instrument_id.canonical() == _IDS[0]
    )
    assert decision.reason_code == "INSUFFICIENT_FACTOR_COVERAGE"
    assert decision.factor_reasons[_ALPHA_REFS[0]] == "SOURCE_INVALID"


def test_multifactor_rejects_future_factor_availability_before_targeting() -> None:
    factors, universe = _frames()
    factors = factors.with_columns(
        pl.when(
            (pl.col("instrument_id") == _IDS[0])
            & (pl.col("factor_ref") == _ALPHA_REFS[0])
        )
        .then(pl.lit(datetime(2026, 8, 3, tzinfo=UTC)))
        .otherwise(pl.col("available_at"))
        .alias("available_at")
    )
    with pytest.raises(ValueError, match="available_at"):
        MultifactorStrategy(_config()).generate_targets(
            _context(_Data(factors, universe)),
            _SIGNAL,
            PortfolioState(_SIGNAL, 1_000_000, 1_000_000, 0, (), 1.0),
        )


def test_multifactor_accepts_exactly_six_valid_factors_with_category_coverage() -> None:
    factors, universe = _frames()
    factors = factors.filter(
        ~(
            (pl.col("instrument_id") == _IDS[0])
            & pl.col("factor_ref").is_in((_ALPHA_REFS[0], _ALPHA_REFS[2]))
        )
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
    assert decision.reason_code == "MULTIFACTOR_SELECTED"
    assert decision.score is not None


def test_multifactor_rejects_seven_factor_coverage_without_momentum_category() -> None:
    factors, universe = _frames()
    factors = factors.filter(
        ~(
            (pl.col("instrument_id") == _IDS[0])
            & (pl.col("factor_ref") == _ALPHA_REFS[4])
        )
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
