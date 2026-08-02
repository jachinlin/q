"""Behavioral tests for the constrained stock multifactor strategy."""

from dataclasses import FrozenInstanceError
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import UUID

import polars as pl
import pytest
import yaml

import quant_core.strategies.multifactor as multifactor_module
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


def _context(data: _Data) -> StrategyContext:
    return StrategyContext(
        _SNAPSHOT,
        _SIGNAL,
        _EXECUTE,
        (date(2026, 7, 30), _SIGNAL, _EXECUTE),
        data,
        PortfolioConstructor(),
    )


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
