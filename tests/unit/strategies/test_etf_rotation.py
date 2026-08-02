"""Behavioral tests for snapshot-bound ETF rotation targets."""

from copy import deepcopy
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import UUID

import polars as pl
import pytest
import yaml

from quant_core.backtest.accounting import AccountSnapshot
from quant_core.domain import InstrumentId
from quant_core.portfolio import PortfolioConstructor
from quant_core.strategies.base import (
    PortfolioState,
    RebalanceFrequency,
    StrategyContext,
    StrategyTargetAdapter,
    StrategyValidationError,
)
from quant_core.strategies.etf_rotation import EtfRotationConfig, EtfRotationStrategy

_SNAPSHOT = UUID("00000000-0000-0000-0000-000000000601")
_SIGNAL = date(2026, 7, 31)
_EXECUTE = date(2026, 8, 3)
_ETF_A = "SSE:510001"
_ETF_B = "SSE:510002"
_ETF_C = "SSE:510003"
_RETURN_REFS = (
    "return_20d_v1@1.0.0",
    "return_60d_v1@1.0.0",
    "return_120d_v1@1.0.0",
)
_TREND_REF = "trend_120d_v1@1.0.0"
_VOL_REF = "volatility_60d_v1@1.0.0"


class _Data:
    def __init__(self, frame: pl.DataFrame) -> None:
        self.frame = frame
        self.calls: list[
            tuple[UUID, date, tuple[InstrumentId, ...] | None, tuple[str, ...]]
        ] = []

    def factor_values(
        self,
        snapshot_id: UUID,
        signal_date: date,
        instruments: tuple[InstrumentId, ...] | None,
        factor_refs: tuple[str, ...],
    ) -> pl.DataFrame:
        self.calls.append((snapshot_id, signal_date, instruments, factor_refs))
        return self.frame

    def stock_universe(self, snapshot_id: UUID, signal_date: date) -> pl.DataFrame:
        raise AssertionError("ETF rotation must not request a stock universe")


def _config(**overrides: object) -> EtfRotationConfig:
    values: dict[str, object] = {
        "etf_pool": tuple(
            InstrumentId.parse(value) for value in (_ETF_A, _ETF_B, _ETF_C)
        ),
        "return_factor_weights": {
            _RETURN_REFS[0]: 0.2,
            _RETURN_REFS[1]: 0.3,
            _RETURN_REFS[2]: 0.5,
        },
        "trend_factor_ref": _TREND_REF,
        "volatility_factor_ref": _VOL_REF,
        "volatility_penalty": 0.5,
        "top_n": 2,
    }
    values.update(overrides)
    return EtfRotationConfig(**values)  # type: ignore[arg-type]


def _factor_frame(values: dict[str, dict[str, float | None]]) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    visible = datetime(2026, 7, 31, 7, tzinfo=UTC)
    for instrument, factors in values.items():
        for factor_ref in (*_RETURN_REFS, _TREND_REF, _VOL_REF):
            value = factors.get(factor_ref)
            rows.append(
                {
                    "trade_date": _SIGNAL,
                    "instrument_id": instrument,
                    "factor_ref": factor_ref,
                    "value": value,
                    "available_at": visible,
                    "is_valid": value is not None,
                    "invalid_reason": None,
                }
            )
    return pl.DataFrame(
        rows,
        schema={
            "trade_date": pl.Date,
            "instrument_id": pl.String,
            "factor_ref": pl.String,
            "value": pl.Float64,
            "available_at": pl.Datetime("us", "UTC"),
            "is_valid": pl.Boolean,
            "invalid_reason": pl.String,
        },
    )


def _fully_valid_etf_values() -> dict[str, dict[str, float]]:
    return {
        _ETF_A: {
            _RETURN_REFS[0]: 0.1,
            _RETURN_REFS[1]: 0.1,
            _RETURN_REFS[2]: 0.1,
            _TREND_REF: 1.0,
            _VOL_REF: 0.1,
        },
        _ETF_B: {
            _RETURN_REFS[0]: 0.3,
            _RETURN_REFS[1]: 0.3,
            _RETURN_REFS[2]: 0.3,
            _TREND_REF: 1.0,
            _VOL_REF: 0.1,
        },
        _ETF_C: {
            _RETURN_REFS[0]: 0.2,
            _RETURN_REFS[1]: 0.2,
            _RETURN_REFS[2]: 0.2,
            _TREND_REF: 1.0,
            _VOL_REF: 0.1,
        },
    }


def _context(
    data: _Data, *, sessions: tuple[date, ...] | None = None
) -> StrategyContext:
    return StrategyContext(
        snapshot_id=_SNAPSHOT,
        signal_date=_SIGNAL,
        execute_date=_EXECUTE,
        sessions=sessions or (date(2026, 7, 30), _SIGNAL, _EXECUTE),
        data=data,
        portfolio_constructor=PortfolioConstructor(),
    )


def _empty_state() -> PortfolioState:
    return PortfolioState(_SIGNAL, 1_000_000, 1_000_000, 0, (), 1.0)


def _empty_account_snapshot() -> AccountSnapshot:
    return AccountSnapshot(_SIGNAL, 1_000_000, (), 0, 1_000_000)


def _etf_mapping() -> dict[str, object]:
    envelope = yaml.safe_load(
        Path("configs/experiments/examples/etf_rotation.yaml").read_text(
            encoding="utf-8"
        )
    )
    mapping = envelope["strategy_config"]
    assert isinstance(mapping, dict)
    return mapping


def test_etf_rotation_scores_selected_etfs_and_normalizes_equal_weights() -> None:
    data = _Data(
        _factor_frame(
            {
                _ETF_A: {
                    _RETURN_REFS[0]: 0.1,
                    _RETURN_REFS[1]: 0.2,
                    _RETURN_REFS[2]: 0.3,
                    _TREND_REF: 1.0,
                    _VOL_REF: 0.1,
                },
                _ETF_B: {
                    _RETURN_REFS[0]: 0.2,
                    _RETURN_REFS[1]: 0.1,
                    _RETURN_REFS[2]: 0.4,
                    _TREND_REF: 1.0,
                    _VOL_REF: 0.2,
                },
                _ETF_C: {
                    _RETURN_REFS[0]: 0.4,
                    _RETURN_REFS[1]: 0.4,
                    _RETURN_REFS[2]: 0.4,
                    _TREND_REF: 1.0,
                    _VOL_REF: 0.8,
                },
            }
        )
    )

    target = EtfRotationStrategy(_config()).generate_targets(
        _context(data), _SIGNAL, _empty_state()
    )

    assert [position.instrument_id.canonical() for position in target.positions] == [
        _ETF_A,
        _ETF_B,
    ]
    assert [position.target_weight for position in target.positions] == [0.5, 0.5]
    assert [position.score for position in target.positions] == pytest.approx(
        [0.18, 0.17]
    )
    assert all(
        position.reason_code == "ETF_ROTATION_SELECTED" for position in target.positions
    )
    assert target.cash_weight == 0.0
    assert data.calls[0][2] == tuple(
        InstrumentId.parse(item) for item in (_ETF_A, _ETF_B, _ETF_C)
    )
    assert data.calls[0][3] == (*_RETURN_REFS, _TREND_REF, _VOL_REF)


def test_etf_rotation_excludes_missing_or_nonpositive_trend_signals_and_moves_to_cash() -> (
    None
):
    data = _Data(
        _factor_frame(
            {
                _ETF_A: {
                    _RETURN_REFS[0]: 0.1,
                    _RETURN_REFS[1]: 0.1,
                    _RETURN_REFS[2]: 0.1,
                    _TREND_REF: 0.0,
                    _VOL_REF: 0.1,
                },
                _ETF_B: {
                    _RETURN_REFS[0]: 0.1,
                    _RETURN_REFS[1]: None,
                    _RETURN_REFS[2]: 0.1,
                    _TREND_REF: 1.0,
                    _VOL_REF: 0.1,
                },
                _ETF_C: {
                    _RETURN_REFS[0]: 0.1,
                    _RETURN_REFS[1]: 0.1,
                    _RETURN_REFS[2]: 0.1,
                    _TREND_REF: -1.0,
                    _VOL_REF: 0.1,
                },
            }
        )
    )

    target = EtfRotationStrategy(_config()).generate_targets(
        _context(data), _SIGNAL, _empty_state()
    )

    assert target.positions == ()
    assert target.cash_weight == 1.0


@pytest.mark.parametrize("mode", ["missing", "invalid"])
def test_etf_excludes_one_instrument_for_each_unusable_signal(mode: str) -> None:
    frame = _factor_frame(_fully_valid_etf_values())
    mask = (pl.col("instrument_id") == _ETF_A) & (
        pl.col("factor_ref") == _RETURN_REFS[0]
    )
    if mode == "missing":
        frame = frame.filter(~mask)
    elif mode == "invalid":
        frame = frame.with_columns(
            pl.when(mask).then(False).otherwise(pl.col("is_valid")).alias("is_valid"),
            pl.when(mask).then(0.2).otherwise(pl.col("value")).alias("value"),
            pl.when(mask)
            .then(pl.lit("SOURCE_INVALID"))
            .otherwise(pl.col("invalid_reason"))
            .alias("invalid_reason"),
        )
    target = EtfRotationStrategy(_config(top_n=3)).generate_targets(
        _context(_Data(frame)), _SIGNAL, _empty_state()
    )
    selected = [position.instrument_id.canonical() for position in target.positions]
    assert selected == [_ETF_B, _ETF_C]
    assert _ETF_A not in selected
    assert [position.target_weight for position in target.positions] == [0.5, 0.5]
    assert target.cash_weight == 0.0


@pytest.mark.parametrize("value", [float("nan"), float("inf")])
def test_etf_rejects_nonfinite_factor_values_before_targeting(value: float) -> None:
    frame = _factor_frame(_fully_valid_etf_values()).with_columns(
        pl.when(
            (pl.col("instrument_id") == _ETF_A)
            & (pl.col("factor_ref") == _RETURN_REFS[0])
        )
        .then(value)
        .otherwise(pl.col("value"))
        .alias("value")
    )
    with pytest.raises(ValueError, match="finite"):
        EtfRotationStrategy(_config(top_n=1)).generate_targets(
            _context(_Data(frame)), _SIGNAL, _empty_state()
        )


def test_etf_rejects_future_factor_availability_before_targeting() -> None:
    frame = _factor_frame(_fully_valid_etf_values()).with_columns(
        pl.when(
            (pl.col("instrument_id") == _ETF_A)
            & (pl.col("factor_ref") == _RETURN_REFS[0])
        )
        .then(pl.lit(datetime(2026, 8, 3, tzinfo=UTC)))
        .otherwise(pl.col("available_at"))
        .alias("available_at")
    )
    with pytest.raises(ValueError, match="available_at"):
        EtfRotationStrategy(_config(top_n=1)).generate_targets(
            _context(_Data(frame)), _SIGNAL, _empty_state()
        )


def test_etf_rotation_breaks_score_ties_by_canonical_identifier() -> None:
    data = _Data(
        _factor_frame(
            {
                _ETF_C: {
                    _RETURN_REFS[0]: 0.2,
                    _RETURN_REFS[1]: 0.2,
                    _RETURN_REFS[2]: 0.2,
                    _TREND_REF: 1.0,
                    _VOL_REF: 0.1,
                },
                _ETF_A: {
                    _RETURN_REFS[0]: 0.2,
                    _RETURN_REFS[1]: 0.2,
                    _RETURN_REFS[2]: 0.2,
                    _TREND_REF: 1.0,
                    _VOL_REF: 0.1,
                },
                _ETF_B: {
                    _RETURN_REFS[0]: 0.2,
                    _RETURN_REFS[1]: 0.2,
                    _RETURN_REFS[2]: 0.2,
                    _TREND_REF: 1.0,
                    _VOL_REF: 0.1,
                },
            }
        )
    )

    target = EtfRotationStrategy(_config(top_n=2)).generate_targets(
        _context(data), _SIGNAL, _empty_state()
    )

    assert [position.instrument_id.canonical() for position in target.positions] == [
        _ETF_A,
        _ETF_B,
    ]


def test_month_end_rebalance_uses_next_actual_session_not_calendar_month_end() -> None:
    strategy = EtfRotationStrategy(_config())
    holiday_after_month_end = StrategyContext(
        _SNAPSHOT,
        date(2026, 1, 30),
        date(2026, 2, 2),
        (date(2026, 1, 29), date(2026, 1, 30), date(2026, 2, 2)),
        _Data(_factor_frame({})),
        PortfolioConstructor(),
    )

    assert strategy.should_rebalance(
        holiday_after_month_end, holiday_after_month_end.signal_date
    )
    assert not strategy.should_rebalance(
        _context(_Data(_factor_frame({}))), _SIGNAL - timedelta(days=1)
    )


def test_etf_rotation_does_not_rebalance_when_next_session_stays_in_month() -> None:
    context = StrategyContext(
        _SNAPSHOT,
        date(2026, 8, 3),
        date(2026, 8, 4),
        (date(2026, 7, 31), date(2026, 8, 3), date(2026, 8, 4)),
        _Data(_factor_frame({})),
        PortfolioConstructor(),
    )
    assert not EtfRotationStrategy(_config()).should_rebalance(
        context, context.signal_date
    )


@pytest.mark.parametrize(
    "override",
    [
        {
            "return_factor_weights": {
                **{ref: 1 / 3 for ref in _RETURN_REFS},
                "unknown@1": 0.0,
            }
        },
        {"frequency": RebalanceFrequency.WEEKLY},
        {"missing_signal_policy": "IMPUTE"},
        {"weighting": "SCORE"},
        {"volatility_penalty": float("inf")},
    ],
)
def test_etf_config_rejects_unknown_enums_and_nonfinite_values(
    override: dict[str, object],
) -> None:
    with pytest.raises((TypeError, ValueError)):
        _config(**override)


@pytest.mark.parametrize(
    "override",
    [
        {"top_n": 4},
        {"volatility_penalty": -0.1},
        {"volatility_penalty": float("nan")},
        {"trend_factor_ref": "other@1"},
        {"volatility_factor_ref": "other@1"},
        {
            "return_factor_weights": {
                _RETURN_REFS[0]: -0.1,
                _RETURN_REFS[1]: 0.5,
                _RETURN_REFS[2]: 0.6,
            }
        },
        {
            "return_factor_weights": {
                _RETURN_REFS[0]: float("nan"),
                _RETURN_REFS[1]: 0.5,
                _RETURN_REFS[2]: 0.5,
            }
        },
    ],
)
def test_etf_config_rejects_remaining_numeric_and_ref_boundaries(
    override: dict[str, object],
) -> None:
    with pytest.raises((TypeError, ValueError)):
        _config(**override)


def test_etf_target_keeps_context_signal_and_execution_dates() -> None:
    data = _Data(
        _factor_frame(
            {
                _ETF_A: {
                    _RETURN_REFS[0]: 0.1,
                    _RETURN_REFS[1]: 0.1,
                    _RETURN_REFS[2]: 0.1,
                    _TREND_REF: 1.0,
                    _VOL_REF: 0.1,
                },
                _ETF_B: {
                    _RETURN_REFS[0]: 0.1,
                    _RETURN_REFS[1]: 0.1,
                    _RETURN_REFS[2]: 0.1,
                    _TREND_REF: 1.0,
                    _VOL_REF: 0.1,
                },
                _ETF_C: {
                    _RETURN_REFS[0]: 0.1,
                    _RETURN_REFS[1]: 0.1,
                    _RETURN_REFS[2]: 0.1,
                    _TREND_REF: 1.0,
                    _VOL_REF: 0.1,
                },
            }
        )
    )
    target = EtfRotationStrategy(_config()).generate_targets(
        _context(data), _SIGNAL, _empty_state()
    )
    assert (target.signal_date, target.execute_date) == (_SIGNAL, _EXECUTE)


@pytest.mark.parametrize(
    "override",
    [
        {
            "return_factor_weights": {
                _RETURN_REFS[0]: 0.2,
                _RETURN_REFS[1]: 0.3,
                _RETURN_REFS[2]: float("inf"),
            }
        },
        {
            "etf_pool": (
                InstrumentId.parse(_ETF_B),
                InstrumentId.parse(_ETF_A),
                InstrumentId.parse(_ETF_C),
            )
        },
        {
            "etf_pool": (
                "SSE:510001",
                InstrumentId.parse(_ETF_B),
                InstrumentId.parse(_ETF_C),
            )
        },
    ],
)
def test_etf_config_rejects_infinite_weight_unsorted_or_noninstrument_pool(
    override: dict[str, object],
) -> None:
    with pytest.raises((TypeError, ValueError)):
        _config(**override)


@pytest.mark.parametrize(
    "override",
    [
        {"top_n": 0},
        {
            "return_factor_weights": {
                _RETURN_REFS[0]: 1.0,
                _RETURN_REFS[1]: 0.0,
                _RETURN_REFS[2]: 0.1,
            }
        },
        {"etf_pool": (InstrumentId.parse(_ETF_A), InstrumentId.parse(_ETF_A))},
    ],
)
def test_etf_config_fails_closed_for_invalid_inputs(
    override: dict[str, object],
) -> None:
    with pytest.raises((TypeError, ValueError)):
        _config(**override)


def test_example_etf_yaml_is_safe_loadable_and_has_one_validated_entry_point() -> None:
    mapping = _etf_mapping()

    config = EtfRotationConfig.from_mapping(mapping)

    assert config.frequency is RebalanceFrequency.MONTHLY
    invalid = deepcopy(mapping)
    invalid["unknown"] = True
    with pytest.raises(ValueError, match="unknown"):
        EtfRotationConfig.from_mapping(invalid)


@pytest.mark.parametrize("identifier", ["BAD", "SSE:51001", "UNKNOWN:510001"])
def test_etf_mapping_rejects_noncanonical_pool_identifier(identifier: str) -> None:
    mapping = deepcopy(_etf_mapping())
    etf_pool = mapping["etf_pool"]
    assert isinstance(etf_pool, list)
    etf_pool[0] = identifier

    with pytest.raises((TypeError, ValueError)):
        EtfRotationConfig.from_mapping(mapping)


def test_adapter_rejects_validation_issues_before_generating_a_target() -> None:
    class _InvalidStrategy(EtfRotationStrategy):
        def validate(self, ctx: StrategyContext) -> list[object]:
            return [object()]

    strategy = _InvalidStrategy(_config())
    adapter = StrategyTargetAdapter(
        {strategy.ref: strategy},
        lambda snapshot_id, signal_date, execute_date: _context(
            _Data(_factor_frame({}))
        ),
    )

    with pytest.raises(StrategyValidationError):
        adapter.generate_target(
            strategy.ref,
            _SNAPSHOT,
            _SIGNAL,
            _EXECUTE,
            _empty_account_snapshot(),
        )
