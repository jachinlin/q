"""Public contract tests for constrained target-portfolio construction."""

from datetime import date, timedelta
from math import isfinite

import polars as pl
import pytest
from hypothesis import given
from hypothesis import strategies as st

from quant_research.portfolio import (
    ConstraintViolation,
    PortfolioConstraints,
    PortfolioConstructor,
)

_SIGNAL_DATE = date(2026, 7, 30)
_EXECUTE_DATE = date(2026, 7, 31)


def _constraints(**overrides: object) -> PortfolioConstraints:
    values: dict[str, object] = {
        "max_position_weight": 0.6,
        "min_positions": 1,
        "max_positions": 3,
        "min_adv_amount": 100.0,
        "max_turnover": 1.0,
    }
    values.update(overrides)
    return PortfolioConstraints(**values)  # type: ignore[arg-type]


def _candidates(rows: list[dict[str, object]]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={
            "instrument_id": pl.String,
            "score": pl.Float64,
            "adv_amount": pl.Float64,
            "current_weight": pl.Float64,
        },
    )


def _row(
    instrument_id: str,
    score: float | None,
    adv_amount: float = 1_000.0,
    current_weight: float = 0.0,
) -> dict[str, object]:
    return {
        "instrument_id": instrument_id,
        "score": score,
        "adv_amount": adv_amount,
        "current_weight": current_weight,
    }


def test_constructor_stably_orders_score_ties_and_limits_positions() -> None:
    candidates = _candidates(
        [
            _row("600002.SH", 10.0),
            _row("600001.SH", 10.0),
            _row("000001.SZ", 9.0),
        ]
    )

    result = PortfolioConstructor().construct(
        candidates,
        _constraints(max_positions=2),
        _SIGNAL_DATE,
        _EXECUTE_DATE,
    )

    assert [position.instrument_id.canonical() for position in result.positions] == [
        "600001.SH",
        "600002.SH",
    ]
    assert [position.target_weight for position in result.positions] == [0.5, 0.5]
    assert result.cash_weight == 0.0
    assert all(position.reason_code == "SELECTED" for position in result.positions)


def test_constructor_caps_single_names_and_leaves_unallocated_cash() -> None:
    result = PortfolioConstructor().construct(
        _candidates([_row("600001.SH", 10.0)]),
        _constraints(max_position_weight=0.25),
        _SIGNAL_DATE,
        _EXECUTE_DATE,
    )

    assert result.positions[0].target_weight == 0.25
    assert result.cash_weight == 0.75


def test_constructor_rejects_insufficient_liquid_candidates() -> None:
    with pytest.raises(ConstraintViolation) as caught:
        PortfolioConstructor().construct(
            _candidates([_row("600001.SH", 4.0, adv_amount=99.0)]),
            _constraints(min_positions=2, min_adv_amount=100.0),
            _SIGNAL_DATE,
            _EXECUTE_DATE,
        )

    assert caught.value.constraint_name == "min_positions"
    assert caught.value.actual_value == 0
    assert caught.value.boundary == 2


def test_constructor_rejects_turnover_above_constraint() -> None:
    with pytest.raises(ConstraintViolation) as caught:
        PortfolioConstructor().construct(
            _candidates(
                [
                    _row("600001.SH", None, current_weight=1.0),
                    _row("000001.SZ", 3.0),
                ]
            ),
            _constraints(max_turnover=0.5),
            _SIGNAL_DATE,
            _EXECUTE_DATE,
        )

    assert caught.value.constraint_name == "max_turnover"
    assert caught.value.actual_value == pytest.approx(1.0)
    assert caught.value.boundary == 0.5


def test_constructor_exempts_initial_cash_deployment_from_turnover_limit() -> None:
    """首次从全现金建仓不应被再平衡换手上限阻断。"""
    result = PortfolioConstructor().construct(
        _candidates(
            [
                _row("600001.SH", 2.0),
                _row("000001.SZ", 1.0),
            ]
        ),
        _constraints(
            max_position_weight=0.5,
            max_positions=2,
            max_turnover=0.5,
        ),
        _SIGNAL_DATE,
        _EXECUTE_DATE,
    )

    assert [position.target_weight for position in result.positions] == [0.5, 0.5]
    assert result.cash_weight == 0.0


def test_constructor_rejects_non_forward_execution_date() -> None:
    with pytest.raises(ValueError, match="execute_date"):
        PortfolioConstructor().construct(
            _candidates([_row("600001.SH", 1.0)]),
            _constraints(),
            _SIGNAL_DATE,
            _SIGNAL_DATE,
        )


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda frame: frame.with_columns(pl.lit("BAD").alias("instrument_id")),
            "instrument_id",
        ),
        (
            lambda frame: frame.with_columns(pl.lit(float("nan")).alias("adv_amount")),
            "adv_amount",
        ),
        (
            lambda frame: frame.with_columns(pl.lit(-0.1).alias("current_weight")),
            "current_weight",
        ),
    ],
)
def test_constructor_fails_closed_for_invalid_candidate_contract(
    mutate: object, message: str
) -> None:
    base = _candidates([_row("600001.SH", 1.0)])

    with pytest.raises((TypeError, ValueError), match=message):
        PortfolioConstructor().construct(
            mutate(base),  # type: ignore[operator]
            _constraints(),
            _SIGNAL_DATE,
            _EXECUTE_DATE,
        )


def test_constraint_violation_exposes_all_public_fields_in_its_message() -> None:
    error = ConstraintViolation("max_positions", 4, (1, 3))

    assert error.constraint_name == "max_positions"
    assert error.actual_value == 4
    assert error.boundary == (1, 3)
    assert "max_positions" in str(error)
    assert "4" in str(error)
    assert "3" in str(error)


@given(
    scores=st.lists(
        st.floats(
            min_value=-1_000, max_value=1_000, allow_nan=False, allow_infinity=False
        ),
        min_size=1,
        max_size=6,
        unique=True,
    ),
    max_position_weight=st.floats(
        min_value=0.01, max_value=1.0, allow_nan=False, allow_infinity=False
    ),
)
def test_successful_portfolios_keep_weights_nonnegative_and_bounded(
    scores: list[float], max_position_weight: float
) -> None:
    rows = [
        _row(f"{600000 + index:06d}.SH", score) for index, score in enumerate(scores)
    ]
    constraints = _constraints(
        max_position_weight=max_position_weight,
        max_positions=len(rows),
    )

    result = PortfolioConstructor().construct(
        _candidates(rows), constraints, _SIGNAL_DATE, _EXECUTE_DATE + timedelta(days=1)
    )

    total_weight = sum(position.target_weight for position in result.positions)
    assert all(
        isfinite(position.target_weight) and position.target_weight >= 0
        for position in result.positions
    )
    assert total_weight <= 1.0 + 1e-10
    assert result.cash_weight >= 0
    assert total_weight + result.cash_weight == pytest.approx(1.0, abs=1e-10)
