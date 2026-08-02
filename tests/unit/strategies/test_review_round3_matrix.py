"""Remaining independent review-matrix boundaries for strategy contracts."""

from datetime import UTC, date, datetime
from uuid import UUID

import polars as pl
import pytest

from quant_core.backtest.accounting import AccountSnapshot
from quant_core.backtest.engine import StrategyRef
from quant_core.domain import InstrumentId
from quant_core.portfolio import PortfolioConstructor, TargetPortfolio
from quant_core.strategies.base import (
    PortfolioPosition,
    PortfolioState,
    StrategyContext,
    StrategyTargetAdapter,
    StrategyValidationError,
    validated_factor_values,
    validated_stock_universe,
)

_DAY = date(2026, 7, 31)
_NEXT = date(2026, 8, 3)
_SNAPSHOT = UUID("00000000-0000-0000-0000-000000000612")
_ID = InstrumentId.parse("SSE:600001")
_ID_2 = InstrumentId.parse("SSE:600002")


class _Data:
    def factor_values(self, *args: object) -> pl.DataFrame:
        raise AssertionError

    def stock_universe(self, *args: object) -> pl.DataFrame:
        raise AssertionError


class _Strategy:
    strategy_id = "round3"
    version = "1"

    def __init__(self, validate_result: object = None) -> None:
        self.validate_result = [] if validate_result is None else validate_result

    def validate(self, ctx: StrategyContext) -> object:
        return self.validate_result

    def should_rebalance(self, ctx: StrategyContext, rebalance_date: date) -> bool:
        return True

    def generate_targets(
        self, ctx: StrategyContext, rebalance_date: date, current: PortfolioState
    ) -> TargetPortfolio:
        return TargetPortfolio(_DAY, _NEXT, (), 1.0)


def _context() -> StrategyContext:
    return StrategyContext(
        _SNAPSHOT, _DAY, _NEXT, (_DAY, _NEXT), _Data(), PortfolioConstructor()
    )


def _adapter(
    strategy: _Strategy, provider: object | None = None
) -> StrategyTargetAdapter:
    return StrategyTargetAdapter(
        {StrategyRef("round3", "1"): strategy},
        provider or (lambda *args: _context()),  # type: ignore[arg-type]
    )


def _account() -> AccountSnapshot:
    return AccountSnapshot(_DAY, 100, (), 0, 100)


def _position(
    instrument: InstrumentId = _ID,
    *,
    market_value_fen: int = 40,
    current_weight: float = 0.4,
) -> PortfolioPosition:
    return PortfolioPosition(instrument, 100, market_value_fen, current_weight)


def _valid_state(**change: object) -> PortfolioState:
    values: dict[str, object] = {
        "trade_date": _DAY,
        "cash_fen": 20,
        "nav_fen": 100,
        "total_market_value_fen": 80,
        "positions": (
            _position(),
            _position(_ID_2, market_value_fen=40, current_weight=0.4),
        ),
        "cash_weight": 0.2,
    }
    values.update(change)
    return PortfolioState(**values)  # type: ignore[arg-type]


def _factor(**change: object) -> pl.DataFrame:
    row = {
        "trade_date": _DAY,
        "instrument_id": _ID.canonical(),
        "factor_ref": "x@1",
        "value": 1.0,
        "available_at": datetime(2026, 7, 31, tzinfo=UTC),
        "is_valid": True,
    }
    row.update(change)
    return pl.DataFrame(
        [row],
        schema={
            "trade_date": pl.Date,
            "instrument_id": pl.String,
            "factor_ref": pl.String,
            "value": pl.Float64,
            "available_at": pl.Datetime("us", "UTC"),
            "is_valid": pl.Boolean,
        },
    )


def _universe(**change: object) -> pl.DataFrame:
    row = {
        "instrument_id": _ID.canonical(),
        "as_of": _DAY,
        "eligible": True,
        "reason_codes": [],
        "industry": "BANK",
        "adv_amount": 1.0,
        "log_market_cap": 1.0,
    }
    row.update(change)
    return pl.DataFrame(
        [row],
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


def _universe_pair(**change: object) -> pl.DataFrame:
    rows = [
        {
            "instrument_id": _ID.canonical(),
            "as_of": _DAY,
            "eligible": True,
            "reason_codes": [],
            "industry": "BANK",
            "adv_amount": 1.0,
            "log_market_cap": 1.0,
        },
        {
            "instrument_id": _ID_2.canonical(),
            "as_of": _DAY,
            "eligible": True,
            "reason_codes": [],
            "industry": "TECH",
            "adv_amount": 2.0,
            "log_market_cap": 2.0,
        },
    ]
    for row in rows:
        row.update(change)
    return pl.DataFrame(
        rows,
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


@pytest.mark.parametrize(
    "reference", [StrategyRef("round3", "2"), StrategyRef("other", "1")]
)
def test_adapter_rejects_unknown_strategy_versions(reference: StrategyRef) -> None:
    with pytest.raises(ValueError, match="unknown"):
        _adapter(_Strategy()).generate_target(
            reference, _SNAPSHOT, _DAY, _NEXT, _account()
        )


@pytest.mark.parametrize("result", [(), [object()]])
def test_adapter_rejects_nonconforming_validation_results(result: object) -> None:
    with pytest.raises(StrategyValidationError):
        _adapter(_Strategy(result)).generate_target(
            StrategyRef("round3", "1"), _SNAPSHOT, _DAY, _NEXT, _account()
        )


def test_adapter_rejects_non_context_provider_result_and_registry_identity_mismatch() -> (
    None
):
    with pytest.raises(ValueError, match="context"):
        _adapter(_Strategy(), lambda *args: object()).generate_target(
            StrategyRef("round3", "1"), _SNAPSHOT, _DAY, _NEXT, _account()
        )
    strategy = _Strategy()
    strategy.strategy_id = "different"
    with pytest.raises(ValueError, match="registry"):
        StrategyTargetAdapter(
            {StrategyRef("round3", "1"): strategy}, lambda *args: _context()
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"snapshot_id": "not-a-uuid"},
        {"signal_date": datetime(2026, 7, 31, tzinfo=UTC)},
        {"execute_date": datetime(2026, 8, 3, tzinfo=UTC)},
        {"sessions": ()},
        {"sessions": (_NEXT, _DAY)},
        {"sessions": (_DAY, _DAY, _NEXT)},
        {"sessions": (_NEXT,)},
        {"sessions": (_DAY,)},
        {"data": None},
        {"portfolio_constructor": object()},
    ],
)
def test_context_rejects_each_invalid_binding(kwargs: dict[str, object]) -> None:
    values: dict[str, object] = {
        "snapshot_id": _SNAPSHOT,
        "signal_date": _DAY,
        "execute_date": _NEXT,
        "sessions": (_DAY, _NEXT),
        "data": _Data(),
        "portfolio_constructor": PortfolioConstructor(),
    }
    values.update(kwargs)
    with pytest.raises((TypeError, ValueError)):
        StrategyContext(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "position",
    [
        (object(), 1, 1, 1.0),
        (_ID, 0, 1, 1.0),
        (_ID, 1, 0, 1.0),
        (_ID, 1, 1, float("nan")),
        (_ID, 1, 1, -0.1),
        (_ID, 1, 1, 1.1),
    ],
)
def test_portfolio_position_rejects_invalid_fields(
    position: tuple[object, object, object, object],
) -> None:
    with pytest.raises((TypeError, ValueError)):
        PortfolioPosition(*position)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "frame, refs, instruments",
    [
        (_factor(factor_ref="unknown@1"), ("x@1",), (_ID,)),
        (_factor(instrument_id="SSE:600002"), ("x@1",), (_ID,)),
        (_factor(instrument_id="BAD"), ("x@1",), (_ID,)),
        (
            _factor(available_at=datetime(2026, 8, 1, tzinfo=UTC), is_valid=False),
            ("x@1",),
            (_ID,),
        ),
        (_factor(value=None), ("x@1",), (_ID,)),
        (_factor(value=float("inf")), ("x@1",), (_ID,)),
    ],
)
def test_factor_matrix_rejects_one_contract_violation(
    frame: pl.DataFrame, refs: tuple[str, ...], instruments: tuple[InstrumentId, ...]
) -> None:
    with pytest.raises((TypeError, ValueError)):
        validated_factor_values(
            frame, signal_date=_DAY, instruments=instruments, factor_refs=refs
        )


def test_factor_matrix_rejects_duplicate_primary_key() -> None:
    frame = pl.concat([_factor(), _factor()])
    with pytest.raises(ValueError, match="unique"):
        validated_factor_values(
            frame, signal_date=_DAY, instruments=(_ID,), factor_refs=("x@1",)
        )


@pytest.mark.parametrize(
    "frame",
    [
        _universe(as_of=date(2026, 7, 30)),
        _universe(eligible=None),
        _universe(reason_codes=None),
        _universe(reason_codes=[""]),
        _universe(adv_amount=-1.0),
        _universe(adv_amount=float("inf")),
        _universe(log_market_cap=None),
        _universe(log_market_cap=float("nan")),
    ],
)
def test_universe_matrix_rejects_one_contract_violation(frame: pl.DataFrame) -> None:
    with pytest.raises((TypeError, ValueError)):
        validated_stock_universe(frame, signal_date=_DAY)


def test_adapter_rejects_signal_only_provider_mismatch_and_accepts_valid_target() -> (
    None
):
    provider = lambda *args: StrategyContext(
        _SNAPSHOT,
        date(2026, 7, 30),
        _NEXT,
        (date(2026, 7, 30), _NEXT),
        _Data(),
        PortfolioConstructor(),
    )
    with pytest.raises(ValueError, match="mismatched"):
        _adapter(_Strategy(), provider).generate_target(
            StrategyRef("round3", "1"), _SNAPSHOT, _DAY, _NEXT, _account()
        )
    target = _adapter(_Strategy()).generate_target(
        StrategyRef("round3", "1"), _SNAPSHOT, _DAY, _NEXT, _account()
    )
    assert target == TargetPortfolio(_DAY, _NEXT, (), 1.0)


@pytest.mark.parametrize(
    "state",
    [
        (datetime(2026, 7, 31, tzinfo=UTC), 100, 100, 0, (), 1.0),
        (_DAY, -1, 100, 0, (), 1.0),
        (_DAY, 100, 0, 0, (), 1.0),
        (_DAY, 100, 100, 1, (), 1.0),
        (_DAY, 100, 100, 0, [], 1.0),
        (_DAY, 100, 100, 0, (), 0.9),
    ],
)
def test_portfolio_state_direct_construction_rejects_core_invariants(
    state: tuple[object, object, object, object, object, object],
) -> None:
    with pytest.raises((TypeError, ValueError)):
        PortfolioState(*state)  # type: ignore[arg-type]


def test_context_rejects_execute_not_after_signal() -> None:
    with pytest.raises(ValueError, match="execute_date"):
        StrategyContext(_SNAPSHOT, _DAY, _DAY, (_DAY,), _Data(), PortfolioConstructor())


def test_adapter_rejects_non_ref_and_non_snapshot_inputs() -> None:
    adapter = _adapter(_Strategy())
    with pytest.raises(TypeError, match="StrategyRef"):
        adapter.generate_target("round3", _SNAPSHOT, _DAY, _NEXT, _account())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="AccountSnapshot"):
        adapter.generate_target(
            StrategyRef("round3", "1"), _SNAPSHOT, _DAY, _NEXT, object()
        )  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "state",
    [
        (_DAY, 1.0, 100, 0, (), 1.0),
        (_DAY, 100, 1.0, 0, (), 1.0),
        (_DAY, 100, 100, -1, (), 1.0),
        (_DAY, 100, 100, 0, (object(),), 1.0),
    ],
)
def test_state_rejects_each_direct_numeric_and_position_invariant(
    state: tuple[object, object, object, object, object, object],
) -> None:
    with pytest.raises((TypeError, ValueError)):
        PortfolioState(*state)  # type: ignore[arg-type]


def test_state_rejects_noninteger_total_market_value_and_negative_nav() -> None:
    with pytest.raises(ValueError, match="total_market_value_fen"):
        _valid_state(total_market_value_fen=80.0)
    with pytest.raises(ValueError, match="nav_fen"):
        _valid_state(nav_fen=-100)


def test_state_rejects_duplicate_and_unsorted_positions() -> None:
    first = _position()
    duplicate = _position()
    with pytest.raises(ValueError, match="unique"):
        _valid_state(positions=(first, duplicate))
    with pytest.raises(ValueError, match="sorted"):
        _valid_state(positions=(_position(_ID_2), first))


def test_state_rejects_each_weight_and_market_sum_invariant() -> None:
    with pytest.raises(ValueError, match="must equal positions"):
        _valid_state(
            positions=(
                _position(market_value_fen=30, current_weight=0.3),
                _position(_ID_2, market_value_fen=30, current_weight=0.3),
            )
        )
    with pytest.raises(ValueError, match="weights must sum"):
        _valid_state(
            positions=(
                _position(current_weight=0.3),
                _position(_ID_2, current_weight=0.3),
            )
        )
    with pytest.raises(ValueError, match="position weight"):
        _valid_state(
            cash_weight=0.2,
            positions=(
                _position(current_weight=0.3),
                _position(_ID_2, current_weight=0.5),
            ),
        )


@pytest.mark.parametrize(
    "frame",
    [
        _factor().drop("value"),
        _factor().with_columns(pl.lit("x").alias("extra")),
        _factor().with_columns(pl.col("value").cast(pl.Float32)),
        _factor().with_columns(pl.col("trade_date").cast(pl.Datetime)),
    ],
)
def test_factor_matrix_rejects_exact_schema_or_dtype_mutation(
    frame: pl.DataFrame,
) -> None:
    with pytest.raises(ValueError, match="schema"):
        validated_factor_values(
            frame, signal_date=_DAY, instruments=(_ID,), factor_refs=("x@1",)
        )


def test_factor_matrix_rejects_null_availability_and_duplicate_request_refs() -> None:
    with pytest.raises(ValueError, match="not available"):
        validated_factor_values(
            _factor(available_at=None),
            signal_date=_DAY,
            instruments=(_ID,),
            factor_refs=("x@1",),
        )
    with pytest.raises(ValueError, match="unique"):
        validated_factor_values(
            _factor(),
            signal_date=_DAY,
            instruments=(_ID,),
            factor_refs=("x@1", "x@1"),
        )


@pytest.mark.parametrize(
    "frame, message",
    [
        (_universe_pair().reverse(), "sorted"),
        (
            _universe_pair().with_columns(
                pl.lit(_ID.canonical()).alias("instrument_id")
            ),
            "unique",
        ),
        (
            _universe_pair().with_columns(
                pl.when(pl.col("instrument_id") == _ID.canonical())
                .then(pl.lit("BAD"))
                .otherwise(pl.col("instrument_id"))
                .alias("instrument_id")
            ),
            r"^instrument_id must be canonical$",
        ),
    ],
)
def test_universe_rejects_identity_order_and_canonicality_mutations(
    frame: pl.DataFrame, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        validated_stock_universe(frame, signal_date=_DAY)


@pytest.mark.parametrize(
    "frame, message",
    [
        (_universe(reason_codes=[None]), "reason_codes"),
        (_universe(adv_amount=None), "adv_amount"),
        (_universe(industry=""), "industry"),
    ],
)
def test_universe_rejects_null_or_empty_eligible_evidence(
    frame: pl.DataFrame, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        validated_stock_universe(frame, signal_date=_DAY)
