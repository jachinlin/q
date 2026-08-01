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
