"""Review-round contract tests for strategy boundaries and PIT inputs."""

from datetime import UTC, date, datetime
from uuid import UUID

import polars as pl
import pytest

from quant_core.backtest.accounting import AccountSnapshot, PositionSnapshot
from quant_core.backtest.engine import StrategyRef
from quant_core.domain import InstrumentId
from quant_core.portfolio import PortfolioConstructor, TargetPortfolio, TargetPosition
from quant_core.portfolio.constructor import validate_target_portfolio
from quant_core.strategies.base import (
    PortfolioState,
    StrategyContext,
    StrategyTargetAdapter,
    StrategyValidationError,
    ValidationIssue,
    validated_factor_values,
    validated_stock_universe,
)
from quant_core.strategies.multifactor import MultifactorConfig

_DAY = date(2026, 7, 31)
_NEXT = date(2026, 8, 3)
_SNAPSHOT = UUID("00000000-0000-0000-0000-000000000611")
_ID = InstrumentId.parse("SSE:600001")


class _Data:
    def factor_values(self, *args: object) -> pl.DataFrame:
        raise AssertionError

    def stock_universe(self, *args: object) -> pl.DataFrame:
        raise AssertionError


class _AdapterStrategy:
    strategy_id = "test"
    version = "1"

    def __init__(
        self,
        target: TargetPortfolio | None,
        *,
        issues: list[ValidationIssue] | None = None,
        rebalance: bool = True,
    ) -> None:
        self.target = target
        self.issues = issues or []
        self.rebalance = rebalance
        self.generated = 0

    def validate(self, ctx: StrategyContext) -> list[ValidationIssue]:
        return self.issues

    def should_rebalance(self, ctx: StrategyContext, rebalance_date: date) -> bool:
        return self.rebalance

    def generate_targets(
        self, ctx: StrategyContext, rebalance_date: date, current: PortfolioState
    ) -> TargetPortfolio:
        self.generated += 1
        if self.target is None:
            raise AssertionError
        return self.target


def _context(*, sessions: tuple[date, ...] = (_DAY, _NEXT)) -> StrategyContext:
    return StrategyContext(
        _SNAPSHOT, _DAY, _NEXT, sessions, _Data(), PortfolioConstructor()
    )


def _factor_frame(**overrides: object) -> pl.DataFrame:
    row = {
        "trade_date": _DAY,
        "instrument_id": _ID.canonical(),
        "factor_ref": "return_20d_v1@1.0.0",
        "value": 1.0,
        "available_at": datetime(2026, 7, 31, 7, tzinfo=UTC),
        "is_valid": True,
    }
    row.update(overrides)
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


def _universe_frame(**overrides: object) -> pl.DataFrame:
    row = {
        "instrument_id": _ID.canonical(),
        "as_of": _DAY,
        "eligible": True,
        "reason_codes": [],
        "industry": "BANK",
        "adv_amount": 1_000.0,
        "log_market_cap": 10.0,
    }
    row.update(overrides)
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
    "target",
    [
        TargetPortfolio(_DAY, _NEXT, (), -0.1),
        TargetPortfolio(
            _DAY, _NEXT, (TargetPosition(_ID, float("nan"), None, "X"),), 0.0
        ),
        TargetPortfolio(_DAY, _NEXT, (TargetPosition(_ID, 0.5, None, ""),), 0.5),
        TargetPortfolio(
            _DAY,
            _NEXT,
            (TargetPosition(_ID, 0.5, None, "X"), TargetPosition(_ID, 0.5, None, "Y")),
            0.0,
        ),
        TargetPortfolio(_DAY, _NEXT, (), 0.9),
    ],
)
def test_shared_target_validator_rejects_malformed_targets(
    target: TargetPortfolio,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        validate_target_portfolio(target, _DAY, _NEXT)


def test_context_requires_execute_to_be_the_immediate_next_session() -> None:
    with pytest.raises(ValueError, match="next actual session"):
        _context(sessions=(_DAY, date(2026, 8, 1), _NEXT))


@pytest.mark.parametrize(
    "frame",
    [
        _factor_frame(available_at=datetime(2026, 8, 1, tzinfo=UTC)),
        _factor_frame(value=float("nan")),
        _factor_frame(is_valid=None),
        _factor_frame(trade_date=date(2026, 7, 30)),
    ],
)
def test_factor_data_fails_closed_for_pit_and_audit_contracts(
    frame: pl.DataFrame,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        validated_factor_values(
            frame,
            signal_date=_DAY,
            instruments=(_ID,),
            factor_refs=("return_20d_v1@1.0.0",),
        )


@pytest.mark.parametrize(
    "frame",
    [
        _universe_frame(reason_codes=["INELIGIBLE"]),
        _universe_frame(eligible=False, reason_codes=[]),
        _universe_frame(industry=None),
        _universe_frame(adv_amount=float("nan")),
        _universe_frame(log_market_cap=float("inf")),
    ],
)
def test_eligible_universe_requires_auditable_evidence(frame: pl.DataFrame) -> None:
    with pytest.raises((TypeError, ValueError)):
        validated_stock_universe(frame, signal_date=_DAY)


def test_config_parser_rejects_nested_factor_definition_keys_before_casting() -> None:
    mapping = {
        "constraints": {
            "max_position_weight": 0.5,
            "max_industry_weight": 0.8,
            "min_positions": 1,
            "max_positions": 2,
            "min_adv_amount": 1.0,
            "max_turnover": 1.0,
        },
        "factor_definitions": {
            "earnings_yield_ttm_v1@1.0.0": {
                "category": "VALUE",
                "direction": 1,
                "extra": True,
            }
        },
    }

    with pytest.raises((TypeError, ValueError), match="definition"):
        MultifactorConfig.from_mapping(mapping)


def test_portfolio_state_conversion_preserves_public_account_snapshot() -> None:
    snapshot = AccountSnapshot(_DAY, 100, (), 0, 100)

    state = PortfolioState.from_account_snapshot(snapshot)

    assert (state.trade_date, state.cash_weight, state.positions) == (_DAY, 1.0, ())


def _adapter(
    strategy: _AdapterStrategy, provider: object | None = None
) -> StrategyTargetAdapter:
    context_provider = provider or (lambda snapshot, signal, execute: _context())
    return StrategyTargetAdapter({StrategyRef("test", "1"): strategy}, context_provider)  # type: ignore[arg-type]


def test_adapter_rejects_unknown_ref_and_provider_scope_mismatches() -> None:
    target = TargetPortfolio(_DAY, _NEXT, (), 1.0)
    strategy = _AdapterStrategy(target)
    current = AccountSnapshot(_DAY, 100, (), 0, 100)
    with pytest.raises(ValueError, match="unknown"):
        _adapter(strategy).generate_target(
            StrategyRef("missing", "1"), _SNAPSHOT, _DAY, _NEXT, current
        )
    for provider in (
        lambda snapshot, signal, execute: StrategyContext(
            UUID(int=1), _DAY, _NEXT, (_DAY, _NEXT), _Data(), PortfolioConstructor()
        ),
        lambda snapshot, signal, execute: StrategyContext(
            _SNAPSHOT,
            _NEXT,
            date(2026, 8, 4),
            (_NEXT, date(2026, 8, 4)),
            _Data(),
            PortfolioConstructor(),
        ),
        lambda snapshot, signal, execute: StrategyContext(
            _SNAPSHOT,
            _DAY,
            date(2026, 8, 4),
            (_DAY, date(2026, 8, 4)),
            _Data(),
            PortfolioConstructor(),
        ),
    ):
        with pytest.raises(ValueError, match="mismatched"):
            _adapter(strategy, provider).generate_target(
                StrategyRef("test", "1"), _SNAPSHOT, _DAY, _NEXT, current
            )


def test_adapter_preserves_validation_issues_and_does_not_generate() -> None:
    issue = ValidationIssue("BAD_CONFIG", "bad setting", "top_n")
    strategy = _AdapterStrategy(TargetPortfolio(_DAY, _NEXT, (), 1.0), issues=[issue])
    with pytest.raises(StrategyValidationError) as caught:
        _adapter(strategy).generate_target(
            StrategyRef("test", "1"),
            _SNAPSHOT,
            _DAY,
            _NEXT,
            AccountSnapshot(_DAY, 100, (), 0, 100),
        )
    assert caught.value.issues == (issue,)
    assert strategy.generated == 0


def test_adapter_returns_none_for_non_rebalance_and_rejects_stale_snapshot() -> None:
    strategy = _AdapterStrategy(TargetPortfolio(_DAY, _NEXT, (), 1.0), rebalance=False)
    assert (
        _adapter(strategy).generate_target(
            StrategyRef("test", "1"),
            _SNAPSHOT,
            _DAY,
            _NEXT,
            AccountSnapshot(_DAY, 100, (), 0, 100),
        )
        is None
    )
    with pytest.raises(ValueError, match="snapshot"):
        _adapter(strategy).generate_target(
            StrategyRef("test", "1"),
            _SNAPSHOT,
            _DAY,
            _NEXT,
            AccountSnapshot(date(2026, 7, 30), 100, (), 0, 100),
        )


@pytest.mark.parametrize(
    "target",
    [
        TargetPortfolio(date(2026, 7, 30), _NEXT, (), 1.0),
        TargetPortfolio(
            _DAY,
            _NEXT,
            (TargetPosition(_ID, -0.1, None, "X"),),
            1.1,
        ),
        TargetPortfolio(
            _DAY,
            _NEXT,
            (
                TargetPosition(_ID, 0.5, None, "X"),
                TargetPosition(_ID, 0.5, None, "Y"),
            ),
            0.0,
        ),
    ],
)
def test_adapter_routes_invalid_targets_through_shared_validator(
    target: TargetPortfolio,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        _adapter(_AdapterStrategy(target)).generate_target(
            StrategyRef("test", "1"),
            _SNAPSHOT,
            _DAY,
            _NEXT,
            AccountSnapshot(_DAY, 100, (), 0, 100),
        )


@pytest.mark.parametrize("value", [datetime(2026, 7, 31, tzinfo=UTC), None])
def test_context_rejects_datetime_sessions_and_non_tuple_sessions(
    value: object,
) -> None:
    sessions = [_DAY, _NEXT] if value is None else (value, _NEXT)
    with pytest.raises((TypeError, ValueError)):
        StrategyContext(
            _SNAPSHOT, _DAY, _NEXT, sessions, _Data(), PortfolioConstructor()
        )  # type: ignore[arg-type]


def test_state_conversion_preserves_position_quantity_value_and_weight() -> None:
    position = PositionSnapshot(_ID, 10, 10, 500, 900)
    snapshot = AccountSnapshot(_DAY, 100, (position,), 900, 1_000)
    state = PortfolioState.from_account_snapshot(snapshot)
    assert (
        state.positions[0].instrument_id,
        state.positions[0].quantity,
        state.positions[0].market_value_fen,
        state.positions[0].current_weight,
    ) == (_ID, 10, 900, 0.9)
