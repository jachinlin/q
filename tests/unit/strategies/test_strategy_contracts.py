"""Review-round contract tests for strategy boundaries and PIT inputs."""

from datetime import UTC, date, datetime

import polars as pl
import pytest

from quant_research.backtest.accounting import AccountSnapshot, PositionSnapshot
from quant_research.backtest.engine import StrategyRef
from quant_research.domain import InstrumentId
from quant_research.portfolio import (
    PortfolioConstraints,
    PortfolioConstructor,
    TargetPortfolio,
    TargetPosition,
)
from quant_research.portfolio.constructor import validate_target_portfolio
from quant_research.strategies.base import (
    PortfolioState,
    RebalanceFrequency,
    StrategyContext,
    StrategyTargetAdapter,
    StrategyValidationError,
    ValidationIssue,
    rebalance_signal_dates,
    validated_factor_values,
    validated_stock_universe,
)
from quant_research.strategies.multifactor import MultifactorConfig, MultifactorStrategy

_DAY = date(2026, 7, 31)
_NEXT = date(2026, 8, 3)
_ID = InstrumentId.parse("600001.SH")


def test_rebalance_signal_dates_match_dates_that_have_a_following_execution_session() -> (
    None
):
    sessions = (
        date(2026, 1, 29),
        date(2026, 1, 30),
        date(2026, 2, 2),
        date(2026, 2, 3),
    )

    assert rebalance_signal_dates(sessions, RebalanceFrequency.DAILY) == sessions[:-1]
    assert rebalance_signal_dates(sessions, RebalanceFrequency.WEEKLY) == (
        date(2026, 1, 30),
    )
    assert rebalance_signal_dates(sessions, RebalanceFrequency.MONTHLY) == (
        date(2026, 1, 30),
    )


class _Data:
    def factor_values(self, *args: object) -> pl.DataFrame:
        raise AssertionError

    def stock_universe(self, *args: object) -> pl.DataFrame:
        raise AssertionError


class _AdapterStrategy:
    strategy_id = "test"

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
    return StrategyContext(_DAY, _NEXT, sessions, _Data(), PortfolioConstructor())


def _factor_frame(**overrides: object) -> pl.DataFrame:
    row = {
        "trade_date": _DAY,
        "instrument_id": _ID.canonical(),
        "factor_ref": "return_20d",
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
        "adv_amount": 1_000.0,
    }
    row.update(overrides)
    return pl.DataFrame(
        [row],
        schema={
            "instrument_id": pl.String,
            "as_of": pl.Date,
            "eligible": pl.Boolean,
            "reason_codes": pl.List(pl.String),
            "adv_amount": pl.Float64,
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
            (
                TargetPosition(_ID, 0.5, None, "X"),
                TargetPosition(_ID, 0.5, None, "Y"),
            ),
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
            factor_refs=("return_20d",),
        )


@pytest.mark.parametrize(
    "frame",
    [
        _universe_frame(reason_codes=["INELIGIBLE"]),
        _universe_frame(eligible=False, reason_codes=[]),
        _universe_frame(adv_amount=float("nan")),
    ],
)
def test_eligible_universe_requires_auditable_evidence(frame: pl.DataFrame) -> None:
    with pytest.raises((TypeError, ValueError)):
        validated_stock_universe(frame, signal_date=_DAY)


def test_config_parser_rejects_nested_factor_definition_keys_before_casting() -> None:
    mapping = {
        "constraints": {
            "max_position_weight": 0.5,
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


def test_config_parser_rejects_removed_industry_constraint() -> None:
    mapping = {
        "constraints": {
            "max_position_weight": 0.5,
            "min_positions": 1,
            "max_positions": 2,
            "min_adv_amount": 1.0,
            "max_turnover": 1.0,
            "max_industry_weight": 0.3,
        }
    }

    with pytest.raises(ValueError, match="unknown constraint key: max_industry_weight"):
        MultifactorConfig.from_mapping(mapping)


def test_multifactor_scores_apply_mad_zscore_direction_then_category_weights() -> None:
    identifiers = [f"60000{index}.SH" for index in range(1, 5)]
    universe = pl.DataFrame(
        {
            "instrument_id": identifiers,
            "as_of": [_DAY] * 4,
            "eligible": [True] * 4,
            "reason_codes": [[] for _ in identifiers],
            "adv_amount": [1_000.0] * 4,
        },
        schema={
            "instrument_id": pl.String,
            "as_of": pl.Date,
            "eligible": pl.Boolean,
            "reason_codes": pl.List(pl.String),
            "adv_amount": pl.Float64,
        },
    )
    factor_refs = (
        "earnings_yield_ttm",
        "book_to_price_mrq",
        "roe_pit",
        "momentum_120_20",
        "volatility_60d",
        "downside_volatility_60d",
        "max_drawdown_120d",
    )
    rows = [
        {
            "trade_date": _DAY,
            "instrument_id": identifier,
            "factor_ref": factor_ref,
            "value": value,
            "available_at": datetime(2026, 7, 31, 7, tzinfo=UTC),
            "is_valid": True,
        }
        for factor_ref in factor_refs
        for identifier, value in zip(identifiers, (1.0, 2.0, 3.0, 100.0), strict=True)
    ]
    factors = pl.DataFrame(
        rows,
        schema={
            "trade_date": pl.Date,
            "instrument_id": pl.String,
            "factor_ref": pl.String,
            "value": pl.Float64,
            "available_at": pl.Datetime("us", "UTC"),
            "is_valid": pl.Boolean,
        },
    )
    config = MultifactorConfig(
        PortfolioConstraints(
            max_position_weight=0.5,
            min_positions=1,
            max_positions=4,
            min_adv_amount=1.0,
            max_turnover=1.0,
        )
    )

    scores, decisions = MultifactorStrategy(config)._scores(universe, factors, _DAY)

    assert [scores[identifier] for identifier in identifiers] == pytest.approx(
        [
            -0.6726915834767423,
            -0.3139227389558131,
            0.04484610556511615,
            0.9417682168674392,
        ]
    )
    assert all(decision.reason_code == "MULTIFACTOR_SELECTED" for decision in decisions)


def test_portfolio_state_conversion_preserves_public_account_snapshot() -> None:
    snapshot = AccountSnapshot(_DAY, 100, (), 0, 100)

    state = PortfolioState.from_account_snapshot(snapshot)

    assert (state.trade_date, state.cash_weight, state.positions) == (_DAY, 1.0, ())


def _adapter(
    strategy: _AdapterStrategy, provider: object | None = None
) -> StrategyTargetAdapter:
    context_provider = provider or (lambda signal, execute: _context())
    return StrategyTargetAdapter({StrategyRef("test"): strategy}, context_provider)  # type: ignore[arg-type]


def test_adapter_rejects_unknown_ref_and_provider_scope_mismatches() -> None:
    target = TargetPortfolio(_DAY, _NEXT, (), 1.0)
    strategy = _AdapterStrategy(target)
    current = AccountSnapshot(_DAY, 100, (), 0, 100)
    with pytest.raises(ValueError, match="unknown"):
        _adapter(strategy).generate_target(StrategyRef("missing"), _DAY, _NEXT, current)
    for provider in (
        lambda signal, execute: StrategyContext(
            _NEXT,
            date(2026, 8, 4),
            (_NEXT, date(2026, 8, 4)),
            _Data(),
            PortfolioConstructor(),
        ),
        lambda signal, execute: StrategyContext(
            _NEXT,
            date(2026, 8, 4),
            (_NEXT, date(2026, 8, 4)),
            _Data(),
            PortfolioConstructor(),
        ),
        lambda signal, execute: StrategyContext(
            _DAY,
            date(2026, 8, 4),
            (_DAY, date(2026, 8, 4)),
            _Data(),
            PortfolioConstructor(),
        ),
    ):
        with pytest.raises(ValueError, match="mismatched"):
            _adapter(strategy, provider).generate_target(
                StrategyRef("test"), _DAY, _NEXT, current
            )


def test_adapter_preserves_validation_issues_and_does_not_generate() -> None:
    issue = ValidationIssue("BAD_CONFIG", "bad setting", "top_n")
    strategy = _AdapterStrategy(TargetPortfolio(_DAY, _NEXT, (), 1.0), issues=[issue])
    with pytest.raises(StrategyValidationError) as caught:
        _adapter(strategy).generate_target(
            StrategyRef("test"),
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
            StrategyRef("test"),
            _DAY,
            _NEXT,
            AccountSnapshot(_DAY, 100, (), 0, 100),
        )
        is None
    )
    with pytest.raises(ValueError, match="snapshot"):
        _adapter(strategy).generate_target(
            StrategyRef("test"),
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
            StrategyRef("test"),
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
        StrategyContext(_DAY, _NEXT, sessions, _Data(), PortfolioConstructor())  # type: ignore[arg-type]


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
