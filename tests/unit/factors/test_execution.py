from __future__ import annotations

from datetime import UTC, date, datetime

import polars as pl

from quant_research.data.contracts import ProviderCapabilities
from quant_research.domain.enums import DatasetKind
from quant_research.factors.base import (
    FACTOR_OUTPUT_SCHEMA,
    FactorContext,
    FactorSpec,
)
from quant_research.factors.builtin import register_stock_factors
from quant_research.factors.registry import FactorEngine, FactorRegistry


class _CountingFactor:
    def __init__(self) -> None:
        self.calls = 0
        self.spec = FactorSpec(
            factor_id="counting_factor",
            frequency="daily",
            lookback_sessions=0,
            dependencies=(),
            direction=1,
            parameters={},
        )

    def compute(self, ctx: FactorContext) -> pl.LazyFrame:
        self.calls += 1
        return pl.DataFrame(
            {
                "trade_date": [ctx.start],
                "instrument_id": ["000001.SZ"],
                "factor_id": [self.spec.factor_id],
                "value": [1.0],
                "available_at": [datetime(2026, 4, 30, tzinfo=UTC)],
                "is_valid": [True],
            },
            schema=FACTOR_OUTPUT_SCHEMA,
        ).lazy()


def test_factor_engine_recomputes_instead_of_reusing_a_cache() -> None:
    factor = _CountingFactor()
    registry = FactorRegistry()
    registry.register(factor, code_hash="c" * 64)
    engine = FactorEngine(registry, capabilities=ProviderCapabilities.complete())
    context = FactorContext(
        data_hash="a" * 64,
        universe_hash="b" * 64,
        start=date(2026, 4, 30),
        end=date(2026, 4, 30),
    )

    first = engine.compute(("counting_factor",), context)["counting_factor"]
    second = engine.compute(("counting_factor",), context)["counting_factor"]

    assert factor.calls == 2
    assert first.content_hash == second.content_hash
    assert not hasattr(first, "cache_key")


def test_stock_factor_registration_has_no_industry_dependency() -> None:
    registry = FactorRegistry()
    provider = object()

    register_stock_factors(  # type: ignore[arg-type]
        registry,
        provider,
        provider,
        (),
        price_service=provider,
    )

    assert registry.registered_references() == (
        "avg_amount_20d",
        "book_to_price_mrq",
        "downside_volatility_60d",
        "earnings_yield_ttm",
        "max_drawdown_120d",
        "momentum_120_20",
        "roe_pit",
        "volatility_60d",
    )
    assert all(
        not registry.spec(reference).required_datasets
        for reference in registry.registered_references()
    )


def test_factor_spec_can_explicitly_declare_industry_dataset_dependency() -> None:
    spec = FactorSpec(
        factor_id="industry_neutralized_value",
        frequency="daily",
        lookback_sessions=0,
        dependencies=(),
        direction=1,
        parameters={
            "taxonomy": "证监会行业分类",
            "unclassified_policy": "EXCLUDE",
        },
        required_datasets=(DatasetKind.INDUSTRY_CLASSIFICATION,),
    )

    assert spec.required_datasets == (DatasetKind.INDUSTRY_CLASSIFICATION,)
