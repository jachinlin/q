"""Registration entry point for built-in versioned factors."""

from __future__ import annotations

from collections.abc import Sequence

from quant_core.domain.identifiers import InstrumentId
from quant_core.factors.base import Factor
from quant_core.factors.builtin._stock_common import BarRepository
from quant_core.factors.builtin.auxiliary import (
    AvgAmount20dFactor,
    IndustryCodePitFactor,
    LogMarketCapFactor,
    PitValueProvider,
)
from quant_core.factors.builtin.code_hash import builtin_source_hash
from quant_core.factors.builtin.momentum import (
    AdjustedBarService,
    Momentum12020Factor,
    ReturnFactor,
    Trend120dFactor,
)
from quant_core.factors.builtin.quality import (
    CfoToNetProfitFactor,
    FinancialProvider,
    RoeAvgPitFactor,
)
from quant_core.factors.builtin.risk import (
    DownsideVolatility60dFactor,
    MaxDrawdown120dFactor,
    Volatility60dFactor,
)
from quant_core.factors.builtin.valuation import BookToPriceFactor, EarningsYieldFactor
from quant_core.factors.registry import FactorRegistry

_RUNTIME_DEPENDENCY_ATTRIBUTES = (
    "_price_service",
    "_repository",
    "_service",
    "_provider",
    "_calendar",
)


def _builtin_runtime_identity(factor: Factor) -> tuple[object, ...]:
    """Identify the concrete providers and instrument domain captured by a factor."""
    identity: list[object] = [type(factor).__module__, type(factor).__qualname__]
    instruments = getattr(factor, "_instruments", ())
    identity.append(
        tuple(
            instrument.canonical()
            if isinstance(instrument, InstrumentId)
            else instrument
            for instrument in instruments
        )
    )
    identity.extend(
        (attribute, id(getattr(factor, attribute)))
        for attribute in _RUNTIME_DEPENDENCY_ATTRIBUTES
        if hasattr(factor, attribute)
    )
    return tuple(identity)


def register_builtin(registry: FactorRegistry, factor: Factor) -> None:
    """Register a bundled factor once, rejecting divergent same-ref code."""
    expected_hash = builtin_source_hash(factor.spec)
    try:
        existing_ref = registry.resolve(factor.spec.canonical_ref)
    except ValueError:
        registry.register(factor, code_hash=expected_hash)
        return
    if registry.code_hash(existing_ref) != expected_hash:
        raise ValueError(
            f"conflicting built-in implementation: {factor.spec.canonical_ref}"
        )
    existing = registry.factor(existing_ref)
    if _builtin_runtime_identity(existing) != _builtin_runtime_identity(factor):
        raise ValueError(
            f"conflicting built-in runtime dependencies: {factor.spec.canonical_ref}"
        )


def register_etf_factors(
    registry: FactorRegistry,
    price_service: AdjustedBarService,
    instruments: Sequence[InstrumentId],
) -> None:
    """Register the five exact Task 6 ETF market-factor identities."""
    factors: tuple[Factor, ...] = (
        ReturnFactor(price_service, instruments, 20),
        ReturnFactor(price_service, instruments, 60),
        ReturnFactor(price_service, instruments, 120),
        Trend120dFactor(price_service, instruments),
        Volatility60dFactor(price_service, instruments),
    )
    for factor in factors:
        register_builtin(registry, factor)


def register_stock_factors(
    registry: FactorRegistry,
    bar_repository: BarRepository,
    financial_provider: FinancialProvider,
    instruments: Sequence[InstrumentId],
    *,
    price_service: AdjustedBarService,
    shares_provider: PitValueProvider | None = None,
    industry_provider: PitValueProvider | None = None,
    min_abs_net_profit: float = 1e-12,
) -> None:
    """Register all Task 7 alpha and auxiliary identities exactly once."""
    factors: tuple[Factor, ...] = (
        EarningsYieldFactor(bar_repository, instruments),
        BookToPriceFactor(bar_repository, instruments),
        RoeAvgPitFactor(financial_provider, instruments),
        CfoToNetProfitFactor(financial_provider, instruments, min_abs_net_profit),
        Momentum12020Factor(price_service, instruments),
        Volatility60dFactor(price_service, instruments),
        DownsideVolatility60dFactor(price_service, instruments),
        MaxDrawdown120dFactor(price_service, instruments),
        AvgAmount20dFactor(bar_repository, instruments),
        LogMarketCapFactor(
            bar_repository,
            instruments,
            shares_provider,
            calendar_provider=financial_provider,
        ),
        IndustryCodePitFactor(
            instruments, industry_provider, calendar_provider=financial_provider
        ),
    )
    for factor in factors:
        register_builtin(registry, factor)


__all__ = [
    "AdjustedBarService",
    "Momentum12020Factor",
    "ReturnFactor",
    "Trend120dFactor",
    "Volatility60dFactor",
    "register_builtin",
    "register_etf_factors",
    "register_stock_factors",
]
