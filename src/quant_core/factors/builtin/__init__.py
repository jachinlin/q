"""Registration entry point for built-in versioned factors."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from typing import cast

from quant_core.domain.identifiers import InstrumentId
from quant_core.factors.base import Factor
from quant_core.factors.builtin._stock_common import BarRepository
from quant_core.factors.builtin.auxiliary import (
    AvgAmount20dFactor,
    IndustryCodePitFactor,
    LogMarketCapFactor,
    PitValueProvider,
)
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

_IMPLEMENTATION_REVISION = "etf-market-factors:task6:v1.2:2026-08-01"
_STOCK_IMPLEMENTATION_REVISION = "mvp-stock-factors:task7:v1:2026-08-01"


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
        material = (
            f"{_IMPLEMENTATION_REVISION}|{factor.spec.canonical_ref}|"
            f"{factor.spec.parameters}"
        ).encode()
        registry.register(factor, code_hash=hashlib.sha256(material).hexdigest())


def register_stock_factors(
    registry: FactorRegistry,
    bar_repository: BarRepository,
    financial_provider: FinancialProvider,
    instruments: Sequence[InstrumentId],
    *,
    price_service: AdjustedBarService | None = None,
    shares_provider: PitValueProvider | None = None,
    industry_provider: PitValueProvider | None = None,
    min_abs_net_profit: float = 1e-12,
) -> None:
    """Register all Task 7 alpha and auxiliary identities exactly once."""
    adjusted = price_service or cast(AdjustedBarService, bar_repository)
    factors: tuple[Factor, ...] = (
        EarningsYieldFactor(bar_repository, instruments),
        BookToPriceFactor(bar_repository, instruments),
        RoeAvgPitFactor(financial_provider, instruments),
        CfoToNetProfitFactor(financial_provider, instruments, min_abs_net_profit),
        Momentum12020Factor(adjusted, instruments),
        Volatility60dFactor(adjusted, instruments),
        DownsideVolatility60dFactor(adjusted, instruments),
        MaxDrawdown120dFactor(adjusted, instruments),
        AvgAmount20dFactor(bar_repository, instruments),
        LogMarketCapFactor(bar_repository, instruments, shares_provider),
        IndustryCodePitFactor(instruments, industry_provider),
    )
    for factor in factors:
        try:
            registry.resolve(factor.spec.canonical_ref)
        except ValueError:
            material = (
                f"{_STOCK_IMPLEMENTATION_REVISION}|{factor.spec.canonical_ref}|"
                f"{factor.spec.parameters}"
            ).encode()
            registry.register(factor, code_hash=hashlib.sha256(material).hexdigest())


__all__ = [
    "AdjustedBarService",
    "Momentum12020Factor",
    "ReturnFactor",
    "Trend120dFactor",
    "Volatility60dFactor",
    "register_etf_factors",
    "register_stock_factors",
]
