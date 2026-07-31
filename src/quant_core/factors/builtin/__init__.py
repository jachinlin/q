"""Registration entry point for built-in versioned factors."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence

from quant_core.domain.identifiers import InstrumentId
from quant_core.factors.base import Factor
from quant_core.factors.builtin.momentum import (
    AdjustedBarService,
    ReturnFactor,
    Trend120dFactor,
)
from quant_core.factors.builtin.risk import Volatility60dFactor
from quant_core.factors.registry import FactorRegistry

_IMPLEMENTATION_REVISION = "etf-market-factors:task6:v1.2:2026-08-01"


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


__all__ = [
    "AdjustedBarService",
    "ReturnFactor",
    "Trend120dFactor",
    "Volatility60dFactor",
    "register_etf_factors",
]
