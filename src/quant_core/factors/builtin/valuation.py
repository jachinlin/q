"""Snapshot-bound valuation factors from daily bar multiples."""

from __future__ import annotations

from collections.abc import Sequence
from math import isfinite

import polars as pl

from quant_core.domain.identifiers import InstrumentId
from quant_core.factors.base import FactorContext, FactorSpec
from quant_core.factors.builtin._stock_common import (
    BarRepository,
    canonical_scope,
    output_frame,
)

_VERSION = "1.0.0"


class _ReciprocalMultipleFactor:
    def __init__(
        self,
        repository: BarRepository,
        instruments: Sequence[InstrumentId],
        *,
        factor_id: str,
        field: str,
    ) -> None:
        self._repository = repository
        self._instruments = canonical_scope(instruments)
        self._field = field
        self._spec = FactorSpec(
            factor_id,
            _VERSION,
            "daily",
            0,
            (),
            1,
            {
                "source_field": field,
                "formula": f"1/{field}",
                "eligible_for_alpha": True,
            },
        )

    @property
    def spec(self) -> FactorSpec:
        return self._spec

    def compute(self, ctx: FactorContext) -> pl.LazyFrame:
        frame = self._repository.bars(
            ctx.snapshot_id, self._instruments, ctx.start, ctx.end
        ).collect()
        required = {"trade_date", "instrument_id", self._field, "available_at"}
        if not required.issubset(frame.columns):
            raise ValueError("valuation bars missing required columns")
        if frame.select(
            pl.struct("trade_date", "instrument_id").is_duplicated().any()
        ).item():
            raise ValueError("duplicate valuation bar key")
        rows = []
        for row in frame.select(
            "trade_date", "instrument_id", self._field, "available_at"
        ).to_dicts():
            denominator = row[self._field]
            value = (
                1.0 / float(denominator)
                if isinstance(denominator, (int, float))
                and not isinstance(denominator, bool)
                and isfinite(denominator)
                and denominator > 0
                else None
            )
            rows.append(
                (row["trade_date"], row["instrument_id"], value, row["available_at"])
            )
        return output_frame(self.spec, rows)


class EarningsYieldFactor(_ReciprocalMultipleFactor):
    def __init__(
        self, repository: BarRepository, instruments: Sequence[InstrumentId]
    ) -> None:
        super().__init__(
            repository, instruments, factor_id="earnings_yield_ttm_v1", field="pe_ttm"
        )


class BookToPriceFactor(_ReciprocalMultipleFactor):
    def __init__(
        self, repository: BarRepository, instruments: Sequence[InstrumentId]
    ) -> None:
        super().__init__(
            repository, instruments, factor_id="book_to_price_mrq_v1", field="pb_mrq"
        )
