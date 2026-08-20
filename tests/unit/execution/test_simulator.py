"""验证研究执行层复用 A 股撮合、费用和账户内核。"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import cast

import polars as pl

from quant_research.backtest.rulebook import AShareRuleBook
from quant_research.data.repository import ResearchDataRepository
from quant_research.domain.identifiers import InstrumentId
from quant_research.execution import AShareExecutionSimulator
from quant_research.portfolio.constructor import TargetPortfolio, TargetPosition


class _Repository:
    def __init__(self) -> None:
        self.instrument = InstrumentId.parse("510300.SH")

    def trade_calendar(self, start: date, end: date) -> pl.LazyFrame:
        del start, end
        return pl.DataFrame(
            {
                "trade_date": [date(2026, 1, 5), date(2026, 1, 6)],
                "is_trading_day": [True, True],
            }
        ).lazy()

    def instruments(self) -> pl.LazyFrame:
        return pl.DataFrame(
            {
                "instrument_id": ["510300.SH"],
                "instrument_type": ["ETF"],
                "board": ["MAIN"],
            }
        ).lazy()

    def bars(
        self,
        instruments: tuple[InstrumentId, ...],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        del instruments, start, end
        return pl.DataFrame(
            {
                "trade_date": [date(2026, 1, 5), date(2026, 1, 6)],
                "instrument_id": ["510300.SH", "510300.SH"],
                "open": [10.0, 10.1],
                "high": [10.2, 10.3],
                "low": [9.9, 10.0],
                "close": [10.1, 10.2],
                "preclose": [9.9, 10.1],
                "volume": [1_000_000, 1_000_000],
            }
        ).lazy()

    def security_status_range(
        self,
        start: date,
        end: date,
        instruments: tuple[InstrumentId, ...],
    ) -> pl.LazyFrame:
        del start, end, instruments
        return pl.DataFrame(
            {
                "trade_date": [date(2026, 1, 5), date(2026, 1, 6)],
                "instrument_id": ["510300.SH", "510300.SH"],
                "is_suspended": [False, False],
                "is_st": [False, False],
            }
        ).lazy()


def test_execution_simulator_applies_real_fees_and_accounting() -> None:
    repository = cast(ResearchDataRepository, _Repository())
    rulebook = AShareRuleBook.load(
        Path(__file__).parents[3] / "configs" / "rules" / "a_share.yaml"
    )
    target = TargetPortfolio(
        signal_date=date(2026, 1, 2),
        execute_date=date(2026, 1, 5),
        positions=(
            TargetPosition(
                InstrumentId.parse("510300.SH"), 0.9, 1.0, "ALLOCATION"
            ),
        ),
        cash_weight=0.1,
    )

    result = AShareExecutionSimulator(repository, rulebook).run(
        (target,),
        start=date(2026, 1, 5),
        end=date(2026, 1, 6),
        initial_cash_fen=1_000_000,
        reference_price="OPEN",
        slippage_bps=5.0,
        max_volume_participation=0.1,
    )

    first = result.fills.row(0, named=True)
    assert first["reason_code"] == "FILLED"
    assert first["filled_quantity"] == 900
    assert first["fee_fen"] == 500
    assert first["fill_price"] == 10.005
    assert result.nav.height == 2
    assert result.nav["nav_fen"].to_list() == [1_008_050, 1_017_050]
    assert result.returns["return"][0] == 0.0
