"""把目标组合连接到现有 A 股规则、撮合和账户内核。"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import cast

import polars as pl

from quant_research.backtest.accounting import PortfolioAccount
from quant_research.backtest.calendar import TradingCalendar
from quant_research.backtest.execution import ExecutionModel
from quant_research.backtest.models import (
    AccountView,
    ExecutionConfig,
    ExecutionPrice,
    FillResult,
    MarketSlice,
    RejectResult,
)
from quant_research.backtest.rulebook import MarketRuleBook
from quant_research.data.repository import ResearchDataRepository
from quant_research.domain.enums import Board
from quant_research.domain.identifiers import InstrumentId
from quant_research.portfolio.constructor import TargetPortfolio
from quant_research.portfolio.rebalance import OrderIntent, RebalancePlanner


@dataclass(frozen=True, slots=True)
class AShareExecutionResult:
    """表示真实规则撮合产生的成交、账户净值和日收益。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    fills: pl.DataFrame
    nav: pl.DataFrame
    returns: pl.DataFrame


class AShareExecutionSimulator:
    """复用现有确定性撮合、费用与流水账户执行目标组合。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def __init__(
        self,
        repository: ResearchDataRepository,
        rulebook: MarketRuleBook,
    ) -> None:
        if repository is None:
            raise TypeError("repository must be supplied")
        self._repository = repository
        self._rulebook = rulebook
        self._planner = RebalancePlanner()
        self._execution = ExecutionModel()

    def run(
        self,
        targets: Sequence[TargetPortfolio],
        *,
        start: date,
        end: date,
        initial_cash_fen: int,
        reference_price: str,
        slippage_bps: float,
        max_volume_participation: float,
    ) -> AShareExecutionResult:
        """按交易日执行目标，返回全部成交/拒绝和逐日账户净值。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        ordered = tuple(sorted(targets, key=lambda item: (item.execute_date, item.signal_date)))
        if len({item.execute_date for item in ordered}) != len(ordered):
            raise ValueError("execution targets must be unique by execute_date")
        calendar = TradingCalendar.load(self._repository, start, end)
        sessions = calendar.sessions(start, end)
        if not sessions:
            raise ValueError("execution interval contains no trading sessions")
        target_map = {item.execute_date: item for item in ordered}
        instruments = tuple(
            sorted(
                {position.instrument_id for item in ordered for position in item.positions},
                key=InstrumentId.canonical,
            )
        )
        if not instruments:
            return self._cash_only(sessions, initial_cash_fen)
        bars = self._repository.bars(instruments, start, end).collect().sort(
            "trade_date", "instrument_id"
        )
        statuses = self._repository.security_status_range(
            start, end, instruments
        ).collect()
        metadata = self._metadata(instruments)
        config = ExecutionConfig(
            reference_price=ExecutionPrice(reference_price),
            slippage_bps=slippage_bps,
            max_volume_participation=max_volume_participation,
        )
        account = PortfolioAccount(initial_cash_fen, calendar)
        result_rows: list[dict[str, object]] = []
        nav_rows: list[dict[str, object]] = []
        for session in sessions:
            account.begin_session(session)
            market = self._market_slice(session, bars, statuses, metadata)
            view = account.execution_view()
            target = target_map.get(session)
            intents: tuple[OrderIntent, ...]
            if target is None:
                intents = ()
            else:
                prices = self._prices(market, config.reference_price)
                profiles = {
                    instrument: self._rulebook.trading_profile(
                        instrument,
                        metadata[instrument.canonical()]["instrument_type"],
                        Board(metadata[instrument.canonical()]["board"]),
                        session,
                    )
                    for instrument in set(view.total_quantities) | {
                        item.instrument_id for item in target.positions
                    }
                }
                intents = self._planner.plan(
                    target,
                    view.total_quantities,
                    view.cash_fen,
                    prices,
                    profiles,
                ).intents
            batch = self._execution.execute(
                intents,
                market,
                AccountView(view.cash_fen, view.sellable_quantities),
                self._rulebook,
                config,
            )
            account.apply(batch)
            for index, item in enumerate(batch.results):
                result_rows.append(self._result_row(session, index, item))
            closes = {
                InstrumentId.parse(identifier): close
                for identifier, close in market.bars.select(
                    "instrument_id", "close"
                ).iter_rows()
                if close is not None
            }
            snapshot = account.mark_to_market(session, closes)
            nav_rows.append(
                {
                    "trade_date": session,
                    "cash_fen": snapshot.cash_fen,
                    "market_value_fen": snapshot.total_market_value_fen,
                    "nav_fen": snapshot.nav_fen,
                    "nav": snapshot.nav_fen / initial_cash_fen,
                }
            )
        nav = pl.from_dicts(nav_rows)
        returns = nav.select(
            "trade_date",
            pl.col("nav").pct_change().fill_null(0.0).alias("return"),
        )
        fills = (
            pl.from_dicts(result_rows)
            if result_rows
            else self._empty_fills()
        )
        return AShareExecutionResult(fills=fills, nav=nav, returns=returns)

    def _metadata(
        self, instruments: tuple[InstrumentId, ...]
    ) -> dict[str, dict[str, str]]:
        wanted = {item.canonical() for item in instruments}
        frame = self._repository.instruments().collect().filter(
            pl.col("instrument_id").is_in(sorted(wanted))
        )
        output = {
            cast(str, row["instrument_id"]): {
                "instrument_type": cast(str, row["instrument_type"]),
                "board": cast(str, row["board"]),
            }
            for row in frame.select(
                "instrument_id", "instrument_type", "board"
            ).iter_rows(named=True)
        }
        if set(output) != wanted:
            raise ValueError("execution instrument metadata is incomplete")
        return output

    @staticmethod
    def _market_slice(
        session: date,
        bars: pl.DataFrame,
        statuses: pl.DataFrame,
        metadata: dict[str, dict[str, str]],
    ) -> MarketSlice:
        day = bars.filter(pl.col("trade_date") == session)
        status = statuses.filter(pl.col("trade_date") == session).select(
            "instrument_id", "is_suspended", "is_st"
        )
        meta = pl.from_dicts(
            [
                {"instrument_id": identifier, **values}
                for identifier, values in sorted(metadata.items())
            ]
        )
        output = (
            day.join(status, on="instrument_id", how="left")
            .join(meta, on="instrument_id", how="left")
            .with_columns(
                pl.col("volume").fill_null(0).cast(pl.Int64),
                pl.col("is_suspended").fill_null(True),
                pl.when(pl.col("is_st").fill_null(False))
                .then(pl.lit("ST"))
                .otherwise(pl.lit("NORMAL"))
                .alias("security_status"),
            )
            .select(
                "instrument_id",
                pl.col("open").cast(pl.Float64),
                pl.col("high").cast(pl.Float64),
                pl.col("low").cast(pl.Float64),
                pl.col("close").cast(pl.Float64),
                pl.col("preclose").cast(pl.Float64),
                "volume",
                "is_suspended",
                "security_status",
                "instrument_type",
                "board",
            )
            .sort("instrument_id")
        )
        return MarketSlice(session, output)

    @staticmethod
    def _prices(
        market: MarketSlice, reference: ExecutionPrice
    ) -> dict[InstrumentId, float]:
        column = "open" if reference is ExecutionPrice.OPEN else "close"
        return {
            InstrumentId.parse(identifier): float(value)
            for identifier, value in market.bars.select(
                "instrument_id", column
            ).iter_rows()
        }

    @staticmethod
    def _result_row(
        trade_date: date,
        ordinal: int,
        result: FillResult | RejectResult,
    ) -> dict[str, object]:
        base: dict[str, object] = {
            "trade_date": trade_date,
            "ordinal": ordinal,
            "instrument_id": result.intent.instrument_id.canonical(),
            "side": result.intent.side.value,
            "requested_quantity": result.requested_quantity,
            "reference_price": result.reference_price,
            "reason_code": result.reason_code.value,
        }
        if isinstance(result, FillResult):
            base.update(
                {
                    "filled_quantity": result.filled_quantity,
                    "unfilled_quantity": result.unfilled_quantity,
                    "fill_price": result.price,
                    "gross_value_fen": result.gross_value_fen,
                    "fee_fen": result.fees.total_cents,
                    "realized_cost_fen": result.fees.total_cents
                    + abs(result.price - result.reference_price)
                    * result.filled_quantity
                    * 100,
                }
            )
        else:
            base.update(
                {
                    "filled_quantity": 0,
                    "unfilled_quantity": result.requested_quantity,
                    "fill_price": None,
                    "gross_value_fen": 0,
                    "fee_fen": 0,
                    "realized_cost_fen": 0.0,
                }
            )
        return base

    @staticmethod
    def _cash_only(
        sessions: tuple[date, ...], initial_cash_fen: int
    ) -> AShareExecutionResult:
        nav = pl.DataFrame(
            {
                "trade_date": sessions,
                "cash_fen": [initial_cash_fen] * len(sessions),
                "market_value_fen": [0] * len(sessions),
                "nav_fen": [initial_cash_fen] * len(sessions),
                "nav": [1.0] * len(sessions),
            }
        )
        returns = nav.select(
            "trade_date", pl.lit(0.0).alias("return")
        )
        return AShareExecutionResult(
            fills=AShareExecutionSimulator._empty_fills(),
            nav=nav,
            returns=returns,
        )

    @staticmethod
    def _empty_fills() -> pl.DataFrame:
        return pl.DataFrame(
            schema={
                "trade_date": pl.Date,
                "ordinal": pl.Int64,
                "instrument_id": pl.String,
                "side": pl.String,
                "requested_quantity": pl.Int64,
                "reference_price": pl.Float64,
                "reason_code": pl.String,
                "filled_quantity": pl.Int64,
                "unfilled_quantity": pl.Int64,
                "fill_price": pl.Float64,
                "gross_value_fen": pl.Int64,
                "fee_fen": pl.Int64,
                "realized_cost_fen": pl.Float64,
            }
        )
