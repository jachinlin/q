"""提供回测与交易执行相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal

import polars as pl

from quant_research.backtest.models import (
    AccountView,
    ExecutionBatch,
    ExecutionConfig,
    ExecutionPrice,
    ExecutionReason,
    FillResult,
    MarketSlice,
    RejectResult,
)
from quant_research.backtest.rulebook import (
    FeeBreakdown,
    InstrumentTradingProfile,
    MarketRuleBook,
    PriceBand,
    SecurityStatus,
    Side,
    SimulatedFill,
)
from quant_research.domain.enums import Board
from quant_research.domain.identifiers import InstrumentId
from quant_research.portfolio.rebalance import OrderIntent, OrderSide

_CENT = Decimal(100)


@dataclass(frozen=True, slots=True)
class ExecutionModel:
    """表示回测流程中的成交执行``model``及其业务不变量。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``TypeError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Execute a stable sequence of daily order intents against one market slice.
    """

    def execute(
        self,
        intents: Sequence[OrderIntent],
        market: MarketSlice,
        account: AccountView,
        rulebook: MarketRuleBook,
        config: ExecutionConfig,
    ) -> ExecutionBatch:
        """执行约定操作。

        入参：
            intents：参与本次处理的委托意图集合；调用方不得依赖未声明的顺序。
            market：当前交易日经过 Schema 校验的市场切片。
            account：撮合层可见的现金和可卖数量只读视图。
            rulebook：从 ``configs/rules/a_share.yaml`` 加载的唯一交易规则。
            config：调用所用的配置对象，类型为 ``ExecutionConfig``。
        返回值：
            返回执行日（``ExecutionBatch``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``。
        """
        ordered = _ExecutionSupport._validate_intents(intents)
        if not isinstance(market, MarketSlice):
            raise TypeError("market must be a MarketSlice")
        if not isinstance(account, AccountView):
            raise TypeError("account must be an AccountView")
        if not isinstance(config, ExecutionConfig):
            raise TypeError("config must be an ExecutionConfig")
        if not ordered:
            return ExecutionBatch(market.trade_date, (), account.cash_fen)
        prepared = _ExecutionSupport._prepare_market(ordered, market, config)
        cash = account.cash_fen
        results: list[FillResult | RejectResult] = []
        for row in prepared.iter_rows(named=True):
            intent = ordered[row["intent_index"]]
            result, cash = _ExecutionSupport._execute_one(
                intent, row, market, cash, account, rulebook, config
            )
            results.append(result)
        return ExecutionBatch(market.trade_date, tuple(results), cash)


class _ExecutionSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _prepare_market(
        intents: tuple[OrderIntent, ...], market: MarketSlice, config: ExecutionConfig
    ) -> pl.DataFrame:
        intent_frame = pl.DataFrame(
            {
                "intent_index": list(range(len(intents))),
                "instrument_id": [
                    intent.instrument_id.canonical() for intent in intents
                ],
            },
            schema={"intent_index": pl.Int64, "instrument_id": pl.String},
        )
        return (
            intent_frame.join(market.bars, on="instrument_id", how="left")
            .with_columns(
                (pl.col("volume") * config.max_volume_participation)
                .floor()
                .cast(pl.Int64)
                .alias("raw_capacity")
            )
            .select("intent_index", *market.bars.columns, "raw_capacity")
            .sort("intent_index")
        )

    @staticmethod
    def _execute_one(
        intent: OrderIntent,
        row: dict[str, object],
        market: MarketSlice,
        cash: int,
        account: AccountView,
        rulebook: MarketRuleBook,
        config: ExecutionConfig,
    ) -> tuple[FillResult | RejectResult, int]:
        if row["is_suspended"] is True:
            return _ExecutionSupport._reject(
                intent, market, ExecutionReason.SUSPENDED
            ), cash
        if row["open"] is None:
            return _ExecutionSupport._reject(
                intent, market, ExecutionReason.NO_MARKET_DATA
            ), cash
        if intent.side in {OrderSide.SHORT_OPEN, OrderSide.SHORT_COVER}:
            return _ExecutionSupport._reject(
                intent,
                market,
                ExecutionReason.SHORT_NOT_SUPPORTED,
                _ExecutionSupport._reference_price(row, config),
            ), cash
        reference_price = _ExecutionSupport._reference_price(row, config)
        profile = rulebook.trading_profile(
            intent.instrument_id,
            _ExecutionSupport._string(row, "instrument_type"),
            Board(_ExecutionSupport._string(row, "board")),
            market.trade_date,
        )
        sellable = account.sellable_quantities.get(intent.instrument_id, 0)
        side = Side.BUY if intent.side is OrderSide.BUY else Side.SELL
        position_quantity = sellable if side is Side.SELL else None
        if not profile.is_quantity_valid(side, intent.quantity, position_quantity):
            return _ExecutionSupport._reject(
                intent, market, ExecutionReason.ODD_LOT, reference_price
            ), cash
        status = SecurityStatus(_ExecutionSupport._string(row, "security_status"))
        band = rulebook.price_limits(
            profile,
            market.trade_date,
            _ExecutionSupport._float(row, "preclose"),
            status,
        )
        if band is not None:
            if (
                intent.side is OrderSide.BUY
                and _ExecutionSupport._float(row, "low") >= band.upper
            ):
                return _ExecutionSupport._reject(
                    intent,
                    market,
                    ExecutionReason.LIMIT_UP_BUY_BLOCKED,
                    reference_price,
                ), cash
            if (
                intent.side is OrderSide.SELL
                and _ExecutionSupport._float(row, "high") <= band.lower
            ):
                return _ExecutionSupport._reject(
                    intent,
                    market,
                    ExecutionReason.LIMIT_DOWN_SELL_BLOCKED,
                    reference_price,
                ), cash
        if intent.side is OrderSide.SELL and sellable == 0:
            return _ExecutionSupport._reject(
                intent,
                market,
                ExecutionReason.INSUFFICIENT_SELLABLE,
                reference_price,
            ), cash
        capacity = _ExecutionSupport._int(row, "raw_capacity")
        if capacity == 0:
            return _ExecutionSupport._reject(
                intent, market, ExecutionReason.VOLUME_CAP, reference_price
            ), cash
        price = _ExecutionSupport._execution_price(
            reference_price, intent.side, config, band, profile
        )
        candidate = min(intent.quantity, capacity)
        if intent.side is OrderSide.SELL:
            candidate = min(candidate, sellable)
        candidate = profile.normalize_quantity(
            side, candidate, position_quantity=position_quantity
        )
        if candidate == 0:
            reason = (
                ExecutionReason.INSUFFICIENT_SELLABLE
                if intent.side is OrderSide.SELL
                else ExecutionReason.VOLUME_CAP
            )
            return _ExecutionSupport._reject(
                intent, market, reason, reference_price
            ), cash
        filled = candidate
        fees = _ExecutionSupport._fees(rulebook, profile, intent, market, filled, price)
        if intent.side is OrderSide.BUY:
            filled = _ExecutionSupport._affordable_quantity(
                cash, candidate, profile, price, rulebook, intent, market
            )
            if filled == 0:
                return _ExecutionSupport._reject(
                    intent,
                    market,
                    ExecutionReason.INSUFFICIENT_CASH,
                    reference_price,
                ), cash
            fees = _ExecutionSupport._fees(
                rulebook, profile, intent, market, filled, price
            )
        gross = _ExecutionSupport._gross_value_fen(price, filled)
        if intent.side is OrderSide.SELL and cash + gross < fees.total_cents:
            return _ExecutionSupport._reject(
                intent,
                market,
                ExecutionReason.INSUFFICIENT_CASH,
                reference_price,
            ), cash
        unfilled = intent.quantity - filled
        reason = _ExecutionSupport._fill_reason(
            intent, filled, candidate, capacity, sellable
        )
        result = FillResult(
            intent,
            market.trade_date,
            intent.quantity,
            reference_price,
            _ExecutionSupport._gross_value_fen(reference_price, intent.quantity),
            filled,
            unfilled,
            price,
            gross,
            profile.settlement_sessions,
            fees,
            reason,
        )
        if intent.side is OrderSide.BUY:
            return result, cash - gross - fees.total_cents
        return result, cash + gross - fees.total_cents

    @staticmethod
    def _execution_price(
        reference_price: float,
        side: OrderSide,
        config: ExecutionConfig,
        band: PriceBand | None,
        profile: InstrumentTradingProfile,
    ) -> float:
        reference = Decimal(str(reference_price))
        direction = Decimal(1) if side is OrderSide.BUY else Decimal(-1)
        price = reference * (
            Decimal(1) + direction * Decimal(str(config.slippage_bps)) / Decimal(10_000)
        )
        price = price.quantize(profile.price_tick, rounding=ROUND_HALF_UP)
        if band is not None:
            lower = Decimal(str(band.lower))
            upper = Decimal(str(band.upper))
            price = min(max(price, lower), upper)
        if not price.is_finite() or price <= 0:
            raise ValueError("execution price must be finite and positive")
        return float(price)

    @staticmethod
    def _reference_price(row: dict[str, object], config: ExecutionConfig) -> float:
        reference_key = (
            "open" if config.reference_price is ExecutionPrice.OPEN else "close"
        )
        return _ExecutionSupport._float(row, reference_key)

    @staticmethod
    def _fees(
        rulebook: MarketRuleBook,
        profile: InstrumentTradingProfile,
        intent: OrderIntent,
        market: MarketSlice,
        quantity: int,
        price: float,
    ) -> FeeBreakdown:
        side = Side.BUY if intent.side is OrderSide.BUY else Side.SELL
        fees = rulebook.fees(
            SimulatedFill(
                intent.instrument_id, market.trade_date, side, quantity, price
            ),
            profile,
        )
        if not isinstance(fees, FeeBreakdown):
            raise TypeError("rulebook fees must return FeeBreakdown")
        if any(type(value) is not int or value < 0 for value in fees.as_tuple()):
            raise ValueError("rulebook fees must be nonnegative integer cents")
        if fees.total_cents != sum(fees.as_tuple()[:3]):
            raise ValueError("rulebook fee total is invalid")
        return fees

    @staticmethod
    def _affordable_quantity(
        cash: int,
        candidate: int,
        profile: InstrumentTradingProfile,
        price: float,
        rulebook: MarketRuleBook,
        intent: OrderIntent,
        market: MarketSlice,
    ) -> int:
        if candidate < profile.buy_minimum:
            return 0
        low = -1
        high = (candidate - profile.buy_minimum) // profile.buy_increment
        while low < high:
            middle = (low + high + 1) // 2
            quantity = profile.buy_minimum + middle * profile.buy_increment
            cost = (
                _ExecutionSupport._gross_value_fen(price, quantity)
                + _ExecutionSupport._fees(
                    rulebook, profile, intent, market, quantity, price
                ).total_cents
            )
            if cost <= cash:
                low = middle
            else:
                high = middle - 1
        return 0 if low < 0 else profile.buy_minimum + low * profile.buy_increment

    @staticmethod
    def _gross_value_fen(price: float, quantity: int) -> int:
        gross = Decimal(str(price)) * quantity * _CENT
        result = int(gross.quantize(Decimal(1), rounding=ROUND_HALF_UP))
        if result <= 0:
            raise ValueError("gross value must round to positive fen")
        return result

    @staticmethod
    def _fill_reason(
        intent: OrderIntent, filled: int, candidate: int, capacity: int, sellable: int
    ) -> ExecutionReason:
        if filled == intent.quantity:
            return ExecutionReason.FILLED
        if intent.side is OrderSide.BUY:
            return (
                ExecutionReason.INSUFFICIENT_CASH
                if filled < candidate
                else ExecutionReason.VOLUME_CAP
            )
        return (
            ExecutionReason.INSUFFICIENT_SELLABLE
            if sellable < capacity
            else ExecutionReason.VOLUME_CAP
        )

    @staticmethod
    def _reject(
        intent: OrderIntent,
        market: MarketSlice,
        reason: ExecutionReason,
        reference_price: float | None = None,
    ) -> RejectResult:
        requested_reference_value = (
            None
            if reference_price is None
            else _ExecutionSupport._gross_value_fen(reference_price, intent.quantity)
        )
        return RejectResult(
            intent,
            market.trade_date,
            intent.quantity,
            reference_price,
            requested_reference_value,
            reason,
        )

    @staticmethod
    def _float(row: dict[str, object], name: str) -> float:
        value = row[name]
        if not isinstance(value, float):
            raise TypeError(f"prepared market {name} must be a float")
        return value

    @staticmethod
    def _int(row: dict[str, object], name: str) -> int:
        value = row[name]
        if type(value) is not int:
            raise ValueError(f"prepared market {name} must be an integer")
        return value

    @staticmethod
    def _string(row: dict[str, object], name: str) -> str:
        value = row[name]
        if not isinstance(value, str):
            raise TypeError(f"prepared market {name} must be a string")
        return value

    @staticmethod
    def _validate_intents(intents: Sequence[OrderIntent]) -> tuple[OrderIntent, ...]:
        if not isinstance(intents, Sequence) or isinstance(intents, (str, bytes)):
            raise TypeError("intents must be a sequence")
        ordered = tuple(intents)
        seen = set()
        for intent in ordered:
            if not isinstance(intent, OrderIntent):
                raise TypeError("intents must contain OrderIntent")
            if not isinstance(intent.instrument_id, InstrumentId):
                raise TypeError("intent instrument_id must be an InstrumentId")
            if (
                not isinstance(intent.side, OrderSide)
                or type(intent.quantity) is not int
                or intent.quantity <= 0
            ):
                raise ValueError("intent quantity and side are invalid")
            if intent.instrument_id in seen:
                raise ValueError("intents must be unique by instrument")
            seen.add(intent.instrument_id)
        return ordered
