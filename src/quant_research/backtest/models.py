"""提供回测与领域模型相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from enum import StrEnum
from math import isfinite
from types import MappingProxyType

import polars as pl

from quant_research.backtest.rulebook import FeeBreakdown, SecurityStatus
from quant_research.domain.enums import Board
from quant_research.domain.identifiers import InstrumentId
from quant_research.strategies.base import OrderIntent, OrderSide


class ExecutionReason(StrEnum):
    """定义 ``ExecutionReason`` 使用的稳定枚举值。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    FILLED = "FILLED"
    SUSPENDED = "SUSPENDED"
    LIMIT_UP_BUY_BLOCKED = "LIMIT_UP_BUY_BLOCKED"
    LIMIT_DOWN_SELL_BLOCKED = "LIMIT_DOWN_SELL_BLOCKED"
    INSUFFICIENT_CASH = "INSUFFICIENT_CASH"
    INSUFFICIENT_SELLABLE = "INSUFFICIENT_SELLABLE"
    VOLUME_CAP = "VOLUME_CAP"
    ODD_LOT = "ODD_LOT"
    NO_MARKET_DATA = "NO_MARKET_DATA"
    SHORT_NOT_SUPPORTED = "SHORT_NOT_SUPPORTED"


class ExecutionPrice(StrEnum):
    """定义 ``ExecutionPrice`` 使用的稳定枚举值。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    OPEN = "OPEN"
    CLOSE = "CLOSE"


@dataclass(frozen=True, slots=True)
class ExecutionConfig:
    """定义回测流程使用的不可变配置及取值约束。

    入参：
        reference_price：撮合时采用的开盘价或收盘价口径。
        slippage_bps：相对参考价施加的单边滑点，单位为基点。
        返回完成字段规范化和不变量校验的对象。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    """

    reference_price: ExecutionPrice
    slippage_bps: float
    max_volume_participation: float

    def __post_init__(self) -> None:
        if not isinstance(self.reference_price, ExecutionPrice):
            raise TypeError("reference_price must be an ExecutionPrice")
        _ModelsSupport._finite_nonnegative(self.slippage_bps, "slippage_bps")
        participation = _ModelsSupport._finite_nonnegative(
            self.max_volume_participation, "max_volume_participation"
        )
        if participation <= 0 or participation > 1:
            raise ValueError("max_volume_participation must be in (0, 1]")


@dataclass(frozen=True, slots=True)
class MarketSlice:
    """绑定单个交易日及其经校验 OHLCV 行情表。

    入参：
        trade_date：目标交易日期，类型为 ``date``。
        bars：包含证券、交易日和 OHLCV 字段的市场行情表。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    """

    trade_date: date
    bars: pl.DataFrame

    def __post_init__(self) -> None:
        _ModelsSupport._trade_date(self.trade_date)
        if not isinstance(self.bars, pl.DataFrame):
            raise TypeError("bars must be a polars DataFrame")
        _ModelsSupport._validate_bars(self.bars)


@dataclass(frozen=True, slots=True)
class AccountView:
    """向撮合层暴露当前可用现金和逐证券可卖数量。

    入参：
        cash_fen：账户可用现金，采用整数分避免浮点货币误差。
        返回完成字段规范化和不变量校验的对象。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    """

    cash_fen: int
    sellable_quantities: Mapping[InstrumentId, int]

    def __post_init__(self) -> None:
        _ModelsSupport._nonnegative_int(self.cash_fen, "cash_fen")
        if not isinstance(self.sellable_quantities, Mapping):
            raise TypeError("sellable_quantities must be a mapping")
        quantities = dict(self.sellable_quantities)
        for instrument, quantity in quantities.items():
            if not isinstance(instrument, InstrumentId):
                raise TypeError("sellable_quantities keys must be InstrumentId")
            _ModelsSupport._nonnegative_int(quantity, "sellable quantity")
        object.__setattr__(self, "sellable_quantities", MappingProxyType(quantities))


@dataclass(frozen=True, slots=True)
class FillResult:
    """记录一次回测操作的结果、业务指标和审计身份。

    入参：
        intent：策略调仓计划产生、尚未撮合的买卖委托意图。
        trade_date：目标交易日期，类型为 ``date``。
        requested_quantity：委托请求成交的证券数量。
        reference_price：执行配置指定的未加滑点参考价格。
        requested_reference_value_fen：参考价格乘请求数量后取整到分的金额。
        filled_quantity：通过交易规则和资金持仓校验后实际成交的数量。
        unfilled_quantity：因成交量上限等原因未成交的剩余数量。
        price：计入滑点后的实际成交价格。
        gross_value_fen：成交价格乘成交数量后取整到分的成交总额。
        settlement_sessions：买入后等待多少个交易日才可卖出。
        fees：根据唯一 A 股规则文件计算的佣金、印花税等费用明细。
        reason_code：说明成交、拒绝或排除原因的稳定机器码。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    """

    intent: OrderIntent
    trade_date: date
    requested_quantity: int
    reference_price: float
    requested_reference_value_fen: int
    filled_quantity: int
    unfilled_quantity: int
    price: float
    gross_value_fen: int
    settlement_sessions: int
    fees: FeeBreakdown
    reason_code: ExecutionReason

    def __post_init__(self) -> None:
        _ModelsSupport._intent(self.intent)
        _ModelsSupport._trade_date(self.trade_date)
        _ModelsSupport._positive_int(self.requested_quantity, "requested_quantity")
        _ModelsSupport._finite_positive(self.reference_price, "reference_price")
        _ModelsSupport._positive_int(
            self.requested_reference_value_fen,
            "requested_reference_value_fen",
        )
        if self.requested_reference_value_fen != _ModelsSupport._reference_value_fen(
            self.reference_price, self.requested_quantity
        ):
            raise ValueError(
                "requested reference value does not match price and quantity"
            )
        _ModelsSupport._positive_int(self.filled_quantity, "filled_quantity")
        _ModelsSupport._nonnegative_int(self.unfilled_quantity, "unfilled_quantity")
        if self.filled_quantity + self.unfilled_quantity != self.requested_quantity:
            raise ValueError("fill quantities must equal requested_quantity")
        _ModelsSupport._finite_positive(self.price, "price")
        _ModelsSupport._nonnegative_int(self.gross_value_fen, "gross_value_fen")
        _ModelsSupport._nonnegative_int(self.settlement_sessions, "settlement_sessions")
        _ModelsSupport._fees(self.fees)
        if not isinstance(self.reason_code, ExecutionReason):
            raise TypeError("reason_code must be an ExecutionReason")


@dataclass(frozen=True, slots=True)
class RejectResult:
    """记录一次回测操作的结果、业务指标和审计身份。

    入参：
        intent：策略调仓计划产生、尚未撮合的买卖委托意图。
        trade_date：目标交易日期，类型为 ``date``。
        requested_quantity：委托请求成交的证券数量。
        reference_price：可用时记录执行配置指定的未加滑点参考价格。
        requested_reference_value_fen：可用时记录参考价格乘请求数量的分币金额。
        reason_code：说明成交、拒绝或排除原因的稳定机器码。
        detail：供调用者诊断失败原因的可选安全文本。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    """

    intent: OrderIntent
    trade_date: date
    requested_quantity: int
    reference_price: float | None
    requested_reference_value_fen: int | None
    reason_code: ExecutionReason
    detail: str | None = None

    def __post_init__(self) -> None:
        _ModelsSupport._intent(self.intent)
        _ModelsSupport._trade_date(self.trade_date)
        _ModelsSupport._positive_int(self.requested_quantity, "requested_quantity")
        if (self.reference_price is None) != (
            self.requested_reference_value_fen is None
        ):
            raise ValueError(
                "reject reference price and value must be jointly available"
            )
        if self.reference_price is not None:
            _ModelsSupport._finite_positive(self.reference_price, "reference_price")
            if self.requested_reference_value_fen is None:
                raise ValueError("requested reference value is missing")
            _ModelsSupport._positive_int(
                self.requested_reference_value_fen,
                "requested_reference_value_fen",
            )
            if (
                self.requested_reference_value_fen
                != _ModelsSupport._reference_value_fen(
                    self.reference_price, self.requested_quantity
                )
            ):
                raise ValueError(
                    "requested reference value does not match price and quantity"
                )
        if not isinstance(self.reason_code, ExecutionReason):
            raise TypeError("reason_code must be an ExecutionReason")
        if self.reason_code is ExecutionReason.FILLED:
            raise ValueError("reject reason_code cannot be FILLED")
        if self.reason_code in {
            ExecutionReason.SUSPENDED,
            ExecutionReason.NO_MARKET_DATA,
        }:
            if self.reference_price is not None:
                raise ValueError("unpriceable rejects cannot carry a reference price")
        elif self.reference_price is None:
            raise ValueError("priceable rejects require a reference price")
        if self.detail is not None and not isinstance(self.detail, str):
            raise TypeError("detail must be a string or None")


ExecutionResult = FillResult | RejectResult


@dataclass(frozen=True, slots=True)
class ExecutionBatch:
    """汇总一个交易日按确定性顺序执行的成交、拒绝和期末现金。

    入参：
        trade_date：目标交易日期，类型为 ``date``。
        results：参与本次处理的结果集合；调用方不得依赖未声明的顺序。
        ending_cash_fen：执行整批委托后账户剩余的整数分现金。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    """

    trade_date: date
    results: tuple[ExecutionResult, ...]
    ending_cash_fen: int

    def __post_init__(self) -> None:
        _ModelsSupport._trade_date(self.trade_date)
        if not isinstance(self.results, tuple):
            raise TypeError("results must be a tuple")
        for result in self.results:
            if not isinstance(result, (FillResult, RejectResult)):
                raise TypeError("results must contain execution results")
            if result.trade_date != self.trade_date:
                raise ValueError("result trade_date must match batch")
        _ModelsSupport._nonnegative_int(self.ending_cash_fen, "ending_cash_fen")


class _ModelsSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _validate_bars(bars: pl.DataFrame) -> None:
        expected = {
            "instrument_id": pl.String,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "preclose": pl.Float64,
            "volume": pl.Int64,
            "is_suspended": pl.Boolean,
            "security_status": pl.String,
            "instrument_type": pl.String,
            "board": pl.String,
        }
        missing = set(expected).difference(bars.columns)
        if missing:
            raise ValueError("market bars missing required columns")
        for name, dtype in expected.items():
            if bars.schema[name] != dtype:
                raise ValueError(f"market bars column {name} has invalid type")
        required_nonnull = bars.select(
            "instrument_id",
            "is_suspended",
            "security_status",
            "instrument_type",
            "board",
        )
        if any(
            value != 0
            for row in required_nonnull.null_count().iter_rows()
            for value in row
        ):
            raise ValueError(
                "market bar identity and status columns cannot contain nulls"
            )
        seen: set[str] = set()
        for row in bars.select(list(expected)).iter_rows(named=True):
            identifier = row["instrument_id"]
            status = row["security_status"]
            instrument_type = row["instrument_type"]
            board = row["board"]
            if not isinstance(identifier, str):
                raise TypeError("market bars instrument_id must be canonical")
            try:
                InstrumentId.parse(identifier)
            except (TypeError, ValueError) as error:
                raise ValueError(
                    "market bars instrument_id must be canonical"
                ) from error
            if identifier in seen:
                raise ValueError("market bars instrument_id must be unique")
            seen.add(identifier)
            if not isinstance(status, str):
                raise TypeError("market bars security_status is invalid")
            try:
                SecurityStatus(status)
            except ValueError as error:
                raise ValueError("market bars security_status is invalid") from error
            if not isinstance(instrument_type, str) or not instrument_type:
                raise ValueError("market bars instrument_type is invalid")
            if not isinstance(board, str):
                raise TypeError("market bars board is invalid")
            try:
                Board(board)
            except ValueError as error:
                raise ValueError("market bars board is invalid") from error
            market_values = tuple(
                row[column]
                for column in ("open", "high", "low", "close", "preclose", "volume")
            )
            if row["is_suspended"] is True and all(
                value is None for value in market_values
            ):
                continue
            for column in ("open", "high", "low", "close", "preclose"):
                value = row[column]
                if not isinstance(value, float) or not isfinite(value) or value <= 0:
                    raise ValueError("market bars OHLC values must be finite positive")
            if row["low"] > row["open"] or row["low"] > row["close"]:
                raise ValueError("market bars low invariant is invalid")
            if row["open"] > row["high"] or row["close"] > row["high"]:
                raise ValueError("market bars high invariant is invalid")
            if row["volume"] is None and row["is_suspended"] is True:
                continue
            if not isinstance(row["volume"], int) or row["volume"] < 0:
                raise ValueError("market bars volume must be nonnegative")

    @staticmethod
    def _intent(value: OrderIntent) -> None:
        if not isinstance(value, OrderIntent):
            raise TypeError("intent must be an OrderIntent")
        if not isinstance(value.instrument_id, InstrumentId):
            raise TypeError("intent instrument_id must be an InstrumentId")
        if not isinstance(value.side, OrderSide):
            raise TypeError("intent side must be an OrderSide")
        _ModelsSupport._positive_int(value.quantity, "intent quantity")
        if not isinstance(value.reason, str):
            raise TypeError("intent reason must be a string")

    @staticmethod
    def _fees(value: FeeBreakdown) -> None:
        if not isinstance(value, FeeBreakdown):
            raise TypeError("fees must be a FeeBreakdown")
        for component in value.as_tuple():
            _ModelsSupport._nonnegative_int(component, "fee")
        if value.total_cents != sum(value.as_tuple()[:3]):
            raise ValueError("fee total must equal fee components")

    @staticmethod
    def _trade_date(value: object) -> None:
        if not isinstance(value, date) or isinstance(value, datetime):
            raise TypeError("trade_date must be a date")

    @staticmethod
    def _positive_int(value: object, name: str) -> None:
        _ModelsSupport._nonnegative_int(value, name)
        if value == 0:
            raise ValueError(f"{name} must be positive")

    @staticmethod
    def _nonnegative_int(value: object, name: str) -> None:
        if type(value) is not int or value < 0:
            raise ValueError(f"{name} must be a nonnegative integer")

    @staticmethod
    def _finite_nonnegative(value: object, name: str) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(f"{name} must be finite and nonnegative")
        result = float(value)
        if not isfinite(result) or result < 0:
            raise ValueError(f"{name} must be finite and nonnegative")
        return result

    @staticmethod
    def _finite_positive(value: object, name: str) -> None:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(f"{name} must be finite and positive")
        if not isfinite(float(value)) or value <= 0:
            raise ValueError(f"{name} must be finite and positive")

    @staticmethod
    def _reference_value_fen(price: float, quantity: int) -> int:
        return int(
            (Decimal(str(price)) * quantity * Decimal(100)).quantize(
                Decimal(1), rounding=ROUND_HALF_UP
            )
        )
