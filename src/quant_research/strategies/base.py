"""提供策略与基础契约相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from itertools import pairwise
from math import isfinite
from types import MappingProxyType
from typing import Protocol

import polars as pl

from quant_research.backtest.accounting import AccountSnapshot
from quant_research.backtest.engine import StrategyRef
from quant_research.domain.identifiers import InstrumentId
from quant_research.factors.base import canonical_factor_ref, is_available_on_signal_day
from quant_research.portfolio.constructor import (
    PortfolioConstructor,
    TargetPortfolio,
    validate_target_portfolio,
)

_FACTOR_SCHEMA = {
    "trade_date": pl.Date,
    "instrument_id": pl.String,
    "factor_ref": pl.String,
    "value": pl.Float64,
    "available_at": pl.Datetime("us", "UTC"),
    "is_valid": pl.Boolean,
}
_UNIVERSE_SCHEMA = {
    "instrument_id": pl.String,
    "as_of": pl.Date,
    "eligible": pl.Boolean,
    "reason_codes": pl.List(pl.String),
    "adv_amount": pl.Float64,
}
_EPSILON = 1e-10


class RebalanceFrequency(StrEnum):
    """定义 ``RebalanceFrequency`` 使用的稳定枚举值。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    """记录策略配置或数据契约违反项的字段、机器码和说明。

    入参：
        code：跨 CLI 和 Dashboard 边界返回的稳定机器可读错误码。
        message：面向用户且已脱敏的错误或状态说明。
        field：字段。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    code: str
    message: str
    field: str | None = None

    def __post_init__(self) -> None:
        for value, name in ((self.code, "code"), (self.message, "message")):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a nonempty string")
        if self.field is not None and (
            not isinstance(self.field, str) or not self.field.strip()
        ):
            raise ValueError("field must be a nonempty string or None")


class StrategyValidationError(ValueError):
    """表示 ``StrategyValidationError`` 对应的领域异常。

    入参：
        issues：参与本次处理的质量问题；调用方不得依赖未声明的顺序。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    issues: tuple[ValidationIssue, ...]

    def __init__(
        self, issues: tuple[ValidationIssue, ...] | list[ValidationIssue]
    ) -> None:
        items = tuple(issues)
        if not items or any(not isinstance(item, ValidationIssue) for item in items):
            raise ValueError("issues must be a nonempty tuple of ValidationIssue")
        self.issues = items
        super().__init__("; ".join(f"{item.code}: {item.message}" for item in items))


class StrategyData(Protocol):
    """定义 ``StrategyData`` 的依赖端口与实现契约。

    入参：
        无。
    返回值：
        构造并返回 ``StrategyData`` 实例。
    异常：
        由具体实现按接口契约定义。
    """

    def factor_values(
        self,
        signal_date: date,
        instruments: tuple[InstrumentId, ...] | None,
        factor_refs: tuple[str, ...],
    ) -> pl.DataFrame:
        """读取因子数值表。

        入参：
            signal_date：只允许使用当日收盘前已知信息的策略信号日。
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
            factor_refs：按规范 ``factor_id`` 指定的因子引用集合。
        返回值：
            返回数值表（``pl.DataFrame``）。
        异常：
            由具体实现按接口契约定义。
        """
        ...

    def stock_universe(self, signal_date: date) -> pl.DataFrame:
        """处理策略信号中的股票股票池。

        入参：
            signal_date：只允许使用当日收盘前已知信息的策略信号日。
        返回值：
            返回股票池（``pl.DataFrame``）。
        异常：
            由具体实现按接口契约定义。
        """
        ...

    def industry_classifications(
        self,
        signal_date: date,
        instruments: tuple[InstrumentId, ...] | None,
        taxonomy: str,
    ) -> pl.DataFrame:
        """读取信号日的供应商 as-of 行业状态并保留 tombstone。

        入参：
            signal_date：策略决策使用的信号日。
            instruments：证券集合；``None`` 表示全市场。
            taxonomy：显式选择的行业分类体系。
        返回值：
            返回信号日最新状态；未分类事件不在此层过滤。
        异常：
            日期、分类体系或 Repository 门禁不合法时传播对应异常。
        """
        ...


@dataclass(frozen=True, slots=True)
class StrategyContext:
    """绑定一个信号日可见的市场、因子、股票池和账户状态。

    入参：
        signal_date：只允许使用当日收盘前已知信息的策略信号日。
        execute_date：使用上一交易日信号生成委托并撮合的交易日。
        sessions：参与本次处理的交易会话集合；调用方不得依赖未声明的顺序。
        data：待处理的数据，类型为 ``StrategyData``。
        portfolio_constructor：组合组合构建器。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    """

    signal_date: date
    execute_date: date
    sessions: tuple[date, ...]
    data: StrategyData
    portfolio_constructor: PortfolioConstructor

    def __post_init__(self) -> None:
        _BaseSupport._require_date(self.signal_date, "signal_date")
        _BaseSupport._require_date(self.execute_date, "execute_date")
        if self.execute_date <= self.signal_date:
            raise ValueError("execute_date must be strictly after signal_date")
        if not isinstance(self.sessions, tuple) or not self.sessions:
            raise ValueError("sessions must be a nonempty tuple")
        if any(type(item) is not date for item in self.sessions):
            raise TypeError("sessions must contain dates")
        if tuple(sorted(self.sessions)) != self.sessions or len(
            set(self.sessions)
        ) != len(self.sessions):
            raise ValueError("sessions must be strictly ascending and unique")
        if (
            self.signal_date not in self.sessions
            or self.execute_date not in self.sessions
        ):
            raise ValueError("sessions must include signal_date and execute_date")
        signal_index = self.sessions.index(self.signal_date)
        if (
            signal_index + 1 >= len(self.sessions)
            or self.sessions[signal_index + 1] != self.execute_date
        ):
            raise ValueError(
                "execute_date must be the next actual session after signal_date"
            )
        if self.data is None:
            raise TypeError("data must be supplied")
        if not isinstance(self.portfolio_constructor, PortfolioConstructor):
            raise TypeError("portfolio_constructor must be a PortfolioConstructor")


@dataclass(frozen=True, slots=True)
class PortfolioPosition:
    """向策略暴露单只证券的数量、可卖数量和市值。

    入参：
        instrument_id：目标证券标识，类型为 ``InstrumentId``。
        quantity：数量。
        market_value_fen：市场数据值分币金额。
        current_weight：当前值权重。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    """

    instrument_id: InstrumentId
    quantity: int
    market_value_fen: int
    current_weight: float

    def __post_init__(self) -> None:
        if not isinstance(self.instrument_id, InstrumentId):
            raise TypeError("instrument_id must be an InstrumentId")
        if type(self.quantity) is not int or self.quantity <= 0:
            raise ValueError("quantity must be a positive integer")
        if type(self.market_value_fen) is not int or self.market_value_fen <= 0:
            raise ValueError("market_value_fen must be a positive integer")
        _BaseSupport._require_weight(self.current_weight, "current_weight")


@dataclass(frozen=True, slots=True)
class PortfolioState:
    """向策略暴露信号日的现金、净值和全部持仓。

    入参：
        trade_date：目标交易日期，类型为 ``date``。
        cash_fen：账户可用现金，采用整数分避免浮点货币误差。
        nav_fen：净值序列分币金额。
        total_market_value_fen：总量市场数据值分币金额。
        positions：参与本次处理的持仓集合；调用方不得依赖未声明的顺序。
        cash_weight：``cash``权重。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    """

    trade_date: date
    cash_fen: int
    nav_fen: int
    total_market_value_fen: int
    positions: tuple[PortfolioPosition, ...]
    cash_weight: float

    def __post_init__(self) -> None:
        _BaseSupport._require_date(self.trade_date, "trade_date")
        for value, name in (
            (self.cash_fen, "cash_fen"),
            (self.nav_fen, "nav_fen"),
            (self.total_market_value_fen, "total_market_value_fen"),
        ):
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
        if (
            self.nav_fen <= 0
            or self.nav_fen != self.cash_fen + self.total_market_value_fen
        ):
            raise ValueError("nav_fen must equal positive cash plus market value")
        if not isinstance(self.positions, tuple) or any(
            not isinstance(position, PortfolioPosition) for position in self.positions
        ):
            raise TypeError("positions must be a tuple of PortfolioPosition")
        canonical = tuple(
            position.instrument_id.canonical() for position in self.positions
        )
        if canonical != tuple(sorted(canonical)) or len(set(canonical)) != len(
            canonical
        ):
            raise ValueError("positions must be unique and canonical-ID sorted")
        if self.total_market_value_fen != sum(
            position.market_value_fen for position in self.positions
        ):
            raise ValueError("total_market_value_fen must equal positions")
        _BaseSupport._require_weight(self.cash_weight, "cash_weight")
        if abs(self.cash_weight - self.cash_fen / self.nav_fen) > _EPSILON:
            raise ValueError("cash_weight must equal cash/nav")
        if (
            abs(
                self.cash_weight
                + sum(position.current_weight for position in self.positions)
                - 1.0
            )
            > _EPSILON
        ):
            raise ValueError("cash and position weights must sum to one")
        for position in self.positions:
            if (
                abs(position.current_weight - position.market_value_fen / self.nav_fen)
                > _EPSILON
            ):
                raise ValueError("position weight must equal market_value/nav")

    @classmethod
    def from_account_snapshot(cls, snapshot: AccountSnapshot) -> PortfolioState:
        """从输入解析账户账户快照。

        入参：
            snapshot：账户快照。
        返回值：
            返回账户账户快照（``PortfolioState``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        """
        if not isinstance(snapshot, AccountSnapshot):
            raise TypeError("snapshot must be an AccountSnapshot")
        if snapshot.nav_fen <= 0:
            raise ValueError("account snapshot nav_fen must be positive")
        positions = tuple(
            PortfolioPosition(
                item.instrument_id,
                item.total_quantity,
                item.market_value_fen,
                item.market_value_fen / snapshot.nav_fen,
            )
            for item in snapshot.positions
        )
        return cls(
            snapshot.trade_date,
            snapshot.cash_fen,
            snapshot.nav_fen,
            snapshot.total_market_value_fen,
            positions,
            snapshot.cash_fen / snapshot.nav_fen,
        )


class Strategy(Protocol):
    """定义 ``Strategy`` 的依赖端口与实现契约。

    入参：
        strategy_id：用于持久化关联和日志追踪的策略标识。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        由具体实现按接口契约定义。
    """

    strategy_id: str

    def validate(self, ctx: StrategyContext) -> list[ValidationIssue]:
        """校验策略信号。

        入参：
            ctx：本次计算的上下文，类型为 ``StrategyContext``。
        返回值：
            返回校验策略信号后的``validate``（``list[ValidationIssue]``）。
        异常：
            由具体实现按接口契约定义。
        """
        ...

    def should_rebalance(self, ctx: StrategyContext, rebalance_date: date) -> bool:
        """判断是否需要调仓。

        入参：
            ctx：本次计算的上下文，类型为 ``StrategyContext``。
            rebalance_date：限定本次业务操作覆盖范围的调仓日期（含边界）。
        返回值：
            返回是否是否需要调仓。
        异常：
            由具体实现按接口契约定义。
        """
        ...

    def generate_targets(
        self, ctx: StrategyContext, rebalance_date: date, current: PortfolioState
    ) -> TargetPortfolio:
        """生成``targets``。

        入参：
            ctx：本次计算的上下文，类型为 ``StrategyContext``。
            rebalance_date：限定本次业务操作覆盖范围的调仓日期（含边界）。
            current：当前值。
        返回值：
            返回生成``targets``后的``targets``（``TargetPortfolio``）。
        异常：
            由具体实现按接口契约定义。
        """
        ...


def is_rebalance_boundary(ctx: StrategyContext, frequency: RebalanceFrequency) -> bool:
    """判断调仓调仓边界；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        ctx：本次计算的上下文，类型为 ``StrategyContext``。
        frequency：调仓频率。
    返回值：
        返回是否调仓调仓边界；该函数作为稳定公开 API 或框架入口保留在模块级。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    Evaluate the close-to-next-session boundary without calendar assumptions.
    """
    return _BaseSupport._is_rebalance_boundary(
        ctx.signal_date, ctx.execute_date, frequency
    )


def rebalance_signal_dates(
    sessions: tuple[date, ...], frequency: RebalanceFrequency
) -> tuple[date, ...]:
    """返回实际调仓信号日；该函数作为稳定公开 API 保留在模块级。

    入参：
        sessions：严格递增的交易日元组。
        frequency：日、周或月调仓频率。
    返回值：
        返回存在下一执行日且跨越相应调仓边界的信号日。
    异常：
        TypeError：日期容器、日期元素或频率类型错误时抛出。
        ValueError：交易日不严格递增或包含重复值时抛出。
    """

    if not isinstance(sessions, tuple) or any(
        not isinstance(value, date) for value in sessions
    ):
        raise TypeError("sessions must be a tuple of dates")
    if len(sessions) < 2:
        return ()
    if sessions != tuple(sorted(sessions)) or len(set(sessions)) != len(sessions):
        raise ValueError("sessions must be strictly ascending and unique")
    return tuple(
        signal
        for signal, execute in pairwise(sessions)
        if _BaseSupport._is_rebalance_boundary(signal, execute, frequency)
    )


def validated_factor_values(
    frame: pl.DataFrame,
    *,
    signal_date: date,
    instruments: tuple[InstrumentId, ...] | None,
    factor_refs: tuple[str, ...],
) -> pl.DataFrame:
    """读取并严格校验因子数值表；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        frame：待处理的数据帧，类型为 ``pl.DataFrame``。
        signal_date：只允许使用当日收盘前已知信息的策略信号日。
        instruments：本次查询、计算或组合构建涉及的规范证券集合。
        factor_refs：按规范 ``factor_id`` 指定的因子引用集合。
    返回值：
        返回读取并严格校验因子数值表后的因子数值表（``pl.DataFrame``）。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Fail closed on a data-port response that is not the requested PIT long table.
    """
    _BaseSupport._require_date(signal_date, "signal_date")
    if not isinstance(frame, pl.DataFrame) or not _BaseSupport._matches_factor_schema(
        frame
    ):
        raise ValueError("factor_values has an invalid schema")
    refs = tuple(canonical_factor_ref(value) for value in factor_refs)
    if len(set(refs)) != len(refs):
        raise ValueError("factor_refs must be unique")
    requested_ids = (
        None if instruments is None else {item.canonical() for item in instruments}
    )
    seen: set[tuple[date, str, str]] = set()
    for row in frame.iter_rows(named=True):
        trade_date = row["trade_date"]
        instrument = row["instrument_id"]
        factor_ref = row["factor_ref"]
        if trade_date != signal_date:
            raise ValueError("factor_values trade_date must equal signal_date")
        _BaseSupport._canonical_instrument(instrument)
        if requested_ids is not None and instrument not in requested_ids:
            raise ValueError(
                "factor_values includes instrument outside requested scope"
            )
        if factor_ref not in refs:
            raise ValueError("factor_values includes unknown factor_ref")
        key = (trade_date, instrument, factor_ref)
        if key in seen:
            raise ValueError("factor_values must have unique primary keys")
        seen.add(key)
        value, available_at, valid = row["value"], row["available_at"], row["is_valid"]
        if valid is not True and valid is not False:
            raise ValueError("factor is_valid must be a nonnull boolean")
        if available_at is not None and not is_available_on_signal_day(
            available_at, signal_date
        ):
            raise ValueError("factor available_at is after signal date")
        if valid is True:
            if not isinstance(value, float) or not isfinite(value):
                raise ValueError("valid factor value must be finite")
            if available_at is None:
                raise ValueError("valid factor value is not available on signal date")
    return frame


def validated_stock_universe(frame: pl.DataFrame, *, signal_date: date) -> pl.DataFrame:
    """读取并严格校验股票股票池；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        frame：待处理的数据帧，类型为 ``pl.DataFrame``。
        signal_date：只允许使用当日收盘前已知信息的策略信号日。
    返回值：
        返回读取并严格校验股票股票池后的股票股票池（``pl.DataFrame``）。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """
    _BaseSupport._require_date(signal_date, "signal_date")
    if not isinstance(frame, pl.DataFrame) or frame.schema != _UNIVERSE_SCHEMA:
        raise ValueError("stock_universe has an invalid schema")
    identifiers = frame["instrument_id"].to_list()
    if any(not isinstance(item, str) for item in identifiers):
        raise ValueError("stock_universe instrument_id must be nonnull")
    if identifiers != sorted(identifiers) or len(set(identifiers)) != len(identifiers):
        raise ValueError("stock_universe must be canonical-ID sorted and unique")
    for row in frame.iter_rows(named=True):
        _BaseSupport._canonical_instrument(row["instrument_id"])
        if row["as_of"] != signal_date:
            raise ValueError("stock_universe as_of must equal signal_date")
        eligible = row["eligible"]
        reasons = row["reason_codes"]
        if eligible is not True and eligible is not False:
            raise ValueError("stock_universe eligible must be a nonnull boolean")
        if not isinstance(reasons, list) or any(
            not isinstance(reason, str) or not reason.strip() for reason in reasons
        ):
            raise ValueError("stock_universe reason_codes must be nonempty strings")
        if eligible is True:
            if reasons:
                raise ValueError("eligible universe rows must have no reason_codes")
            adv_amount = row["adv_amount"]
            if (
                not isinstance(adv_amount, float)
                or not isfinite(adv_amount)
                or adv_amount < 0.0
            ):
                raise ValueError("eligible universe rows require finite adv_amount")
        elif not reasons:
            raise ValueError("ineligible universe rows require reason_codes")
    return frame


class StrategyTargetAdapter:
    """表示策略信号流程中的策略目标组合``adapter``及其业务不变量。

    入参：
        registry：登记并查询不可变业务身份和生命周期状态的登记簿。
        context_provider：由组合根注入、用于隔离外部副作用的运行上下文数据供应商端口。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    Resolve an exact strategy ref into the existing Task 5 target port.
    """

    def __init__(
        self,
        registry: Mapping[StrategyRef, Strategy],
        context_provider: Callable[[date, date], StrategyContext],
    ) -> None:
        if not isinstance(registry, Mapping) or not callable(context_provider):
            raise TypeError("registry and context_provider are required")
        registered = dict(registry)
        for ref, strategy in registered.items():
            if (
                not isinstance(ref, StrategyRef)
                or strategy.strategy_id != ref.strategy_id
            ):
                raise ValueError("registry entries must match exact StrategyRef")
        self._registry = MappingProxyType(registered)
        self._context_provider = context_provider

    def generate_target(
        self,
        strategy: StrategyRef,
        signal_date: date,
        execute_date: date,
        current: AccountSnapshot,
    ) -> TargetPortfolio | None:
        """生成目标组合。

        入参：
            strategy：策略。
            signal_date：只允许使用当日收盘前已知信息的策略信号日。
            execute_date：使用上一交易日信号生成委托并撮合的交易日。
            current：当前值。
        返回值：
            返回生成目标组合后的目标组合（``TargetPortfolio | None``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``StrategyValidationError``、``TypeError``、``ValueError``。
        """
        if not isinstance(strategy, StrategyRef):
            raise TypeError("strategy must be a StrategyRef")
        if not isinstance(current, AccountSnapshot):
            raise TypeError("current must be an AccountSnapshot")
        if current.trade_date != signal_date:
            raise ValueError("current account snapshot must match signal_date")
        try:
            resolved = self._registry[strategy]
        except KeyError as error:
            raise ValueError("unknown strategy") from error
        ctx = self._context_provider(signal_date, execute_date)
        if not isinstance(ctx, StrategyContext) or (
            ctx.signal_date != signal_date or ctx.execute_date != execute_date
        ):
            raise ValueError("context provider returned mismatched strategy context")
        issues = resolved.validate(ctx)
        if not isinstance(issues, list) or any(
            not isinstance(item, ValidationIssue) for item in issues
        ):
            raise StrategyValidationError(
                (
                    ValidationIssue(
                        "INVALID_VALIDATION_RESULT", "strategy returned invalid issues"
                    ),
                )
            )
        if issues:
            raise StrategyValidationError(issues)
        if not resolved.should_rebalance(ctx, signal_date):
            return None
        target = resolved.generate_targets(
            ctx, signal_date, PortfolioState.from_account_snapshot(current)
        )
        return validate_target_portfolio(target, signal_date, execute_date)


class _BaseSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _is_rebalance_boundary(
        signal_date: date,
        execute_date: date,
        frequency: RebalanceFrequency,
    ) -> bool:
        _BaseSupport._require_date(signal_date, "signal_date")
        _BaseSupport._require_date(execute_date, "execute_date")
        if execute_date <= signal_date:
            raise ValueError("execute_date must follow signal_date")
        if not isinstance(frequency, RebalanceFrequency):
            raise TypeError("frequency must be a RebalanceFrequency")
        if frequency is RebalanceFrequency.DAILY:
            return True
        if frequency is RebalanceFrequency.WEEKLY:
            return execute_date.isocalendar()[:2] != signal_date.isocalendar()[:2]
        return (execute_date.year, execute_date.month) != (
            signal_date.year,
            signal_date.month,
        )

    @staticmethod
    def _require_date(value: object, name: str) -> None:
        if type(value) is not date:
            raise TypeError(f"{name} must be a date")

    @staticmethod
    def _require_weight(value: object, name: str) -> None:
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not isfinite(value)
        ):
            raise ValueError(f"{name} must be a finite weight")
        if value < 0.0 or value > 1.0:
            raise ValueError(f"{name} must be in [0, 1]")

    @staticmethod
    def _canonical_instrument(value: object) -> None:
        if not isinstance(value, str):
            raise TypeError("instrument_id must be a canonical string")
        try:
            InstrumentId.parse(value)
        except (TypeError, ValueError) as error:
            raise ValueError("instrument_id must be canonical") from error

    @staticmethod
    def _matches_factor_schema(frame: pl.DataFrame) -> bool:
        expected = dict(_FACTOR_SCHEMA)
        if "invalid_reason" in frame.columns:
            expected["invalid_reason"] = pl.String
        return frame.schema == expected
