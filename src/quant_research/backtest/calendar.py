"""提供回测与交易日历相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import polars as pl

from quant_research.data.repository import ResearchDataRepository


@dataclass(frozen=True, slots=True)
class TradingCalendar:
    """表示回测流程中的``trading``交易日历及其业务不变量。

    入参：
        start：处理区间的开始日期，类型为 ``date``。
        end：处理区间的结束日期，类型为 ``date``。
        _sessions：参与本次处理的交易会话集合；调用方不得依赖未声明的顺序。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``TypeError``、``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    The open sessions returned for one explicit date coverage.
    """

    start: date
    end: date
    _sessions: tuple[date, ...]

    @classmethod
    def load(
        cls,
        repository: ResearchDataRepository,
        start: date,
        end: date,
    ) -> TradingCalendar:
        """加载并校验约定资源。

        入参：
            repository：提供持久化访问的仓储，类型为 ``ResearchDataRepository``。
            start：处理区间的开始日期，类型为 ``date``。
            end：处理区间的结束日期，类型为 ``date``。
        返回值：
            返回加载并校验回测后的``load``（``TradingCalendar``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        Load and validate one inclusive calendar range.
        """
        _CalendarSupport._validate_range(start, end)
        frame = repository.trade_calendar(start, end).collect()
        _CalendarSupport._validate_calendar_columns(frame)
        rows = frame.select("trade_date", "is_trading_day").rows()
        if not rows:
            raise ValueError("trade calendar interval must not be empty")
        observed: set[date] = set()
        sessions: list[date] = []
        for trade_date, is_trading_day in rows:
            if not isinstance(trade_date, date) or isinstance(trade_date, datetime):
                raise ValueError("trade_date must contain date values")  # noqa: TRY004
            if type(is_trading_day) is not bool:
                raise ValueError("is_trading_day must contain bool values")
            if trade_date in observed:
                raise ValueError("trade calendar contains duplicate trade_date")
            if trade_date < start or trade_date > end:
                raise ValueError("trade calendar row is outside requested range")
            observed.add(trade_date)
            if is_trading_day:
                sessions.append(trade_date)
        sessions.sort()
        return cls(start, end, tuple(sessions))

    def sessions(self, start: date, end: date) -> tuple[date, ...]:
        """处理回测中的交易会话集合。

        入参：
            start：处理区间的开始日期，类型为 ``date``。
            end：处理区间的结束日期，类型为 ``date``。
        返回值：
            返回交易会话集合（``tuple[date, ...]``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        Return open sessions in the inclusive loaded subrange.
        """
        _CalendarSupport._validate_range(start, end)
        if start < self.start or end > self.end:
            raise ValueError("requested sessions are outside loaded coverage")
        left = bisect_right(self._sessions, start - _ONE_DAY)
        right = bisect_right(self._sessions, end)
        return self._sessions[left:right]

    def next_session(self, trade_date: date) -> date:
        """处理回测中的``next``交易会话。

        入参：
            trade_date：目标交易日期，类型为 ``date``。
        返回值：
            返回交易会话（``date``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        Return the first loaded session strictly after ``trade_date``.
        """
        if not isinstance(trade_date, date) or isinstance(trade_date, datetime):
            raise TypeError("trade_date must be a date")
        index = bisect_right(self._sessions, trade_date)
        if index == len(self._sessions):
            raise ValueError("no later trading session in loaded coverage")
        return self._sessions[index]


class _CalendarSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _validate_range(start: date, end: date) -> None:
        if (
            not isinstance(start, date)
            or isinstance(start, datetime)
            or not isinstance(end, date)
            or isinstance(end, datetime)
        ):
            raise TypeError("start and end must be date values")
        if start > end:
            raise ValueError("start must not follow end")

    @staticmethod
    def _validate_calendar_columns(frame: pl.DataFrame) -> None:
        required = {"trade_date", "is_trading_day"}
        if not required.issubset(frame.columns):
            raise ValueError("trade calendar is missing required columns")


_ONE_DAY = timedelta(days=1)
