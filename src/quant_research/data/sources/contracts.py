"""定义端点驱动的全市场数据源采集端口。"""

from collections.abc import Iterable, Mapping
from datetime import date
from typing import Protocol

from quant_research.data.contracts import JsonValue, PublishedPartition, RawBatch


class PipelineSource(Protocol):
    """约束采集端口。入参：端点与窗口。返回值：请求和批次。异常：供应商异常按原类型传播。"""

    @property
    def provider(self) -> str:
        """返回供应商。入参：无。返回值：标识。异常：无。"""
        ...

    def login(self) -> None:
        """登录。入参：无。返回值：无。异常：认证失败时传播。"""
        ...

    def close(self) -> None:
        """关闭。入参：无。返回值：无。异常：关闭失败时传播。"""
        ...

    def requests(
        self, endpoint: str, start: date, end: date
    ) -> tuple[Mapping[str, JsonValue], ...]:
        """构造请求。入参：端点和日期范围。返回值：请求单元。异常：范围非法时抛出。

        除 ``index_daily`` 外，请求不得包含 ``ts_code``。
        """
        ...

    def fetch(
        self, endpoint: str, request: Mapping[str, JsonValue]
    ) -> Iterable[RawBatch]:
        """执行请求。入参：端点和请求。返回值：Raw 批次。异常：供应商失败时传播。"""
        ...

    def calendar_trading_days(
        self, calendar_partition: PublishedPartition, start: date, end: date
    ) -> tuple[date, ...]:
        """读取交易日。入参：分区和范围。返回值：开市日。异常：分区非法时抛出。"""
        ...
