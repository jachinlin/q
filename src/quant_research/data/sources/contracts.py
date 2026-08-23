"""定义数据源客户端及流水线采集端口。"""

from collections.abc import Iterable, Mapping, Sequence
from datetime import date
from typing import Protocol

from quant_research.data.contracts import JsonValue, PublishedPartition, RawBatch
from quant_research.domain.identifiers import InstrumentId


class SourceClient(Protocol):
    """约束所有数据源客户端共享的最小采集接口。

    入参：
        按枚举值或实现类契约构造；无额外运行时输入。
    返回值：
        构造并返回 ``SourceClient`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    def fetch_daily_bars(
        self,
        start: date,
        end: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> Iterable[RawBatch]:
        """获取指定证券或交易日范围的日行情 Raw 批次。

        入参：
            start：日期闭区间的开始日期。
            end：日期闭区间的结束日期。
            instruments：需要读取或采集的证券标识集合。
        返回值：
            返回从供应商获取日频行情后的日频行情（``Iterable[RawBatch]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """


class PipelineSource(Protocol):
    """约束数据流水线编排器使用的细粒度采集接口。

    入参：
        按枚举值或实现类契约构造；无额外运行时输入。
    返回值：
        构造并返回 ``PipelineSource`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    @property
    def provider(self) -> str:
        """返回当前数据源的稳定供应商标识。

        入参：
            无。
        返回值：
            返回数据供应商（``str``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ...

    def login(self) -> None:
        """建立供应商会话；重复调用保持幂等。

        入参：
            无。
        返回值：
            无。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ...

    def close(self) -> None:
        """关闭供应商会话；未登录时不执行额外操作。

        入参：
            无。
        返回值：
            无。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ...

    def fetch_instruments(self) -> Iterable[RawBatch]:
        """获取完整的供应商原生证券目录。

        入参：
            无。
        返回值：
            返回从供应商获取证券集合后的证券集合（``Iterable[RawBatch]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """

    def trade_calendar_request(self, start: date, end: date) -> Mapping[str, JsonValue]:
        """构造指定闭区间的规范化交易日历请求。

        入参：
            start：日期闭区间的开始日期。
            end：日期闭区间的结束日期。
        返回值：
            返回交易日历请求（``Mapping[str, JsonValue]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """

    def fetch_trade_calendar(self, start: date, end: date) -> Iterable[RawBatch]:
        """获取指定闭区间的供应商原生交易日历批次。

        入参：
            start：日期闭区间的开始日期。
            end：日期闭区间的结束日期。
        返回值：
            返回从供应商获取交易交易日历后的交易交易日历（``Iterable[RawBatch]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """

    def daily_bars_request(self, trade_date: date) -> Mapping[str, JsonValue]:
        """构造单个开市日的全市场日行情请求。

        入参：
            trade_date：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回行情请求（``Mapping[str, JsonValue]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """

    def fetch_daily_bars(self, trade_date: date) -> Iterable[RawBatch]:
        """获取指定证券或交易日范围的日行情 Raw 批次。

        入参：
            trade_date：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回从供应商获取日频行情后的日频行情（``Iterable[RawBatch]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """

    def etf_bar_requests(
        self, start: date, end: date
    ) -> tuple[Mapping[str, JsonValue], ...]:
        """构造配置中 ETF 白名单的确定性区间请求。

        入参：日期闭区间。返回值：按稳定顺序返回 ETF 请求。异常：日期或配置非法时传播
        ``ValueError``。
        """

    def fetch_etf_bars(self, request: Mapping[str, JsonValue]) -> Iterable[RawBatch]:
        """获取一个 ETF 区间的未复权 Raw 行情。

        入参：规范化 ETF 请求。返回值：供应商 Raw 批次。异常：传播供应商边界异常。
        """

    def benchmark_bars_request(self, trade_date: date) -> Mapping[str, JsonValue]:
        """构造单个开市日的基准指数行情请求。

        入参：
            trade_date：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回行情请求（``Mapping[str, JsonValue]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """

    def fetch_benchmark_bars(self, trade_date: date) -> Iterable[RawBatch]:
        """获取单个开市日的基准指数 Raw 批次。

        入参：
            trade_date：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回从供应商获取基准行情后的基准行情（``Iterable[RawBatch]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """

    def index_bar_requests(
        self, start: date, end: date
    ) -> tuple[Mapping[str, JsonValue], ...]:
        """构造配置中指数的确定性区间请求。

        入参：日期闭区间。返回值：按稳定顺序返回指数请求。异常：日期或配置非法时传播
        ``ValueError``。
        """

    def fetch_index_bars(self, request: Mapping[str, JsonValue]) -> Iterable[RawBatch]:
        """获取一个指数区间的未复权 Raw 行情。

        入参：规范化指数请求。返回值：供应商 Raw 批次。异常：传播供应商边界异常。
        """

    def financial_requests(
        self, start: date, end: date
    ) -> tuple[Mapping[str, JsonValue], ...]:
        """为报告期末闭区间构造已越过披露截止日的财务请求单元。

        入参：
            start：最早报告期末日。
            end：最晚报告期末日。
        返回值：
            返回``requests``（``tuple[Mapping[str, JsonValue], ...]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """

    def fetch_financials(self, request: Mapping[str, JsonValue]) -> Iterable[RawBatch]:
        """获取一个报告单元的供应商原生财务批次。

        入参：
            request：包含完整业务字段的规范化供应商请求。
        返回值：
            返回从供应商获取``financials``后的``financials``（``Iterable[RawBatch]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """

    def industry_requests(
        self, trading_days: Sequence[date]
    ) -> tuple[Mapping[str, JsonValue], ...]:
        """为已完整结束的交易日构造全市场行业分类请求。

        入参：
            trading_days：按升序提供的已完整结束交易日集合。
        返回值：
            返回``requests``（``tuple[Mapping[str, JsonValue], ...]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """

    def fetch_industry(self, request: Mapping[str, JsonValue]) -> Iterable[RawBatch]:
        """获取指定时点的全市场行业分类批次。

        入参：
            request：包含完整业务字段的规范化供应商请求。
        返回值：
            返回从供应商获取行业分类后的行业分类（``Iterable[RawBatch]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """

    def calendar_trading_days(
        self, calendar_partition: PublishedPartition, start: date, end: date
    ) -> tuple[date, ...]:
        """从交易日历分区提取指定闭区间内的开市日期。

        入参：
            calendar_partition：调用接口所需的同名参数，具体约束见类型标注。
            start：日期闭区间的开始日期。
            end：日期闭区间的结束日期。
        返回值：
            返回交易``days``（``tuple[date, ...]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
