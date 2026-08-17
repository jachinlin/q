"""定义供应商边界与 Canonical 边界之间的不可变数据契约。"""

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Protocol

import polars as pl

from quant_research.domain.enums import DatasetKind
from quant_research.domain.identifiers import InstrumentId

type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | list[JsonValue] | Mapping[str, JsonValue]


def canonical_json_bytes(value: JsonValue) -> bytes:
    """将 JSON 值序列化为确定性的 UTF-8 字节；该函数是跨模块稳定序列化 API，因此保留为模块级入口。

    入参：
        value：待处理或解析的输入值。
    返回值：
        返回JSON字节（``bytes``）。
    异常：
        ValueError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
    """
    try:
        serialized = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as error:
        raise ValueError("value must be JSON serializable") from error
    return serialized.encode("utf-8")


@dataclass(frozen=True, slots=True)
class ProviderCapabilities:
    """描述供应商能够可靠提供的数据能力。

    入参：
        daily_bars：构造对象所需的同名字段，约束见类型标注。
        trade_calendar：构造对象所需的同名字段，约束见类型标注。
        instruments：需要读取或采集的证券标识集合。
        security_status：构造对象所需的同名字段，约束见类型标注。
        financials_with_announcement_date：构造对象所需的同名字段，约束见类型标注。
        adjustment_factors：构造对象所需的同名字段，约束见类型标注。
    返回值：
        构造并返回 ``ProviderCapabilities`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    daily_bars: bool
    trade_calendar: bool
    instruments: bool
    security_status: bool
    financials_with_announcement_date: bool
    adjustment_factors: bool

    @classmethod
    def complete(cls) -> "ProviderCapabilities":
        """构造声明全部研究输入均受支持的能力集合。

        入参：
            无。
        返回值：
            返回``complete``（``'ProviderCapabilities'``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        return cls(
            daily_bars=True,
            trade_calendar=True,
            instruments=True,
            security_status=True,
            financials_with_announcement_date=True,
            adjustment_factors=True,
        )

    def missing(self, required: Sequence[str]) -> tuple[str, ...]:
        """返回当前供应商未支持的已声明能力名称。

        入参：
            required：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回缺失项（``tuple[str, ...]``）。
        异常：
            ValueError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        missing: list[str] = []
        for name in required:
            if not isinstance(name, str) or not hasattr(self, name):
                raise ValueError(f"unknown provider capability: {name!r}")
            if not getattr(self, name):
                missing.append(name)
        return tuple(missing)


@dataclass(frozen=True, slots=True, init=False)
class RawBatch:
    """封装一次供应商无关且可复现的 Raw 响应批次。

    入参：
        source：供应商标识。
        endpoint：供应商原生端点名称。
        request：包含完整业务字段的规范化供应商请求。
        retrieved_at：构造对象所需的同名字段，约束见类型标注。
        schema：构造对象所需的同名字段，约束见类型标注。
        rows：构造对象所需的同名字段，约束见类型标注。
    返回值：
        构造并返回 ``RawBatch`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    source: str
    endpoint: str
    request: Mapping[str, JsonValue]
    retrieved_at: datetime
    schema: tuple[str, ...]
    rows: Sequence[Mapping[str, JsonValue]]

    def __init__(
        self,
        *,
        source: str,
        endpoint: str,
        request: Mapping[str, JsonValue],
        retrieved_at: datetime,
        schema: tuple[str, ...],
        rows: Sequence[Mapping[str, JsonValue]],
    ) -> None:
        """Reject timestamps or requests that cannot be reproduced."""
        if retrieved_at.tzinfo is None or retrieved_at.utcoffset() is None:
            raise ValueError("retrieved_at must be timezone-aware")
        if not source or not endpoint:
            raise ValueError("source and endpoint must not be empty")
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "endpoint", endpoint)
        object.__setattr__(self, "request", request)
        object.__setattr__(self, "retrieved_at", retrieved_at.astimezone(UTC))
        object.__setattr__(self, "schema", schema)
        object.__setattr__(self, "rows", rows)
        canonical_json_bytes(request)


@dataclass(frozen=True, slots=True)
class PublishedPartition:
    """描述一个可见的不可变 Raw 分区及其完整性元数据。

    入参：
        source：供应商标识。
        endpoint：供应商原生端点名称。
        request：包含完整业务字段的规范化供应商请求。
        retrieved_at：构造对象所需的同名字段，约束见类型标注。
        data_path：构造对象所需的同名字段，约束见类型标注。
        manifest_path：构造对象所需的同名字段，约束见类型标注。
        request_hash：构造对象所需的同名字段，约束见类型标注。
        content_hash：构造对象所需的同名字段，约束见类型标注。
        schema_fingerprint：构造对象所需的同名字段，约束见类型标注。
        row_count：构造对象所需的同名字段，约束见类型标注。
    返回值：
        构造并返回 ``PublishedPartition`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    source: str
    endpoint: str
    request: Mapping[str, JsonValue]
    retrieved_at: datetime
    data_path: Path
    manifest_path: Path
    request_hash: str
    content_hash: str
    schema_fingerprint: str
    row_count: int


@dataclass(frozen=True, slots=True)
class CanonicalBatch:
    """封装由已发布 Raw 证据规范化得到的 Canonical 数据帧。

    入参：
        dataset：目标 Canonical 数据集标识。
        frame：待校验或转换的数据帧。
        source_content_hashes：构造对象所需的同名字段，约束见类型标注。
    返回值：
        构造并返回 ``CanonicalBatch`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    dataset: DatasetKind
    frame: pl.DataFrame
    source_content_hashes: tuple[str, ...]


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


class CanonicalMapper(Protocol):
    """约束 Raw 分区到 Canonical 批次的纯映射接口。

    入参：
        按枚举值或实现类契约构造；无额外运行时输入。
    返回值：
        构造并返回 ``CanonicalMapper`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    def accepts_raw_schema(self, endpoint: str, schema_fingerprint: str) -> bool:
        """判断 Raw 元数据是否符合当前端点契约。

        入参：
            endpoint：供应商原生端点名称。
            schema_fingerprint：Raw 字段名称和类型形成的确定性 Schema 身份。
        返回值：
            当前 mapper 支持该端点及 Schema 时返回 ``True``，否则返回 ``False``。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """

    def normalize(self, raw_partition: PublishedPartition) -> Iterable[CanonicalBatch]:
        """读取并校验已发布 Raw 分区，再生成 Canonical 批次。

        入参：
            raw_partition：已发布且不可变的 Raw 分区。
        返回值：
            返回规范化Canonical 数据后的``normalize``（``Iterable[CanonicalBatch]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """

    def candidate_partition_keys(
        self, dataset: DatasetKind, raw_partition: PublishedPartition
    ) -> tuple[str, ...]:
        """在不读取 Raw 文件的情况下推导候选 Canonical 分区键。

        入参：
            dataset：目标 Canonical 数据集标识。
            raw_partition：已发布且不可变的 Raw 分区。
        返回值：
            返回分区``keys``（``tuple[str, ...]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """

    def transform_hash(self, dataset: DatasetKind) -> str:
        """返回映射代码与目标 Canonical 契约的确定性身份。

        入参：
            dataset：目标 Canonical 数据集标识。
        返回值：
            返回哈希（``str``）。
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

    def financial_requests(
        self, start: date, end: date
    ) -> tuple[Mapping[str, JsonValue], ...]:
        """构造已达到保守披露截止日的财务请求单元。

        入参：
            start：日期闭区间的开始日期。
            end：日期闭区间的结束日期。
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
