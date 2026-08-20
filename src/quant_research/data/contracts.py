"""定义供应商边界与 Canonical 边界之间的不可变数据契约。"""

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from quant_research.domain.enums import DatasetKind

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
        daily_bars：是否能够提供证券日频价量行情。
        trade_calendar：是否能够提供交易日历及开闭市状态。
        instruments：是否能够提供证券主数据和上市状态。
        security_status：是否能够提供停牌、ST 等逐日交易状态。
        financials_with_announcement_date：是否能够提供带公告日期的财务数据。
        adjustment_factors：是否能够提供复权因子或计算复权所需的等价数据。
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
        retrieved_at：供应商响应完成时的带时区时间，保存时统一转换为 UTC。
        schema：供应商响应中字段的确定性顺序。
        rows：与 ``schema`` 对应的供应商原始记录序列。
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
        retrieved_at：该 Raw 响应从供应商取得的 UTC 时间。
        data_path：可信 Raw 根目录内的内容寻址 Parquet 路径。
        manifest_path：记录请求、内容身份和完整性元数据的 manifest 路径。
        request_hash：规范化请求 JSON 的 SHA-256，用于请求级幂等定位。
        content_hash：Raw 表逻辑内容的 SHA-256，用于内容寻址和完整性校验。
        schema_fingerprint：Raw Arrow Schema 的稳定指纹。
        row_count：已发布 Raw Parquet 的记录数。
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
        source_content_hashes：生成该数据帧的 Raw 内容哈希，按确定性顺序保存。
    返回值：
        构造并返回 ``CanonicalBatch`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    dataset: DatasetKind
    frame: pl.DataFrame
    source_content_hashes: tuple[str, ...]
