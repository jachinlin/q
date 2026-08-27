"""定义 Raw 证据映射为 Canonical 批次的端口。"""

from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from typing import Protocol

import polars as pl

from quant_research.data.contracts import CanonicalBatch, JsonValue, PublishedPartition
from quant_research.domain.enums import DatasetKind


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
        """根据 Raw 发布信息推导候选 Canonical 分区键。

        入参：
            dataset：目标 Canonical 数据集标识。
            raw_partition：已发布且不可变的 Raw 分区。
        返回值：
            返回分区``keys``（``tuple[str, ...]``）。
        异常：
            实现可读取形成分区键所需的最小 Raw 字段，并传播参数校验、目录状态或
            文件完整性异常。
        """

    def raw_head_is_usable(
        self,
        dataset: DatasetKind,
        request: Mapping[str, JsonValue],
        observed_at: datetime,
    ) -> bool:
        """判断 Raw 当前头在本次 Curate 时点是否已经完整可用。

        入参：目标数据集、规范化 Raw 请求和本次 Curate 观察时点。
        返回值：允许参与当前输入身份与映射时返回 ``True``。
        异常：请求缺少策略所需字段或字段非法时传播 ``TypeError`` 或 ``ValueError``。
        """

    def requires_raw_history(self, dataset: DatasetKind) -> bool:
        """判断重建分区时是否需要读取同一请求的历史 Raw 对象。

        入参：目标数据集。返回值：需要历史观测时返回 ``True``。异常：无主动抛出的异常。
        """

    def consolidate_partition(
        self, dataset: DatasetKind, frames: Sequence[pl.DataFrame]
    ) -> pl.DataFrame:
        """把一个 Canonical 分区的映射片段折叠为最终确定性数据帧。

        入参：目标数据集和按 Raw 身份稳定排序的映射片段。
        返回值：符合目标 Canonical Schema、主键和排序契约的数据帧。
        异常：映射片段不满足数据集策略时传播 ``TypeError`` 或 ``ValueError``。
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
