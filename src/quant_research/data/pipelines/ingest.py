"""提供不可变 Raw 分区证据的稳定序列化接口。"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import cast

from quant_research.data.contracts import (
    JsonValue,
    PublishedPartition,
    canonical_json_bytes,
)
from quant_research.data.partitions import RawPartitionStore
from quant_research.data.storage import resolved_storage_root


def partition_to_json(partition: PublishedPartition) -> dict[str, JsonValue]:
    """将已发布 Raw 分区编码为可持久化 JSON；该函数是稳定的序列化边界，因此保留为模块级入口。

    入参：
        partition：待读取、校验或映射的分区。
    返回值：
        返回目标JSON（``dict[str, JsonValue]``）。
    异常：
        实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
    """
    return {
        "source": partition.source,
        "endpoint": partition.endpoint,
        "request": dict(partition.request),
        "retrieved_at": partition.retrieved_at.isoformat(),
        "data_path": partition.data_path.absolute().as_posix(),
        "manifest_path": partition.manifest_path.absolute().as_posix(),
        "request_hash": partition.request_hash,
        "content_hash": partition.content_hash,
        "schema_fingerprint": partition.schema_fingerprint,
        "row_count": partition.row_count,
    }


def partition_from_json(
    value: Mapping[str, object], raw_root: Path
) -> PublishedPartition:
    """从持久化 JSON 重建已发布 Raw 分区；该函数是稳定的序列化边界，因此保留为模块级入口。

    入参：
        value：待处理或解析的输入值。
        raw_root：调用接口所需的同名参数，具体约束见类型标注。
    返回值：
        返回来源JSON（``PublishedPartition``）。
    异常：
        TypeError、ValueError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
    """
    root = resolved_storage_root(raw_root)
    source = str(value["source"])
    endpoint = str(value["endpoint"])
    request_value = value["request"]
    if not isinstance(request_value, Mapping):
        raise TypeError("raw evidence request must be a mapping")
    request = cast(Mapping[str, JsonValue], request_value)
    request_hash = str(value["request_hash"])
    if hashlib.sha256(canonical_json_bytes(request)).hexdigest() != request_hash:
        raise ValueError("raw evidence request hash is invalid")
    content_hash = str(value["content_hash"])
    request_dir = root / f"source={source}" / f"endpoint={endpoint}" / request_hash
    partition = PublishedPartition(
        source=source,
        endpoint=endpoint,
        request=request,
        retrieved_at=datetime.fromisoformat(str(value["retrieved_at"])),
        data_path=Path(str(value["data_path"])).absolute(),
        manifest_path=Path(str(value["manifest_path"])).absolute(),
        request_hash=request_hash,
        content_hash=content_hash,
        schema_fingerprint=str(value["schema_fingerprint"]),
        row_count=int(str(value["row_count"])),
    )
    if partition.data_path != request_dir / f"{content_hash}.parquet":
        raise ValueError("raw evidence data path does not match canonical layout")
    if partition.manifest_path != request_dir / "manifest.json":
        raise ValueError("raw evidence manifest path does not match canonical layout")
    RawPartitionStore.verify_partition(partition)
    return partition
