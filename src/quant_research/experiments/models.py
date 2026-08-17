"""提供实验与领域模型相关的公开模型、协议与处理流程。"""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from quant_research.data.contracts import JsonValue, canonical_json_bytes

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_GIT_OID = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")


class ExperimentStatus(StrEnum):
    """定义 ``ExperimentStatus`` 使用的稳定枚举值。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    CREATED = "CREATED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class ResearchMark(StrEnum):
    """定义 ``ResearchMark`` 使用的稳定枚举值。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    UNREVIEWED = "UNREVIEWED"
    BASELINE = "BASELINE"
    CANDIDATE = "CANDIDATE"
    DISCARDED = "DISCARDED"


class _ExperimentModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ExperimentSpec(_ExperimentModel):
    """定义可持久化并参与身份计算的实验不可变规格。

    入参：
        strategy_id：用于持久化关联和日志追踪的策略标识。
        config：调用所用的配置对象，类型为 ``dict[str, JsonValue]``。
        config_hash：确定性序列化后的实验或策略配置身份。
        data_hash：Canonical 数据内容或本次研究输入的数据身份。
        source_tree_hash：参与幂等、漂移或完整性校验的数据来源``tree``哈希；使用 SHA-256 十六进制文本。
        返回完成字段规范化和不变量校验的对象。
        lockfile_hash：参与幂等、漂移或完整性校验的依赖锁文件哈希；使用 SHA-256 十六进制文本。
        rulebook_hash：唯一 A 股交易规则文件的内容身份。
        fingerprint：由策略、数据、源码、依赖锁和交易规则共同形成的研究指纹。
        created_at：记录创建时的 UTC 时间戳。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    strategy_id: str
    config: dict[str, JsonValue]
    config_hash: str
    data_hash: str
    source_tree_hash: str | None = None
    git_commit_hash: str | None = None
    lockfile_hash: str
    rulebook_hash: str
    fingerprint: str
    created_at: datetime

    @field_validator(
        "config_hash",
        "data_hash",
        "source_tree_hash",
        "lockfile_hash",
        "rulebook_hash",
        "fingerprint",
    )
    @classmethod
    def validate_hash(cls, value: str | None, info: object) -> str | None:
        """校验可选文本为小写 SHA-256 十六进制摘要。

        入参：
            value：待校验或转换的值，类型为 ``str | None``。
            info：Pydantic 传入的字段元数据，用于在错误中指出具体字段。
        返回值：
            返回校验可选文本为小写 SHA-256 十六进制摘要后的哈希（``str | None``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if value is not None and not _SHA256.fullmatch(value):
            field_name = getattr(info, "field_name", "hash")
            raise ValueError(f"{field_name} must be a lowercase SHA-256 hex digest")
        return value

    @field_validator("git_commit_hash")
    @classmethod
    def validate_git_oid(cls, value: str | None) -> str | None:
        """校验 Git 提交标识为 40 或 64 位十六进制文本。

        入参：
            value：待校验或转换的值，类型为 ``str | None``。
        返回值：
            返回校验 Git 提交标识为 40 或 64 位十六进制文本后的Git对象标识（``str | None``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if value is not None and not _GIT_OID.fullmatch(value):
            raise ValueError("git_commit_hash must be a 40- or 64-character Git OID")
        return value

    @field_validator("created_at")
    @classmethod
    def normalize_created_at(cls, value: datetime) -> datetime:
        """规范化创建时间``at``。

        入参：
            value：待校验或转换的值，类型为 ``datetime``。
        返回值：
            返回将创建时间规范化为带时区 UTC 时间戳后的创建时间``at``（``datetime``）。
        异常：
            无。
        """
        return _ModelsSupport._utc(value, "created_at")

    @field_validator("config")
    @classmethod
    def validate_config(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        """确认配置可被确定性 JSON 序列化。

        入参：
            value：待校验或转换的值，类型为 ``dict[str, JsonValue]``。
        返回值：
            返回确认配置可被确定性 JSON 序列化后的配置（``dict[str, JsonValue]``）。
        异常：
            无。
        """
        canonical_json_bytes(value)
        return value

    @model_validator(mode="after")
    def validate_metadata(self) -> ExperimentSpec:
        """校验实验源码身份完整且配置哈希与规范内容一致。

        入参：
            无。
        返回值：
            返回校验实验源码身份完整且配置哈希与规范内容一致后的元数据（``ExperimentSpec``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if not self.strategy_id:
            raise ValueError("strategy_id must not be empty")
        if self.source_tree_hash is None and self.git_commit_hash is None:
            raise ValueError("source_tree_hash or git_commit_hash is required")
        canonical_hash = hashlib.sha256(canonical_json_bytes(self.config)).hexdigest()
        if self.config_hash != canonical_hash:
            raise ValueError("config_hash must match canonical config")
        return self


class ExperimentRecord(_ExperimentModel):
    """表示从持久化边界读取的实验记录快照。

    入参：
        id：用于持久化关联和日志追踪的标识。
        strategy_id：用于持久化关联和日志追踪的策略标识。
        config：调用所用的配置对象，类型为 ``dict[str, JsonValue]``。
        config_hash：确定性序列化后的实验或策略配置身份。
        data_hash：Canonical 数据内容或本次研究输入的数据身份。
        source_tree_hash：参与幂等、漂移或完整性校验的数据来源``tree``哈希；使用 SHA-256 十六进制文本。
        返回完成字段规范化和不变量校验的对象。
        lockfile_hash：参与幂等、漂移或完整性校验的依赖锁文件哈希；使用 SHA-256 十六进制文本。
        rulebook_hash：唯一 A 股交易规则文件的内容身份。
        fingerprint：由策略、数据、源码、依赖锁和交易规则共同形成的研究指纹。
        status：当前记录所处的受控生命周期状态。
        research_mark：用户对实验标记的基线、候选或废弃研究结论。
        created_at：记录创建时的 UTC 时间戳。
        queued_at：记录入队时间``at``的带时区 UTC 时间戳。
        started_at：执行实际开始的 UTC 时间戳。
        completed_at：执行进入终态的 UTC 时间戳。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    id: str
    strategy_id: str
    config: dict[str, JsonValue]
    config_hash: str
    data_hash: str
    source_tree_hash: str | None
    git_commit_hash: str | None
    lockfile_hash: str
    rulebook_hash: str
    fingerprint: str
    status: ExperimentStatus
    research_mark: ResearchMark
    created_at: datetime
    queued_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None


class ExperimentTag(_ExperimentModel):
    """记录实验上的单个用户标签及创建时间。

    入参：
        experiment_id：目标实验标识，类型为 ``str``。
        tag：标签。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    experiment_id: str
    tag: str


class ExperimentMetric(_ExperimentModel):
    """记录实验成功后登记的单个标量指标和单位。

    入参：
        experiment_id：目标实验标识，类型为 ``str``。
        name：供用户识别研究、任务或数据对象的非空名称。
        value：待校验或转换的值，类型为 ``float``。
        unit：计量单位。
        created_at：记录创建时的 UTC 时间戳。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    experiment_id: str
    name: str
    value: float
    unit: str | None = None
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def normalize_created_at(cls, value: datetime) -> datetime:
        """规范化创建时间``at``。

        入参：
            value：待校验或转换的值，类型为 ``datetime``。
        返回值：
            返回将创建时间规范化为带时区 UTC 时间戳后的创建时间``at``（``datetime``）。
        异常：
            无。
        """
        return _ModelsSupport._utc(value, "created_at")


class ExperimentArtifact(_ExperimentModel):
    """记录实验产物的类型、路径、哈希和字节数。

    入参：
        experiment_id：目标实验标识，类型为 ``str``。
        name：供用户识别研究、任务或数据对象的非空名称。
        artifact_type：产物类型。
        path：待处理的文件系统路径，类型为 ``str``。
        content_hash：按规范字节计算、用于内容寻址和完整性校验的 SHA-256。
        metadata：元数据。
        created_at：记录创建时的 UTC 时间戳。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    experiment_id: str
    name: str
    artifact_type: str
    path: str
    content_hash: str
    metadata: dict[str, JsonValue]
    created_at: datetime

    @field_validator("content_hash")
    @classmethod
    def validate_content_hash(cls, value: str) -> str:
        """校验内容哈希。

        入参：
            value：待校验或转换的值，类型为 ``str``。
        返回值：
            返回校验内容哈希后的内容哈希（``str``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if not _SHA256.fullmatch(value):
            raise ValueError("content_hash must be a lowercase SHA-256 hex digest")
        return value

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        """校验实验源码身份完整且配置哈希与规范内容一致。

        入参：
            value：待校验或转换的值，类型为 ``dict[str, JsonValue]``。
        返回值：
            返回校验实验源码身份完整且配置哈希与规范内容一致后的元数据（``dict[str, JsonValue]``）。
        异常：
            无。
        """
        canonical_json_bytes(value)
        return value

    @field_validator("created_at")
    @classmethod
    def normalize_created_at(cls, value: datetime) -> datetime:
        """规范化创建时间``at``。

        入参：
            value：待校验或转换的值，类型为 ``datetime``。
        返回值：
            返回将创建时间规范化为带时区 UTC 时间戳后的创建时间``at``（``datetime``）。
        异常：
            无。
        """
        return _ModelsSupport._utc(value, "created_at")


class _ModelsSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _utc(value: datetime, field: str) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{field} must be timezone-aware")
        return value.astimezone(UTC)
