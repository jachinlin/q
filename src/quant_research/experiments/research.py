"""定义自动研究族、执行、候选与运行的不可变领域记录。"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, field_validator

from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.research_protocols import ResearchMode


class ResearchStatus(StrEnum):
    """定义研究族执行和单次运行的生命周期。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    CREATED = "CREATED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class ResearchPhase(StrEnum):
    """定义候选评估和锁定测试阶段。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    TRAIN_VALIDATION = "TRAIN_VALIDATION"
    TEST = "TEST"


class ResearchStage(StrEnum):
    """定义每个运行的固定七阶段。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    VALIDATE = "VALIDATE"
    UNIVERSE = "UNIVERSE"
    RESEARCH_COMPUTE = "RESEARCH_COMPUTE"
    SIMULATE = "SIMULATE"
    ANALYTICS = "ANALYTICS"
    ARTIFACT_VERIFY = "ARTIFACT_VERIFY"
    REGISTER = "REGISTER"


class ResearchMark(StrEnum):
    """定义研究者对研究族的可审计标记。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    UNREVIEWED = "UNREVIEWED"
    BASELINE = "BASELINE"
    CANDIDATE = "CANDIDATE"
    DISCARDED = "DISCARDED"


class _Record(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("created_at", "started_at", "completed_at", check_fields=False)
    @classmethod
    def normalize_time(cls, value: datetime | None) -> datetime | None:
        """将持久化边界时间统一为 UTC。"""
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("research timestamps must be timezone-aware")
        return value.astimezone(UTC)


class ResearchFamilyRecord(_Record):
    """表示一个不可变研究问题和组件搜索定义。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    id: str
    name: str
    hypothesis: str
    strategy_id: str
    research_mode: ResearchMode
    config: dict[str, JsonValue]
    config_hash: str
    mark: ResearchMark
    note: str | None
    created_at: datetime
    archived_at: datetime | None

    @field_validator("config")
    @classmethod
    def validate_config(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        """确认研究配置可确定性序列化。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        canonical_json_bytes(value)
        return value


class FamilyExecutionRecord(_Record):
    """表示研究族在一组数据、源码和规则身份上的执行。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    id: str
    family_id: str
    catalog_hash: str
    source_hash: str
    lockfile_hash: str
    rulebook_hash: str
    environment_hash: str
    status: ResearchStatus
    selected_variant_id: str | None
    selection_reason: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    error: dict[str, JsonValue] | None


class ResearchVariantRecord(_Record):
    """表示一次执行内展开的确定性候选组合。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    id: str
    execution_id: str
    ordinal: int
    composition_hash: str
    parameters: dict[str, JsonValue]
    config: dict[str, JsonValue]
    rejection_reasons: tuple[str, ...]
    created_at: datetime


class ResearchRunRecord(_Record):
    """表示候选在 TRAIN/VALIDATION 或 TEST 上的一次不可变运行。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    id: str
    execution_id: str
    variant_id: str
    phase: ResearchPhase
    status: ResearchStatus
    stage: ResearchStage
    stage_status: dict[str, JsonValue]
    manifest_path: str | None
    manifest_hash: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    error: dict[str, JsonValue] | None


class ResearchMetricRecord(_Record):
    """表示按样本分区登记的标量研究指标。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    run_id: str
    split: str
    category: str
    name: str
    value: float
    unit: str | None
    p_value: float | None
    adjusted_p_value: float | None


class ResearchArtifactRecord(_Record):
    """表示由可信 Manifest 登记的不可变研究产物。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    id: int
    execution_id: str
    run_id: str | None
    relative_path: str
    artifact_type: str
    producer_component_id: str
    content_hash: str
    byte_count: int
    row_count: int | None
    metadata: dict[str, JsonValue]
    created_at: datetime
