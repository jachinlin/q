"""提供任务与领域模型相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from quant_research.data.contracts import JsonValue, canonical_json_bytes


class TaskStatus(StrEnum):
    """定义 ``TaskStatus`` 使用的稳定枚举值。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELLED = "CANCELLED"
    ORPHANED = "ORPHANED"


class _TaskModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class TaskSpec(_TaskModel):
    """定义可持久化并参与身份计算的任务不可变规格。

    入参：
        experiment_id：目标实验标识，类型为 ``str | None``。
        task_type：任务类型。
        payload：任务载荷。
        priority：任务在同一可运行集合中的调度优先级。
        created_at：记录创建时的 UTC 时间戳。
        available_at：该条观测首次可供研究使用的带时区时间戳。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    experiment_id: str | None = None
    task_type: str
    payload: dict[str, JsonValue]
    priority: int = 0
    created_at: datetime
    available_at: datetime

    @field_validator("created_at", "available_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime, info: object) -> datetime:
        """规范化``timestamp``。

        入参：
            value：待校验或转换的值，类型为 ``datetime``。
            info：Pydantic 传入的字段元数据，用于在错误中指出具体字段。
        返回值：
            返回将任务时间戳规范化为带时区 UTC 值后的``timestamp``（``datetime``）。
        异常：
            无。
        """
        return _ModelsSupport._utc(value, getattr(info, "field_name", "timestamp"))

    @field_validator("payload")
    @classmethod
    def validate_payload(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        """确认任务载荷是可冻结的确定性 JSON 对象。

        入参：
            value：待校验或转换的值，类型为 ``dict[str, JsonValue]``。
        返回值：
            返回确认任务载荷是可冻结的确定性 JSON 对象后的任务载荷（``dict[str, JsonValue]``）。
        异常：
            无。
        """
        canonical_json_bytes(value)
        return value

    @model_validator(mode="after")
    def validate_identity(self) -> TaskSpec:
        """校验任务标识、幂等键和重试身份之间的一致性。

        入参：
            无。
        返回值：
            返回校验任务标识、幂等键和重试身份之间的一致性后的身份（``TaskSpec``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if self.experiment_id is not None and not self.experiment_id:
            raise ValueError("experiment_id must not be empty")
        if not self.task_type:
            raise ValueError("task_type must not be empty")
        return self


class TaskRecord(_TaskModel):
    """表示从持久化边界读取的任务记录快照。

    入参：
        id：用于持久化关联和日志追踪的标识。
        experiment_id：目标实验标识，类型为 ``str | None``。
        task_type：任务类型。
        payload：任务载荷。
        status：当前记录所处的受控生命周期状态。
        priority：任务在同一可运行集合中的调度优先级。
        progress：当前尝试已完成量、总量和阶段说明。
        created_at：记录创建时的 UTC 时间戳。
        available_at：该条观测首次可供研究使用的带时区时间戳。
        updated_at：记录最近持久化变更的 UTC 时间戳。
        heartbeat_at：Worker 最近证明仍持有任务的 UTC 时间戳。
        completed_at：执行进入终态的 UTC 时间戳。
        idempotency_key：幂等键``key``。
        worker_id：当前 Worker 实例的稳定所有者标识。
        返回完成字段规范化和不变量校验的对象。
        error：需要处理或传播的异常，类型为 ``dict[str, JsonValue] | None``。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    id: str
    experiment_id: str | None
    task_type: str
    payload: dict[str, JsonValue]
    status: TaskStatus
    priority: int
    progress: dict[str, JsonValue]
    created_at: datetime
    available_at: datetime
    updated_at: datetime
    heartbeat_at: datetime | None
    completed_at: datetime | None
    idempotency_key: str | None = None
    worker_id: str | None = None
    locked_at: datetime | None = None
    error: dict[str, JsonValue] | None = None
    result: dict[str, JsonValue] | None = None


class TaskAttemptRecord(_TaskModel):
    """表示从持久化边界读取的任务执行尝试记录快照。

    入参：
        id：用于持久化关联和日志追踪的标识。
        task_id：目标任务标识，类型为 ``str``。
        attempt_no：执行尝试从一开始计数的尝试序号。
        status：当前记录所处的受控生命周期状态。
        worker_id：当前 Worker 实例的稳定所有者标识。
        started_at：执行实际开始的 UTC 时间戳。
        heartbeat_at：Worker 最近证明仍持有任务的 UTC 时间戳。
        completed_at：执行进入终态的 UTC 时间戳。
        log_path：经可信根边界校验后使用的日志路径。
        progress：当前尝试已完成量、总量和阶段说明。
        error：需要处理或传播的异常，类型为 ``dict[str, JsonValue] | None``。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    id: str
    task_id: str
    attempt_no: int
    status: TaskStatus
    worker_id: str | None
    started_at: datetime
    heartbeat_at: datetime | None
    completed_at: datetime | None
    log_path: str | None
    progress: dict[str, JsonValue]
    error: dict[str, JsonValue] | None
    result: dict[str, JsonValue] | None = None


class AuditEventSpec(_TaskModel):
    """定义可持久化并参与身份计算的审计事件事件不可变规格。

    入参：
        event_type：事件类型。
        details：详情。
        created_at：记录创建时的 UTC 时间戳。
        experiment_id：目标实验标识，类型为 ``str | None``。
        task_id：目标任务标识，类型为 ``str | None``。
        actor：操作主体。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    event_type: str
    details: dict[str, JsonValue]
    created_at: datetime
    experiment_id: str | None = None
    task_id: str | None = None
    actor: str | None = None

    @field_validator("details")
    @classmethod
    def validate_details(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        """校验``details``。

        入参：
            value：待校验或转换的值，类型为 ``dict[str, JsonValue]``。
        返回值：
            返回校验``details``后的``details``（``dict[str, JsonValue]``）。
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

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, value: str) -> str:
        """校验事件类型。

        入参：
            value：待校验或转换的值，类型为 ``str``。
        返回值：
            返回校验事件类型后的事件类型（``str``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if not value:
            raise ValueError("event_type must not be empty")
        return value


class AuditEventRecord(AuditEventSpec):
    """表示从持久化边界读取的审计事件事件记录快照。

    入参：
        id：用于持久化关联和日志追踪的标识。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    id: int


class TaskProgress(_TaskModel):
    """记录任务当前阶段、完成量、总量和安全详情。

    入参：
        stage：执行阶段。
        completed：完成。
        total：总量。
        message：面向用户且已脱敏的错误或状态说明。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    stage: str = Field(min_length=1, max_length=128)
    completed: int
    total: int
    message: str = Field(max_length=2048)
    context: dict[str, JsonValue] = Field(default_factory=dict)

    @field_validator("stage")
    @classmethod
    def validate_stage(cls, value: str) -> str:
        """校验任务进度阶段名称非空且长度受限。

        入参：
            value：待校验或转换的值，类型为 ``str``。
        返回值：
            返回校验任务进度阶段名称非空且长度受限后的执行阶段（``str``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if not value.strip():
            raise ValueError("stage must not be empty")
        return value

    @model_validator(mode="after")
    def validate_bounds(self) -> TaskProgress:
        """校验任务完成量与总量均为非负且完成量不越界。

        入参：
            无。
        返回值：
            返回校验任务完成量与总量均为非负且完成量不越界后的``bounds``（``TaskProgress``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if self.total < 0:
            raise ValueError("total must be non-negative")
        if self.completed < 0 or self.completed > self.total:
            raise ValueError("completed must satisfy 0 <= completed <= total")
        return self


class TaskOutcome(_TaskModel):
    """记录任务终态的业务结果或结构化失败信息。

    入参：
        status：当前记录所处的受控生命周期状态。
        error：需要处理或传播的异常，类型为 ``dict[str, JsonValue] | None``。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    status: TaskStatus
    error: dict[str, JsonValue] | None = None
    result: dict[str, JsonValue] | None = None

    @model_validator(mode="after")
    def validate_outcome(self) -> TaskOutcome:
        """校验成功结果与失败错误字段互斥且符合终态。

        入参：
            无。
        返回值：
            返回校验成功结果与失败错误字段互斥且符合终态后的执行结果（``TaskOutcome``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        allowed = {
            TaskStatus.SUCCEEDED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        }
        if self.status not in allowed:
            raise ValueError("outcome status must be SUCCEEDED, FAILED, or CANCELLED")
        if self.status is TaskStatus.FAILED:
            if self.error is None:
                raise ValueError("FAILED outcome requires an error")
            encoded = canonical_json_bytes(self.error)
            if len(encoded) > 65_536:
                raise ValueError("error JSON exceeds 65536 bytes")
        elif self.error is not None:
            raise ValueError("SUCCEEDED and CANCELLED outcomes must not include error")
        if self.status is not TaskStatus.SUCCEEDED and self.result is not None:
            raise ValueError("only SUCCEEDED outcomes may include result")
        if self.result is not None and len(canonical_json_bytes(self.result)) > 65_536:
            raise ValueError("result JSON exceeds 65536 bytes")
        return self


class ClaimedTask(_TaskModel):
    """绑定 Worker 所有权围栏的一次可执行任务及其尝试身份。

    入参：
        id：用于持久化关联和日志追踪的标识。
        attempt_id：一次任务执行尝试的 UUID 标识。
        attempt_no：执行尝试从一开始计数的尝试序号。
        experiment_id：目标实验标识，类型为 ``str | None``。
        task_type：任务类型。
        payload：任务载荷。
        priority：任务在同一可运行集合中的调度优先级。
        worker_id：当前 Worker 实例的稳定所有者标识。
        progress：当前尝试已完成量、总量和阶段说明。
        claimed_at：记录认领``at``的带时区 UTC 时间戳。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    id: str
    attempt_id: str
    attempt_no: int
    experiment_id: str | None
    task_type: str
    payload: dict[str, JsonValue]
    priority: int
    worker_id: str
    progress: TaskProgress
    claimed_at: datetime

    @field_validator("claimed_at")
    @classmethod
    def normalize_claimed_at(cls, value: datetime) -> datetime:
        """规范化认领``at``。

        入参：
            value：待校验或转换的值，类型为 ``datetime``。
        返回值：
            返回将任务认领时间规范化为带时区 UTC 值后的认领``at``（``datetime``）。
        异常：
            无。
        """
        return _ModelsSupport._utc(value, "claimed_at")

    @field_validator("payload")
    @classmethod
    def validate_claimed_payload(
        cls, value: dict[str, JsonValue]
    ) -> dict[str, JsonValue]:
        """校验认领任务载荷。

        入参：
            value：待校验或转换的值，类型为 ``dict[str, JsonValue]``。
        返回值：
            返回校验认领任务载荷后的认领任务载荷（``dict[str, JsonValue]``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        encoded = canonical_json_bytes(value)
        if len(encoded) > 1_048_576:
            raise ValueError("payload JSON exceeds 1048576 bytes")
        return value

    @model_validator(mode="after")
    def validate_claim_identity(self) -> ClaimedTask:
        """校验``claim``身份。

        入参：
            无。
        返回值：
            返回校验``claim``身份后的``claim``身份（``ClaimedTask``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if (
            not self.id
            or not self.attempt_id
            or not self.task_type
            or not self.worker_id
        ):
            raise ValueError("claim identity fields must not be empty")
        if self.attempt_no <= 0:
            raise ValueError("attempt_no must be positive")
        if self.experiment_id is not None and not self.experiment_id:
            raise ValueError("experiment_id must not be empty")
        return self


class _ModelsSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _utc(value: datetime, field: str) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{field} must be timezone-aware")
        return value.astimezone(UTC)
