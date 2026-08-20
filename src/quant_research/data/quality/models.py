"""定义质量评估使用的不可变输入与结果模型。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from types import MappingProxyType

from quant_research.data.contracts import JsonScalar, JsonValue, canonical_json_bytes
from quant_research.domain.enums import DatasetKind, Severity

type FrozenJsonValue = (
    JsonScalar | tuple["FrozenJsonValue", ...] | Mapping[str, "FrozenJsonValue"]
)
type QualityJsonValue = (
    JsonScalar
    | list["QualityJsonValue"]
    | tuple["QualityJsonValue", ...]
    | Mapping[str, "QualityJsonValue"]
)


class QualityRuleStatus(StrEnum):
    """定义单条质量规则的执行状态。

    入参：无。返回值：枚举成员表示规则结果。异常：非法枚举值由 ``StrEnum`` 拒绝。
    """

    PASS = "PASS"
    FAIL = "FAIL"
    SKIPPED = "SKIPPED"


class QualityEvidenceSource(StrEnum):
    """定义质量规则结果的证据来源。

    入参：无。返回值：枚举成员表示证据来源。异常：非法枚举值由 ``StrEnum`` 拒绝。
    """

    RUN_SNAPSHOT = "RUN_SNAPSHOT"


@dataclass(frozen=True, slots=True)
class QualityRuleResult:
    """保存一次规则与数据集组合的不可变执行证据。

    入参：由字段声明给出。返回值：构造冻结结果。异常：字段或 JSON 值非法时抛出校验异常。
    """

    rule_id: str
    dataset: DatasetKind
    status: QualityRuleStatus
    severity: Severity
    title: str
    description: str
    pass_criterion: str
    scope: Mapping[str, QualityJsonValue]
    actual: QualityJsonValue
    threshold: QualityJsonValue
    skip_reason: str | None = None
    evidence: QualityEvidenceSource = QualityEvidenceSource.RUN_SNAPSHOT

    def __post_init__(self) -> None:
        if not all((self.rule_id, self.title, self.description, self.pass_criterion)):
            raise ValueError("quality rule result text fields must not be empty")
        if self.status is QualityRuleStatus.SKIPPED and not self.skip_reason:
            raise ValueError("skipped quality rule result requires a reason")
        if (
            self.status is not QualityRuleStatus.SKIPPED
            and self.skip_reason is not None
        ):
            raise ValueError("only skipped quality rule result may have a reason")
        frozen_scope = freeze_json(dict(self.scope))
        frozen_actual = freeze_json(self.actual)
        frozen_threshold = freeze_json(self.threshold)
        canonical_json_bytes(thaw_json(frozen_scope))
        canonical_json_bytes(thaw_json(frozen_actual))
        canonical_json_bytes(thaw_json(frozen_threshold))
        if not isinstance(frozen_scope, Mapping):
            raise TypeError("quality rule result scope must be a mapping")
        object.__setattr__(self, "scope", frozen_scope)
        object.__setattr__(self, "actual", frozen_actual)
        object.__setattr__(self, "threshold", frozen_threshold)


@dataclass(frozen=True, slots=True)
class QualityEvaluation:
    """汇总一次质量执行产生的规则结果和失败问题。

    入参：由字段声明给出。返回值：构造冻结汇总。异常：无主动抛出的异常。
    """

    rule_results: tuple[QualityRuleResult, ...]
    issues: tuple[QualityIssue, ...]


@dataclass(frozen=True, slots=True)
class QualityIssue:
    """描述一个可执行修复的数据质量规则违例。

    入参：
        rule_id：产生该问题的稳定质量规则标识。
        severity：问题严重级别，用于决定是否阻断研究读取。
        dataset：目标 Canonical 数据集标识。
        scope：定位问题数据范围的确定性 JSON 对象，例如日期或证券集合。
        actual：规则在该范围内观测到的实际值或统计证据。
        threshold：规则用于判定通过与否的期望值或边界。
        message：面向操作者的问题说明。
        remediation：修复数据、配置或上游采集问题的建议。
    返回值：
        构造并返回 ``QualityIssue`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    rule_id: str
    severity: Severity
    dataset: DatasetKind
    scope: Mapping[str, QualityJsonValue]
    actual: QualityJsonValue
    threshold: QualityJsonValue
    message: str
    remediation: str

    def __post_init__(self) -> None:
        if not self.rule_id or not self.message or not self.remediation:
            raise ValueError("quality issue text fields must not be empty")
        frozen_scope = freeze_json(dict(self.scope))
        frozen_actual = freeze_json(self.actual)
        frozen_threshold = freeze_json(self.threshold)
        canonical_json_bytes(thaw_json(frozen_scope))
        canonical_json_bytes(thaw_json(frozen_actual))
        canonical_json_bytes(thaw_json(frozen_threshold))
        if not isinstance(frozen_scope, Mapping):
            raise TypeError("quality issue scope must be a mapping")
        object.__setattr__(self, "scope", frozen_scope)
        object.__setattr__(self, "actual", frozen_actual)
        object.__setattr__(self, "threshold", frozen_threshold)


@dataclass(frozen=True, slots=True)
class QualityRunSpec:
    """绑定一个精确 Canonical 状态上的质量运行。

    入参：
        dataset_hashes：本次运行绑定的数据集名称到当前内容哈希的映射。
        input_hash：被校验 Canonical 状态的目录身份；全目录运行时即 ``catalog_hash``。
        scope：质量运行范围，只允许 ``ALL`` 或 ``DATASET``。
        started_at：质量规则开始执行的带时区时间。
        completed_at：全部规则完成的带时区时间；运行中为 ``None``。
        issues：由失败规则生成、需要处置的数据质量问题集合。
    返回值：
        构造并返回 ``QualityRunSpec`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    dataset_hashes: Mapping[str, str]
    input_hash: str
    scope: str
    started_at: datetime
    completed_at: datetime | None
    issues: tuple[QualityIssue, ...]
    rule_results: tuple[QualityRuleResult, ...] = ()
    results_complete: bool = False

    def __post_init__(self) -> None:
        if not self.dataset_hashes:
            raise ValueError("quality run must bind at least one canonical dataset")
        if self.scope not in {"ALL", "DATASET"}:
            raise ValueError("quality run scope must be ALL or DATASET")
        _QualityModelValidator.require_hash(self.input_hash, "input_hash")
        for dataset, content_hash in self.dataset_hashes.items():
            if not dataset:
                raise ValueError("quality dataset name must not be empty")
            _QualityModelValidator.require_hash(content_hash, "dataset content hash")
        started = _QualityModelValidator.utc(self.started_at, "started_at")
        completed = (
            _QualityModelValidator.utc(self.completed_at, "completed_at")
            if self.completed_at is not None
            else None
        )
        if completed is not None and completed < started:
            raise ValueError("completed_at must not precede started_at")
        object.__setattr__(self, "started_at", started)
        object.__setattr__(self, "completed_at", completed)
        object.__setattr__(
            self, "dataset_hashes", MappingProxyType(dict(self.dataset_hashes))
        )
        object.__setattr__(self, "issues", tuple(self.issues))
        object.__setattr__(self, "rule_results", tuple(self.rule_results))
        if self.results_complete and not self.rule_results:
            raise ValueError("complete quality run must contain rule results")
        result_keys = tuple(
            (result.dataset.value, result.rule_id) for result in self.rule_results
        )
        if len(result_keys) != len(set(result_keys)):
            raise ValueError("quality rule results must be unique per dataset and rule")
        if any(dataset not in self.dataset_hashes for dataset, _ in result_keys):
            raise ValueError("quality rule result dataset must belong to the run")
        if self.results_complete:
            failed_keys = {
                (result.dataset.value, result.rule_id)
                for result in self.rule_results
                if result.status is QualityRuleStatus.FAIL
            }
            issue_keys = {(issue.dataset.value, issue.rule_id) for issue in self.issues}
            if not issue_keys <= failed_keys:
                raise ValueError("every quality issue must have a failed rule result")


def freeze_json(value: object) -> FrozenJsonValue:
    """递归复制 JSON 值并转换为不可变容器；该函数是质量模型的稳定转换 API，因此保留为模块级入口。

    入参：
        value：待处理或解析的输入值。
    返回值：
        返回``json``（``FrozenJsonValue``）。
    异常：
        TypeError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
    """
    if isinstance(value, Mapping):
        return MappingProxyType(
            {str(key): freeze_json(item) for key, item in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(freeze_json(item) for item in value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def thaw_json(value: object) -> JsonValue:
    """将不可变 JSON 容器恢复为普通持久化容器；该函数是质量模型的稳定转换 API，因此保留为模块级入口。

    入参：
        value：待处理或解析的输入值。
    返回值：
        返回``json``（``JsonValue``）。
    异常：
        TypeError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
    """
    if isinstance(value, Mapping):
        return {str(key): thaw_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [thaw_json(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


class _QualityModelValidator:
    """集中校验质量模型共享的时间与哈希字段。"""

    @staticmethod
    def utc(value: datetime, field: str) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{field} must be timezone-aware")
        return value.astimezone(UTC)

    @staticmethod
    def require_hash(value: object, field: str) -> None:
        if not isinstance(value, str) or len(value) != 64:
            raise ValueError(f"{field} must be a SHA-256 digest")
        try:
            int(value, 16)
        except ValueError as error:
            raise ValueError(f"{field} must be a SHA-256 digest") from error
