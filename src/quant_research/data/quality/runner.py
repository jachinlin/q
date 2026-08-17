"""以确定顺序执行全部基础质量规则。"""

from __future__ import annotations

from collections.abc import Callable

import polars as pl

from quant_research.data.quality.catalog import (
    QUALITY_RULE_CATALOG,
    QualityRuleDefinition,
)
from quant_research.data.quality.models import (
    QualityEvaluation,
    QualityIssue,
    QualityJsonValue,
    QualityRuleResult,
    QualityRuleStatus,
)
from quant_research.data.quality.rules import (
    CanonicalPartitions,
    canonical_conforming_partitions,
    canonical_schema_issues,
    coverage_issues,
    cross_partition_schema_issues,
    daily_bar_value_issues,
    financial_availability_issues,
    industry_state_issues,
    primary_key_issues,
    required_dataset_issues,
    required_value_issues,
)
from quant_research.domain.enums import DatasetKind


class QualityRunner:
    """执行全部基础质量检查并返回稳定排序的问题。

    入参：
        按枚举值或实现类契约构造；无额外运行时输入。
    返回值：
        构造并返回 ``QualityRunner`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    def evaluate(
        self,
        inputs: CanonicalPartitions,
        heartbeat: Callable[[], None] = lambda: None,
    ) -> QualityEvaluation:
        """执行基础质量规则并以稳定顺序返回全部问题。

        入参：
            inputs：质量规则使用的 Canonical 分区集合。
            heartbeat：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回``evaluate``（``QualityEvaluation``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        heartbeat()
        issues = required_dataset_issues(inputs, frozenset(inputs))
        issues.extend(canonical_schema_issues(inputs))
        conforming_inputs = canonical_conforming_partitions(inputs)
        for rule in (
            primary_key_issues,
            required_value_issues,
            daily_bar_value_issues,
            coverage_issues,
            financial_availability_issues,
            industry_state_issues,
        ):
            heartbeat()
            issues.extend(rule(conforming_inputs))
        heartbeat()
        issues.extend(cross_partition_schema_issues(inputs))
        heartbeat()
        ordered_issues = tuple(
            sorted(issues, key=lambda issue: (issue.dataset.value, issue.rule_id))
        )
        return QualityEvaluation(
            rule_results=self._rule_results(inputs, ordered_issues),
            issues=ordered_issues,
        )

    @staticmethod
    def _rule_results(
        inputs: CanonicalPartitions,
        issues: tuple[QualityIssue, ...],
    ) -> tuple[QualityRuleResult, ...]:
        """把问题集合扩展成每个适用规则与数据集的完整结果。"""
        issue_by_key = {(issue.dataset, issue.rule_id): issue for issue in issues}
        status_by_key: dict[tuple[DatasetKind, str], QualityRuleStatus] = {}
        results: list[QualityRuleResult] = []
        datasets = tuple(sorted(inputs, key=lambda item: item.value))
        for dataset in datasets:
            for definition in QUALITY_RULE_CATALOG:
                if dataset not in definition.datasets:
                    continue
                issue = issue_by_key.get((dataset, definition.rule_id))
                skip_reason = QualityRunner._skip_reason(
                    definition, dataset, inputs, status_by_key
                )
                status = (
                    QualityRuleStatus.FAIL
                    if issue is not None
                    else QualityRuleStatus.SKIPPED
                    if skip_reason is not None
                    else QualityRuleStatus.PASS
                )
                actual, threshold = (
                    (None, None)
                    if status is QualityRuleStatus.SKIPPED
                    else QualityRunner._evidence(
                        definition.rule_id, dataset, inputs, issue
                    )
                )
                result = QualityRuleResult(
                    rule_id=definition.rule_id,
                    dataset=dataset,
                    status=status,
                    severity=definition.severity,
                    title=definition.title,
                    description=definition.description,
                    pass_criterion=definition.pass_criterion,
                    scope={} if issue is None else issue.scope,
                    actual=actual,
                    threshold=threshold,
                    skip_reason=skip_reason,
                )
                results.append(result)
                status_by_key[(dataset, definition.rule_id)] = status
        return tuple(results)

    @staticmethod
    def _skip_reason(
        definition: QualityRuleDefinition,
        dataset: DatasetKind,
        inputs: CanonicalPartitions,
        statuses: dict[tuple[DatasetKind, str], QualityRuleStatus],
    ) -> str | None:
        failed_prerequisites = tuple(
            rule_id
            for rule_id in definition.prerequisite_rules
            if statuses.get((dataset, rule_id)) is not QualityRuleStatus.PASS
        )
        if failed_prerequisites:
            return "前置规则未通过：" + ", ".join(failed_prerequisites)
        missing_datasets = tuple(
            item.value
            for item in definition.prerequisite_datasets
            if item not in inputs or item not in canonical_conforming_partitions(inputs)
        )
        if missing_datasets:
            return "缺少可用依赖数据集：" + ", ".join(missing_datasets)
        return None

    @staticmethod
    def _evidence(
        rule_id: str,
        dataset: DatasetKind,
        inputs: CanonicalPartitions,
        issue: QualityIssue | None,
    ) -> tuple[QualityJsonValue, QualityJsonValue]:
        if issue is not None:
            return issue.actual, issue.threshold
        partitions = inputs.get(dataset, ())
        if rule_id == "required_dataset_missing":
            return len(partitions), 1
        if rule_id == "required_dataset_empty":
            rows = 0
            for partition_frame in partitions:
                lazy = (
                    partition_frame.lazy()
                    if isinstance(partition_frame, pl.DataFrame)
                    else partition_frame
                )
                rows += int(lazy.select(pl.len()).collect().item())
            return rows, 1
        if rule_id == "trading_window_empty":
            conforming_partitions = canonical_conforming_partitions(inputs).get(
                dataset, ()
            )
            count = 0
            for partition in conforming_partitions:
                lazy = (
                    partition.lazy()
                    if isinstance(partition, pl.DataFrame)
                    else partition
                )
                count += int(
                    lazy.filter(pl.col("is_trading_day"))
                    .select(pl.len())
                    .collect()
                    .item()
                )
            return count, 1
        return 0, 0
