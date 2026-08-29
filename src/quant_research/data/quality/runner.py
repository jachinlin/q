"""以确定顺序执行全部基础质量规则。"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence

import polars as pl

from quant_research.data.canonical.schemas import CANONICAL_SCHEMAS
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
    CanonicalFrame,
    CanonicalPartitions,
    canonical_conforming_partitions,
    canonical_schema_issues,
    coverage_issues,
    cross_partition_schema_issues,
    daily_bar_value_issues,
    dividend_event_issues,
    financial_availability_issues,
    industry_state_issues,
    instrument_identifier_issues,
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
        issues = canonical_schema_issues(inputs)
        conforming_inputs = canonical_conforming_partitions(inputs)
        evidence: dict[
            tuple[DatasetKind, str], tuple[QualityJsonValue, QualityJsonValue]
        ] = {}
        coverage_inputs: dict[DatasetKind, tuple[pl.DataFrame, ...]] = {}
        local_rules = (
            primary_key_issues,
            required_value_issues,
            instrument_identifier_issues,
            daily_bar_value_issues,
            dividend_event_issues,
            financial_availability_issues,
            industry_state_issues,
        )
        for dataset in sorted(inputs, key=lambda item: item.value):
            heartbeat()
            materialized = self._materialize(inputs[dataset])
            materialized_input = {dataset: materialized}
            issues.extend(
                required_dataset_issues(materialized_input, frozenset({dataset}))
            )
            evidence[(dataset, "required_dataset_missing")] = (
                len(materialized),
                1,
            )
            evidence[(dataset, "required_dataset_empty")] = (
                sum(frame.height for frame in materialized),
                1,
            )
            if dataset is DatasetKind.TRADE_CALENDAR:
                trading_days = sum(
                    int(frame.get_column("is_trading_day").fill_null(False).sum())
                    for frame in materialized
                    if "is_trading_day" in frame.columns
                )
                evidence[(dataset, "trading_window_empty")] = (trading_days, 1)
            conforming = tuple(
                frame
                for frame in materialized
                if frame.schema == CANONICAL_SCHEMAS[dataset].columns
            )
            local_input = {dataset: conforming}
            for rule in local_rules:
                heartbeat()
                issues.extend(rule(local_input))
            coverage = self._coverage_projection(dataset, conforming)
            if coverage:
                coverage_inputs[dataset] = coverage
        heartbeat()
        issues.extend(coverage_issues(coverage_inputs))
        heartbeat()
        issues.extend(cross_partition_schema_issues(inputs))
        heartbeat()
        ordered_issues = tuple(
            sorted(issues, key=lambda issue: (issue.dataset.value, issue.rule_id))
        )
        return QualityEvaluation(
            rule_results=self._rule_results(
                inputs,
                ordered_issues,
                evidence=evidence,
                conforming_datasets=frozenset(conforming_inputs),
            ),
            issues=ordered_issues,
        )

    @staticmethod
    def _rule_results(
        inputs: CanonicalPartitions,
        issues: tuple[QualityIssue, ...],
        *,
        evidence: Mapping[
            tuple[DatasetKind, str], tuple[QualityJsonValue, QualityJsonValue]
        ],
        conforming_datasets: frozenset[DatasetKind],
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
                    definition,
                    dataset,
                    conforming_datasets,
                    status_by_key,
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
                        definition.rule_id,
                        dataset,
                        issue,
                        evidence,
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
        conforming_datasets: frozenset[DatasetKind],
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
            if item not in conforming_datasets
        )
        if missing_datasets:
            return "缺少可用依赖数据集：" + ", ".join(missing_datasets)
        return None

    @staticmethod
    def _evidence(
        rule_id: str,
        dataset: DatasetKind,
        issue: QualityIssue | None,
        evidence: Mapping[
            tuple[DatasetKind, str], tuple[QualityJsonValue, QualityJsonValue]
        ],
    ) -> tuple[QualityJsonValue, QualityJsonValue]:
        if issue is not None:
            return issue.actual, issue.threshold
        return evidence.get((dataset, rule_id), (0, 0))

    @staticmethod
    def _materialize(
        partitions: Sequence[CanonicalFrame],
    ) -> tuple[pl.DataFrame, ...]:
        """每个质量运行只从物理分区收集一次数据。"""
        lazy = tuple(
            frame.lazy() if isinstance(frame, pl.DataFrame) else frame
            for frame in partitions
        )
        if not lazy:
            return ()
        return tuple(pl.collect_all(lazy))

    @staticmethod
    def _coverage_projection(
        dataset: DatasetKind,
        partitions: Sequence[pl.DataFrame],
    ) -> tuple[pl.DataFrame, ...]:
        """保留跨数据集覆盖规则需要的小型去重投影。"""
        columns = {
            DatasetKind.STOCK_DAILY_BAR: ("trade_date", "instrument_id"),
            DatasetKind.FUND_DAILY_BAR: ("instrument_id",),
            DatasetKind.TRADE_CALENDAR: ("trade_date", "is_trading_day"),
            DatasetKind.STOCK_MASTER: ("instrument_id",),
            DatasetKind.FUND_MASTER: ("instrument_id",),
        }.get(dataset)
        if columns is None:
            return ()
        return tuple(frame.select(columns).unique() for frame in partitions)
