"""提供研究界面与查询服务相关的公开模型、协议与处理流程。"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

import polars as pl
from sqlalchemy import Engine, desc, func, select
from sqlalchemy.orm import Session

from quant_research.config import Settings
from quant_research.dashboard.market_review import MarketReviewService
from quant_research.dashboard.models import MarketReviewDates, MarketReviewResponse
from quant_research.data.catalog import DATASET_CATALOG
from quant_research.data.contracts import JsonValue
from quant_research.data.freshness import DatasetFreshness, FreshnessEvaluator
from quant_research.data.quality.catalog import QUALITY_RULE_CATALOG
from quant_research.data.quality.models import QualityIssue, thaw_json
from quant_research.data.repository import ResearchDataRepository
from quant_research.data.sources.routing import RoutingTable
from quant_research.domain.enums import DatasetKind
from quant_research.domain.errors import QuantError
from quant_research.domain.identifiers import QualityRunId
from quant_research.infrastructure.persistence.orm import TaskORM
from quant_research.infrastructure.persistence.repositories import (
    CanonicalDatasetRecord,
    DatasetOperationalStateRecord,
    MetadataRepository,
    QualityRunRecord,
)
from quant_research.infrastructure.persistence.task_queue import TaskQueue
from quant_research.logging import MAX_TASK_LOG_BYTES
from quant_research.tasks.models import TaskRecord, TaskStatus


class DashboardViewService:
    """向接口层提供不暴露基础设施实现的研究工作台用例。

    入参：
        engine：引擎。
        settings：应用设置。
        repository：已由组合根装配的研究数据查询仓库。
        market_review：只读市场全景查询服务。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``RuntimeError``、``TypeError``、``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Return JSON-safe dashboard views without exposing persistence handles.
    """

    def __init__(
        self,
        engine: Engine,
        settings: Settings,
        repository: ResearchDataRepository,
        market_review: MarketReviewService,
        routes: RoutingTable,
    ) -> None:
        self._engine = engine
        self._settings = settings
        self._repository = repository
        self._catalog = MetadataRepository(engine)
        self._tasks = TaskQueue(
            engine,
            task_log_root=settings.data_root / "state" / "task-logs",
        )
        self._market_review = market_review
        self._routes = routes

    def market_review_dates(self) -> MarketReviewDates:
        """返回当前验证目录支持的市场全景交易日。

        入参：无。
        返回值：不可变的市场全景日期目录 DTO。
        异常：数据门未开放或必要数据集覆盖不完整时传播依赖异常。
        """

        return self._market_review.available_dates()

    def market_review(
        self, trade_date: date | None, *, exclude_st: bool
    ) -> MarketReviewResponse:
        """返回指定交易日和股票口径的完整市场全景。

        入参：
            trade_date：目标交易日；为空时使用最新有效交易日。
            exclude_st：是否统一剔除风险警示股票。
        返回值：不可变的市场全景 DTO。
        异常：日期、数据门、目录身份或 Canonical 内容异常时传播依赖异常。
        """

        return self._market_review.review(trade_date, exclude_st=exclude_st)

    def health(self) -> dict[str, object]:
        """汇总 Dashboard 健康状态研究工作台。

        入参：
            无。
        返回值：
            返回``health``（``dict[str, object]``）。
        异常：
            无。
        """
        with self._engine.connect() as connection:
            connection.exec_driver_sql("SELECT 1").scalar_one()
        return {"status": "ok", "service": "quant-dashboard", "version": "1"}

    def overview(self) -> dict[str, object]:
        """返回研究工作台的就绪状态、运行证据与近期研究结果。

        入参：
            无。
        返回值：
            返回可由 ``OverviewResponse`` 严格校验的 JSON 安全字典。
        异常：
            无。
        """
        operations = self.data_summary()
        datasets = self._catalog.list_canonical_datasets()
        active_statuses = (
            TaskStatus.QUEUED,
            TaskStatus.RUNNING,
            TaskStatus.CANCEL_REQUESTED,
        )
        active_candidates = [
            item
            for status in active_statuses
            for item in self._tasks.list(status=status, limit=5)
        ]
        active_tasks = tuple(
            sorted(
                active_candidates,
                key=lambda item: (item.updated_at, item.id),
                reverse=True,
            )[:5]
        )
        task_status_counts = {status.value: 0 for status in TaskStatus}
        with Session(self._engine) as session:
            for status, count in session.execute(
                select(TaskORM.status, func.count())
                .select_from(TaskORM)
                .group_by(TaskORM.status)
            ):
                task_status_counts[str(status)] = int(count)
        latest_trade_date = max(
            (item.end_date for item in datasets if item.end_date is not None),
            default=None,
        )
        return {
            "gate": operations["gate"],
            "freshness": operations["freshness"],
            "latest_trade_date": _ServicesSupport._iso(latest_trade_date),
            "dataset_count": len(datasets),
            "gate_quality_run": operations["gate_quality_run"],
            "latest_quality_run": operations["latest_quality_run"],
            "worker": operations["worker"],
            "last_successful_update": operations["last_successful_update"],
            "tasks": {
                "status_counts": task_status_counts,
                "active": tuple(_ServicesSupport._task(item) for item in active_tasks),
            },
        }

    def data_summary(self) -> dict[str, object]:
        """返回数据中心门禁、新鲜度与更新任务汇总。

        入参：
            无。
        返回值：
            返回不包含持久化句柄的 JSON 安全汇总。
        异常：
            依赖查询异常按原契约传播。
        """
        state = self._catalog.catalog_state()
        runs = self._catalog.list_quality_runs()
        latest = runs[0] if runs else None
        gate_run = (
            self._catalog.get_quality_run(state.quality_run_id)
            if state.quality_run_id is not None
            else None
        )
        freshness, latest_complete_session = self._freshness_state()
        initialization = self._catalog.find_data_initialization()
        data_tasks = tuple(
            sorted(
                (
                    *self._tasks.list(task_type="DATA_BOOTSTRAP", limit=100),
                    *self._tasks.list(task_type="DATA_UPDATE", limit=100),
                ),
                key=lambda item: (item.created_at, item.id),
                reverse=True,
            )
        )
        active = next(
            (
                item
                for item in data_tasks
                if item.status
                in {
                    TaskStatus.QUEUED,
                    TaskStatus.RUNNING,
                    TaskStatus.CANCEL_REQUESTED,
                }
            ),
            None,
        )
        last_success = next(
            (item for item in data_tasks if item.status is TaskStatus.SUCCEEDED), None
        )
        with Session(self._engine) as session:
            active_research = int(
                session.scalar(
                    select(func.count())
                    .select_from(TaskORM)
                    .where(
                        TaskORM.task_type == "EXPERIMENT_RUN",
                        TaskORM.status.in_(["QUEUED", "RUNNING", "CANCEL_REQUESTED"]),
                    )
                )
                or 0
            )
            worker_row = session.scalar(
                select(TaskORM)
                .where(TaskORM.heartbeat_at.is_not(None))
                .order_by(desc(TaskORM.heartbeat_at))
                .limit(1)
            )
        counts = {
            status: sum(item.status.value == status for item in freshness)
            for status in ("CURRENT", "STALE", "MISSING", "UNKNOWN")
        }
        overall = (
            "MISSING"
            if counts["MISSING"]
            else "STALE"
            if counts["STALE"]
            else "UNKNOWN"
            if counts["UNKNOWN"]
            else "CURRENT"
        )
        return {
            "initialization": (
                {
                    "status": "NOT_STARTED",
                    "years": None,
                    "start_date": None,
                    "end_date": None,
                    "started_at": None,
                    "completed_at": None,
                }
                if initialization is None
                else {
                    "status": initialization.status,
                    "years": initialization.years,
                    "start_date": initialization.start_date.isoformat(),
                    "end_date": initialization.end_date.isoformat(),
                    "started_at": _ServicesSupport._iso(initialization.started_at),
                    "completed_at": _ServicesSupport._iso(
                        initialization.completed_at
                    ),
                }
            ),
            "gate": {
                "status": "READY" if state.is_validated else "BLOCKED",
                "reason": self._gate_reason(
                    state.is_validated, state.catalog_hash, runs
                ),
                "catalog_hash": state.catalog_hash,
                "validated_catalog_hash": state.validated_catalog_hash,
                "quality_run_id": (
                    None if state.quality_run_id is None else str(state.quality_run_id)
                ),
                "updated_at": _ServicesSupport._iso(state.updated_at),
                "validated_at": _ServicesSupport._iso(state.validated_at),
            },
            "freshness": {
                "status": overall,
                "counts": counts,
                "evaluated_at": (
                    freshness[0].evaluated_at.isoformat() if freshness else None
                ),
                "latest_complete_session": _ServicesSupport._iso(
                    latest_complete_session
                ),
            },
            "gate_quality_run": self._quality_run_summary(gate_run),
            "latest_quality_run": self._quality_run_summary(latest),
            "active_update": None if active is None else _ServicesSupport._task(active),
            "last_successful_update": (
                None if last_success is None else _ServicesSupport._task(last_success)
            ),
            "worker": (
                None
                if worker_row is None
                else {
                    "worker_id": worker_row.worker_id,
                    "task_id": worker_row.id,
                    "task_status": worker_row.status,
                    "heartbeat_at": worker_row.heartbeat_at,
                }
            ),
            "active_research_task_count": active_research,
        }

    def data_datasets(self) -> dict[str, object]:
        """返回全部数据资产及其新鲜度和运营状态。

        入参：无。返回值：返回不可变响应可验证的字典。异常：依赖查询异常按原契约传播。
        """
        """返回全部目录定义、Canonical 汇总与新鲜度状态。"""

        current = {
            item.dataset: item for item in self._catalog.list_canonical_datasets()
        }
        states = {
            item.dataset: item
            for item in self._catalog.list_dataset_operational_states()
        }
        fresh = {item.dataset: item for item in self._freshness_state()[0]}
        latest = self._catalog.latest_quality_run()
        return {
            "items": tuple(
                self._dataset_summary(
                    dataset,
                    current.get(dataset),
                    states.get(dataset),
                    fresh[dataset],
                    latest,
                )
                for dataset in DATASET_CATALOG
            )
        }

    def data_dataset(self, dataset: str) -> dict[str, object]:
        """返回单个数据集契约、分区和运营详情。

        入参：
            dataset：Canonical 数据集名称。
        返回值：
            返回不含受信文件路径的详情。
        异常：
            ValueError：数据集名称不在目录中时抛出。
        """
        definition = DATASET_CATALOG.parse(dataset)
        current = self._catalog.find_canonical_dataset(definition.kind)
        states = {
            item.dataset: item
            for item in self._catalog.list_dataset_operational_states()
        }
        freshness = {item.dataset: item for item in self._freshness_state()[0]}[
            definition.kind
        ]
        summary = self._dataset_summary(
            definition.kind,
            current,
            states.get(definition.kind),
            freshness,
            self._catalog.latest_quality_run(),
        )
        return {
            **summary,
            "contract": {
                "partitioning": definition.partitioning.value,
                "fetch_granularity": definition.fetch_granularity.value,
                "cadence": definition.cadence.value,
                "reuse": definition.reuse.value,
                "overlap_days": definition.overlap_days,
                "primary_key": tuple(definition.schema.primary_key),
                "sort_key": tuple(definition.schema.sort_key),
                "pit_fields": tuple(definition.pit_fields),
                "schema": tuple(
                    {"name": name, "type": str(dtype)}
                    for name, dtype in definition.schema.columns.items()
                ),
                "sources": tuple(
                    {
                        "source": source,
                        "endpoints": tuple(item.endpoint for item in endpoints),
                    }
                    for source, endpoints in definition.source_endpoints.items()
                ),
            },
            "partitions": (
                ()
                if current is None
                else tuple(
                    {
                        "partition_key": item.partition_key,
                        "ordinal": index,
                        "row_count": item.row_count,
                        "content_hash": item.content_hash,
                        "schema_fingerprint": item.schema_fingerprint,
                        "input_hash": item.input_hash,
                    }
                    for index, item in enumerate(current.partitions)
                )
            ),
        }

    def quality_runs(
        self,
        *,
        scope: str | None,
        status: str | None,
        dataset: str | None,
        severity: str | None,
        rule: str | None,
        page: int,
        page_size: int,
    ) -> dict[str, object]:
        """分页筛选质量运行历史。

        入参：范围、状态、数据集、严重度、规则和分页参数。返回值：分页结果。异常：非法参数抛出 ValueError。
        """
        """分页筛选质量运行历史。"""

        runs = list(self._catalog.list_quality_runs())
        if scope:
            runs = [item for item in runs if item.scope.upper() == scope.upper()]
        if status:
            runs = [item for item in runs if item.status.upper() == status.upper()]
        if dataset:
            runs = [item for item in runs if dataset in item.dataset_hashes]
        if severity:
            runs = [
                item
                for item in runs
                if any(
                    issue.severity.value == severity.upper() for issue in item.issues
                )
            ]
        if rule:
            runs = [
                item
                for item in runs
                if any(issue.rule_id == rule for issue in item.issues)
            ]
        total = len(runs)
        start = (page - 1) * page_size
        return {
            "items": tuple(
                self._quality_run_summary(item)
                for item in runs[start : start + page_size]
            ),
            "page": page,
            "page_size": page_size,
            "total": total,
        }

    def quality_run(self, run_id: str) -> dict[str, object]:
        """返回一次质量运行的完整审计证据。

        入参：质量运行标识。返回值：问题字段和数据集哈希。异常：运行不存在或标识非法时按原契约传播。
        """
        """返回一次质量运行的完整审计证据。"""

        run = self._catalog.get_quality_run(QualityRunId.parse(run_id))
        issues = tuple(self._quality_issue_view(issue) for issue in run.issues)
        rule_results = self._quality_rule_results(run, issues)
        counts = {
            status: sum(item["status"] == status for item in rule_results)
            for status in ("PASS", "FAIL", "SKIPPED", "UNKNOWN")
        }
        return {
            **cast(dict[str, object], self._quality_run_summary(run)),
            "dataset_hashes": dict(run.dataset_hashes),
            "results_complete": run.results_complete,
            "result_counts": counts,
            "rule_results": rule_results,
            "issues": issues,
        }

    @staticmethod
    def _quality_issue_view(issue: QualityIssue) -> dict[str, object]:
        """把持久化质量问题转换为 JSON 安全详情。"""
        return {
            "rule_id": issue.rule_id,
            "severity": issue.severity.value,
            "dataset": issue.dataset.value,
            "scope": thaw_json(issue.scope),
            "actual": thaw_json(issue.actual),
            "threshold": thaw_json(issue.threshold),
            "message": issue.message,
            "remediation": issue.remediation,
        }

    @staticmethod
    def _quality_rule_results(
        run: QualityRunRecord,
        issues: tuple[dict[str, object], ...],
    ) -> tuple[dict[str, object], ...]:
        """返回运行快照，或为旧运行显式构造 FAIL/UNKNOWN 证据。"""
        issues_by_key: dict[tuple[str, str], list[dict[str, object]]] = {}
        for issue in issues:
            key = (str(issue["dataset"]), str(issue["rule_id"]))
            issues_by_key.setdefault(key, []).append(issue)
        if run.results_complete:
            return tuple(
                {
                    "rule_id": result.rule_id,
                    "dataset": result.dataset.value,
                    "status": result.status.value,
                    "severity": result.severity.value,
                    "title": result.title,
                    "description": result.description,
                    "pass_criterion": result.pass_criterion,
                    "scope": thaw_json(result.scope),
                    "actual": thaw_json(result.actual),
                    "threshold": thaw_json(result.threshold),
                    "skip_reason": result.skip_reason,
                    "evidence": result.evidence.value,
                    "issues": tuple(
                        issues_by_key.get((result.dataset.value, result.rule_id), ())
                    ),
                }
                for result in run.rule_results
            )

        rows: list[dict[str, object]] = []
        known_keys: set[tuple[str, str]] = set()
        run_datasets = tuple(DatasetKind(item) for item in sorted(run.dataset_hashes))
        for dataset in run_datasets:
            for definition in QUALITY_RULE_CATALOG:
                if dataset not in definition.datasets:
                    continue
                key = (dataset.value, definition.rule_id)
                known_keys.add(key)
                matching = tuple(issues_by_key.get(key, ()))
                legacy_issue = matching[0] if matching else None
                rows.append(
                    {
                        "rule_id": definition.rule_id,
                        "dataset": dataset.value,
                        "status": "FAIL" if legacy_issue is not None else "UNKNOWN",
                        "severity": definition.severity.value,
                        "title": definition.title,
                        "description": definition.description,
                        "pass_criterion": definition.pass_criterion,
                        "scope": {} if legacy_issue is None else legacy_issue["scope"],
                        "actual": None
                        if legacy_issue is None
                        else legacy_issue["actual"],
                        "threshold": None
                        if legacy_issue is None
                        else legacy_issue["threshold"],
                        "skip_reason": (
                            "该历史运行未保存规则执行证据。"
                            if legacy_issue is None
                            else None
                        ),
                        "evidence": "MISSING"
                        if legacy_issue is None
                        else "LEGACY_ISSUE",
                        "issues": matching,
                    }
                )
        for key in sorted(set(issues_by_key) - known_keys):
            matching = tuple(issues_by_key[key])
            issue = matching[0]
            rows.append(
                {
                    "rule_id": key[1],
                    "dataset": key[0],
                    "status": "FAIL",
                    "severity": issue["severity"],
                    "title": key[1],
                    "description": "历史运行保存的质量问题；当前规则目录中已无对应定义。",
                    "pass_criterion": "历史运行未保存通过条件。",
                    "scope": issue["scope"],
                    "actual": issue["actual"],
                    "threshold": issue["threshold"],
                    "skip_reason": None,
                    "evidence": "LEGACY_ISSUE",
                    "issues": matching,
                }
            )
        return tuple(rows)

    def _freshness_state(
        self,
    ) -> tuple[tuple[DatasetFreshness, ...], date | None]:
        evaluated_at = datetime.now(UTC)
        latest_session = self._latest_complete_session(evaluated_at)
        evaluator = FreshnessEvaluator(
            DATASET_CATALOG,
            timezone=self._settings.timezone,
        )
        return (
            evaluator.evaluate(
                canonical=self._catalog.list_canonical_datasets(),
                operational=self._catalog.list_dataset_operational_states(),
                evaluated_at=evaluated_at,
                latest_complete_session=latest_session,
            ),
            latest_session,
        )

    def _latest_complete_session(self, evaluated_at: datetime) -> date | None:
        local = evaluated_at.astimezone(self._settings.timezone)
        cutoff = local.date() if local.hour >= 18 else local.date() - timedelta(days=1)
        calendar = self._catalog.find_canonical_dataset(DatasetKind.TRADE_CALENDAR)
        if calendar is None or calendar.end_date is None or calendar.end_date < cutoff:
            return None
        if not self._catalog.catalog_state().is_validated:
            return None
        try:
            rows = (
                self._repository.trade_calendar(cutoff - timedelta(days=45), cutoff)
                .filter(pl.col("is_trading_day"))
                .select(pl.col("trade_date").max())
                .collect()
            )
        except (QuantError, ValueError, KeyError, OSError):
            return None
        if rows.height == 0:
            return None
        value = rows.item(0, 0)
        return value if isinstance(value, date) else None

    @staticmethod
    def _gate_reason(
        validated: bool,
        catalog_hash: str,
        runs: tuple[QualityRunRecord, ...],
    ) -> str:
        if validated:
            return "VALIDATED"
        latest_global = next(
            (item for item in runs if item.scope.upper() == "ALL"), None
        )
        if latest_global is None:
            return "NEVER_VALIDATED"
        if latest_global.input_hash != catalog_hash:
            return "CATALOG_CHANGED"
        return "VALIDATION_FAILED"

    @staticmethod
    def _quality_run_summary(run: QualityRunRecord | None) -> dict[str, object] | None:
        if run is None:
            return None
        return {
            "run_id": str(run.id),
            "scope": run.scope,
            "input_hash": run.input_hash,
            "status": run.status,
            "started_at": _ServicesSupport._iso(run.started_at),
            "completed_at": _ServicesSupport._iso(run.completed_at),
            "issue_count": len(run.issues),
            "blocking_issue_count": sum(
                issue.severity.value in {"SEVERE", "FATAL"} for issue in run.issues
            ),
        }

    @staticmethod
    def _dataset_summary(
        dataset: DatasetKind,
        current: CanonicalDatasetRecord | None,
        operational: DatasetOperationalStateRecord | None,
        freshness: DatasetFreshness,
        latest_run: QualityRunRecord | None,
    ) -> dict[str, object]:
        definition = DATASET_CATALOG[dataset]
        issues = (
            ()
            if latest_run is None
            else tuple(item for item in latest_run.issues if item.dataset is dataset)
        )
        source = (
            current.source
            if current is not None
            else next(iter(definition.source_endpoints), None)
        )
        return {
            "dataset": dataset.value,
            "source": source,
            "start_date": None
            if current is None
            else _ServicesSupport._iso(current.start_date),
            "end_date": None
            if current is None
            else _ServicesSupport._iso(current.end_date),
            "partition_count": 0 if current is None else len(current.partitions),
            "row_count": 0
            if current is None
            else sum(item.row_count for item in current.partitions),
            "content_hash": None if current is None else current.content_hash,
            "updated_at": None
            if current is None
            else _ServicesSupport._iso(current.updated_at),
            "partitioning": definition.partitioning.value,
            "cadence": definition.cadence.value,
            "fetch_granularity": definition.fetch_granularity.value,
            "reuse": definition.reuse.value,
            "overlap_days": definition.overlap_days,
            "freshness": {
                "status": freshness.status.value,
                "actual_watermark": _ServicesSupport._iso(freshness.actual_watermark),
                "expected_watermark": _ServicesSupport._iso(
                    freshness.expected_watermark
                ),
                "lag_days": freshness.lag_days,
                "evaluated_at": freshness.evaluated_at.isoformat(),
                "reason": freshness.reason,
                "trigger_date": _ServicesSupport._iso(freshness.trigger_date),
                "update_required": freshness.update_required,
            },
            "operational": {
                "last_localized_at": None
                if operational is None
                else _ServicesSupport._iso(operational.last_localized_at),
                "localized_through": None
                if operational is None
                else _ServicesSupport._iso(operational.localized_through),
                "last_curated_at": None
                if operational is None
                else _ServicesSupport._iso(operational.last_curated_at),
                "last_validated_at": None
                if operational is None
                else _ServicesSupport._iso(operational.last_validated_at),
            },
            "quality_issue_count": len(issues),
            "blocking_issue_count": sum(
                item.severity.value in {"SEVERE", "FATAL"} for item in issues
            ),
        }

    def data_catalog(self) -> dict[str, object]:
        """处理研究工作台中的数据数据目录。

        入参：
            无。
        返回值：
            返回数据目录（``dict[str, object]``）。
        异常：
            无。
        """
        state = self._catalog.catalog_state()
        current = {
            item.dataset: item for item in self._catalog.list_canonical_datasets()
        }
        datasets: list[dict[str, object]] = []
        for dataset in DATASET_CATALOG:
            item = current.get(dataset)
            routes = self._routes[dataset]
            datasets.append(
                {
                    "dataset": dataset.value,
                    "source": (
                        item.source
                        if item is not None
                        else (routes[0].source if routes else None)
                    ),
                    "start_date": None
                    if item is None
                    else _ServicesSupport._iso(item.start_date),
                    "end_date": None
                    if item is None
                    else _ServicesSupport._iso(item.end_date),
                    "partition_count": (None if item is None else len(item.partitions)),
                    "row_count": (
                        None
                        if item is None
                        else sum(part.row_count for part in item.partitions)
                    ),
                    "content_hash": None if item is None else item.content_hash,
                    "updated_at": None
                    if item is None
                    else _ServicesSupport._iso(item.updated_at),
                }
            )
        return {
            "catalog_hash": state.catalog_hash,
            "validated_catalog_hash": state.validated_catalog_hash,
            "is_validated": state.is_validated,
            "updated_at": _ServicesSupport._iso(state.updated_at),
            "validated_at": _ServicesSupport._iso(state.validated_at),
            "datasets": datasets,
        }

    def quality(self) -> dict[str, object]:
        """处理研究工作台中的质量校验。

        入参：
            无。
        返回值：
            返回质量校验（``dict[str, object]``）。
        异常：
            无。
        """
        run = self._catalog.latest_quality_run()
        if run is None:
            return {"run": None, "issues": []}
        return {
            "run": {
                "id": str(run.id),
                "scope": run.scope,
                "input_hash": run.input_hash,
                "status": run.status,
                "started_at": _ServicesSupport._iso(run.started_at),
                "completed_at": _ServicesSupport._iso(run.completed_at),
            },
            "issues": [
                {
                    "rule_id": issue.rule_id,
                    "severity": issue.severity.value,
                    "dataset": issue.dataset.value,
                    "message": issue.message,
                    "remediation": issue.remediation,
                }
                for issue in run.issues
            ],
        }

    def task_list(
        self,
        *,
        status: str | None,
        task_type: str | None = None,
        page: int,
        page_size: int,
    ) -> dict[str, object]:
        """分页返回任务列表与不受当前筛选影响的全局状态计数。

        入参：
            status：当前记录所处的受控生命周期状态。
            task_type：可选任务类型筛选；为空时返回所有任务类型。
            page：页码。
            page_size：页码字节数。
        返回值：
            返回任务页、分页信息和 ``status_counts``，类型为 ``dict[str, object]``。
        异常：
            无。
        """
        parsed = TaskStatus(status) if status else None
        items = self._tasks.list(
            status=parsed,
            task_type=task_type,
            limit=page_size,
            offset=(page - 1) * page_size,
        )
        with Session(self._engine) as session:
            statement = select(func.count()).select_from(TaskORM)
            if parsed is not None:
                statement = statement.where(TaskORM.status == parsed.value)
            if task_type is not None:
                statement = statement.where(TaskORM.task_type == task_type)
            total = int(session.scalar(statement) or 0)
            status_counts = {item.value: 0 for item in TaskStatus}
            for task_status, count in session.execute(
                select(TaskORM.status, func.count())
                .select_from(TaskORM)
                .group_by(TaskORM.status)
            ):
                status_counts[str(task_status)] = int(count)
        return {
            "items": [_ServicesSupport._task(item) for item in items],
            "page": page,
            "page_size": page_size,
            "total": total,
            "status_counts": status_counts,
        }

    def task_detail(self, task_id: str) -> dict[str, object]:
        """返回任务运行详情与按新到旧排列的有界尝试历史。

        入参：
            task_id：目标任务标识，类型为 ``str``。
        返回值：
            返回任务、只读 ``payload`` 及尝试摘要；日志只暴露 ``has_log``
            而不暴露路径。
        异常：
            无。
        """
        task = self._tasks.get(task_id)
        return {
            **_ServicesSupport._task(task),
            "payload": _ServicesSupport._task_payload(task.payload),
            "attempts": [
                {
                    "id": item.id,
                    "attempt_no": item.attempt_no,
                    "status": item.status.value,
                    "worker_id": item.worker_id,
                    "started_at": _ServicesSupport._iso(item.started_at),
                    "heartbeat_at": _ServicesSupport._iso(item.heartbeat_at),
                    "completed_at": _ServicesSupport._iso(item.completed_at),
                    "progress": item.progress,
                    "error": item.error,
                    "has_log": item.log_path is not None,
                    "result": item.result,
                }
                for item in self._tasks.list_attempts(task_id)
            ],
        }

    def task_log(
        self, task_id: str, attempt_id: str, tail_lines: int
    ) -> dict[str, object]:
        """读取受信任任务日志尾部并提取可操作的结构化失败诊断。

        入参：
            task_id：目标任务标识，类型为 ``str``。
            attempt_id：一次任务执行尝试的 UUID 标识。
            tail_lines：尾部``lines``。
        返回值：
            返回日志可用性、行数、截断状态、尾部文本和 ``diagnostic``。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if not 1 <= tail_lines <= 5000:
            raise ValueError("tail_lines must be from 1 through 5000")
        attempt = next(
            (
                item
                for item in self._tasks.list_attempts(task_id)
                if item.id == attempt_id
            ),
            None,
        )
        if attempt is None:
            raise ValueError("attempt does not belong to task")
        if attempt.log_path is None:
            return _ServicesSupport._task_log_payload(
                task_id=task_id,
                attempt_id=attempt_id,
                available=False,
                all_lines=(),
                tail_lines=tail_lines,
                error=attempt.error,
                progress=attempt.progress,
            )
        root = (self._settings.data_root / "state" / "task-logs").resolve()
        path = Path(attempt.log_path).resolve()
        if not path.is_relative_to(root):
            raise ValueError("registered task log is outside the trusted root")
        if not path.is_file():
            return _ServicesSupport._task_log_payload(
                task_id=task_id,
                attempt_id=attempt_id,
                available=False,
                all_lines=(),
                tail_lines=tail_lines,
                error=attempt.error,
                progress=attempt.progress,
            )
        stat = path.stat()
        if stat.st_size > MAX_TASK_LOG_BYTES:
            raise ValueError("registered task log exceeds the safe size limit")
        text = path.read_text(encoding="utf-8")
        return _ServicesSupport._task_log_payload(
            task_id=task_id,
            attempt_id=attempt_id,
            available=True,
            all_lines=tuple(text.splitlines()),
            tail_lines=tail_lines,
            error=attempt.error,
            progress=attempt.progress,
        )


class _ServicesSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _task(item: TaskRecord) -> dict[str, object]:
        return {
            "id": item.id,
            "subject_kind": item.subject_kind,
            "subject_id": item.subject_id,
            "task_type": item.task_type,
            "status": item.status.value,
            "priority": item.priority,
            "progress": item.progress,
            "created_at": _ServicesSupport._iso(item.created_at),
            "started_at": _ServicesSupport._iso(item.locked_at),
            "updated_at": _ServicesSupport._iso(item.updated_at),
            "heartbeat_at": _ServicesSupport._iso(item.heartbeat_at),
            "completed_at": _ServicesSupport._iso(item.completed_at),
            "worker_id": item.worker_id,
            "error": item.error,
            "result": item.result,
        }

    @staticmethod
    def _task_payload(payload: dict[str, JsonValue]) -> dict[str, JsonValue]:
        return {key: payload[key] for key in sorted(payload)}

    @staticmethod
    def _task_log_payload(
        *,
        task_id: str,
        attempt_id: str,
        available: bool,
        all_lines: tuple[str, ...],
        tail_lines: int,
        error: dict[str, JsonValue] | None,
        progress: dict[str, JsonValue],
    ) -> dict[str, object]:
        returned = all_lines[-tail_lines:]
        return {
            "task_id": task_id,
            "attempt_id": attempt_id,
            "available": available,
            "lines": list(returned),
            "total_lines": len(all_lines),
            "truncated": len(returned) < len(all_lines),
            "diagnostic": _ServicesSupport._task_diagnostic(
                all_lines,
                error=error,
                progress=progress,
            ),
        }

    @staticmethod
    def _task_diagnostic(
        lines: tuple[str, ...],
        *,
        error: dict[str, JsonValue] | None,
        progress: dict[str, JsonValue],
    ) -> dict[str, object] | None:
        stage = _ServicesSupport._text_value(progress.get("stage"))
        substage = _ServicesSupport._progress_substage(progress)
        fallback = _ServicesSupport._persisted_diagnostic(
            error,
            stage=stage,
            substage=substage,
        )
        for line in reversed(lines):
            try:
                raw_record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if (
                not isinstance(raw_record, dict)
                or raw_record.get("event") != "task.handler_failed"
            ):
                continue
            context = raw_record.get("context")
            values = context if isinstance(context, dict) else {}
            logged_substage = _ServicesSupport._progress_substage(
                values.get("last_progress")
            )
            return {
                "code": _ServicesSupport._text_value(raw_record.get("error_code"))
                or (None if fallback is None else fallback["code"]),
                "message": _ServicesSupport._text_value(values.get("exception_message"))
                or (
                    None
                    if fallback is None
                    else _ServicesSupport._text_value(fallback.get("message"))
                ),
                "exception_type": _ServicesSupport._text_value(
                    values.get("exception_type")
                ),
                "stage": _ServicesSupport._text_value(raw_record.get("stage")) or stage,
                "substage": logged_substage or substage,
                "retryable": _ServicesSupport._bool_value(values.get("retryable"))
                if _ServicesSupport._bool_value(values.get("retryable")) is not None
                else (
                    None
                    if fallback is None
                    else _ServicesSupport._bool_value(fallback.get("retryable"))
                ),
                "remediation": _ServicesSupport._text_value(values.get("remediation"))
                or (
                    None
                    if fallback is None
                    else _ServicesSupport._text_value(fallback.get("remediation"))
                )
                or "检查完整 traceback 和本次任务输入后再安全重试。",
                "traceback": _ServicesSupport._text_value(values.get("traceback")),
            }
        return fallback

    @staticmethod
    def _persisted_diagnostic(
        error: dict[str, JsonValue] | None,
        *,
        stage: str | None,
        substage: str | None,
    ) -> dict[str, object] | None:
        if error is None:
            return None
        code = _ServicesSupport._text_value(error.get("code"))
        message = _ServicesSupport._text_value(error.get("message"))
        retryable = _ServicesSupport._bool_value(error.get("retryable"))
        remediation = _ServicesSupport._text_value(error.get("remediation"))
        if code == "TASK_ORPHANED" and remediation is None:
            remediation = "确认 Worker 已停止，检查未发布临时产物后再确认重试。"
        elif code is not None and remediation is None:
            remediation = "查看该次尝试日志，修复原因后再安全重试。"
        return {
            "code": code,
            "message": message,
            "exception_type": None,
            "stage": stage,
            "substage": substage,
            "retryable": retryable,
            "remediation": remediation,
            "traceback": None,
        }

    @staticmethod
    def _progress_substage(value: object) -> str | None:
        if not isinstance(value, dict):
            return None
        context = value.get("context")
        if not isinstance(context, dict):
            return None
        return _ServicesSupport._text_value(context.get("substage"))

    @staticmethod
    def _text_value(value: object) -> str | None:
        return value if isinstance(value, str) and value else None

    @staticmethod
    def _bool_value(value: object) -> bool | None:
        return value if isinstance(value, bool) else None

    @staticmethod
    def _iso(value: object) -> str | None:
        if value is None:
            return None
        method = getattr(value, "isoformat", None)
        if not callable(method):
            raise TypeError("value must provide isoformat()")
        result = method()
        if not isinstance(result, str):
            raise TypeError("isoformat() must return text")
        return result
