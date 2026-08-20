"""提供 commands 模块的公开模型、协议与处理流程。"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Protocol, cast

from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.data.pipeline.publish import DataUpdatePlan
from quant_research.data.repository import CanonicalCatalog
from quant_research.domain.enums import DatasetKind, Severity
from quant_research.domain.errors import ErrorDetail, QuantError
from quant_research.experiments.fingerprint import (
    ExperimentFingerprintInput,
    capture_environment,
    compute_fingerprint,
)
from quant_research.experiments.models import (
    ExperimentRecord,
    ExperimentSpec,
    ResearchMark,
)
from quant_research.experiments.query import ExperimentQuery
from quant_research.experiments.registry import ExperimentRegistry
from quant_research.factor_studies.contracts import FactorStudyStore
from quant_research.factor_studies.models import FactorStudyConfig
from quant_research.tasks.models import (
    TaskRecord,
    TaskStatus,
)


class _ExperimentSubmitter(Protocol):
    """定义 Dashboard 所需的实验文本提交边界。"""

    def create_and_submit_from_yaml_text(
        self,
        config_yaml: str,
        *,
        priority: int = 0,
        actor: str = "dashboard",
        request_id: str | None = None,
    ) -> tuple[ExperimentRecord, TaskRecord]: ...


class ExperimentDeletionPort(Protocol):
    """定义应用层删除非活动实验所需的持久化与文件清理边界。

    入参：
        由具体实现的构造契约定义。
    返回值：
        返回满足实验删除端口的实现实例。
    异常：
        具体实现的构造异常按原契约传播。
    """

    def delete(
        self,
        experiment_id: str,
        actor: str,
        *,
        request_id: str,
    ) -> None:
        """删除实验及其受控资源。

        入参：
            experiment_id：目标实验标识。
            actor：执行删除的审计主体。
            request_id：写请求的关联标识。
        返回值：
            无。
        异常：
            活动实验或清理失败时抛出结构化异常。
        """


class _ResearchTaskQueue(Protocol):
    """约束研究写用例所需的任务队列端口。"""

    def enqueue(
        self,
        task_type: str,
        payload: Mapping[str, JsonValue],
        priority: int,
        experiment_id: str | None = None,
        *,
        idempotency_key: str | None = None,
        available_at: datetime | None = None,
        actor: str = "system",
        request_id: str | None = None,
    ) -> str: ...

    def get(self, task_id: str) -> TaskRecord: ...

    def submit_backtest(
        self,
        experiment_id: str,
        config_hash: str,
        *,
        priority: int = 0,
        actor: str = "notebook",
        request_id: str | None = None,
    ) -> str: ...

    def request_cancel(
        self,
        task_id: str,
        actor: str,
        *,
        request_id: str | None = None,
        strict: bool = False,
    ) -> None: ...

    def delete(
        self,
        task_id: str,
        actor: str,
        *,
        request_id: str | None = None,
    ) -> None: ...

    def retry(
        self,
        task_id: str,
        actor: str,
        *,
        available_at: datetime | None = None,
        request_id: str | None = None,
    ) -> str: ...

    def clone_for_retry(
        self,
        task_id: str,
        actor: str,
        *,
        request_id: str | None = None,
    ) -> tuple[str | None, str]: ...


class _DataUpdatePlanner(Protocol):
    """约束 Dashboard 创建数据更新任务时所需的计划能力。"""

    def plan(
        self,
        *,
        start: date | None,
        end: date | None,
        datasets: tuple[DatasetKind, ...] | None = None,
    ) -> DataUpdatePlan: ...


class ResearchApplicationService:
    """执行 Dashboard 允许的受控写操作并维护审计身份。

    入参：
        queue：持久化任务状态、认领和重试的任务队列。
        query：查询条件。
        registry：登记并查询不可变业务身份和生命周期状态的登记簿。
        catalog：提供当前 Canonical 数据身份和质量门禁的只读目录。
        source_root：所有派生路径必须位于其中的数据来源可信根目录。
        factor_studies：因子``studies``。
        experiment_submitter：实验提交端口。
        experiment_deletion：删除实验数据库记录与受控文件资源的应用端口。
        data_update_planner：生成可预览并固化执行的数据更新计划。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    """

    def __init__(
        self,
        *,
        queue: _ResearchTaskQueue,
        query: ExperimentQuery,
        registry: ExperimentRegistry,
        catalog: CanonicalCatalog,
        source_root: object,
        factor_studies: FactorStudyStore,
        experiment_submitter: _ExperimentSubmitter,
        experiment_deletion: ExperimentDeletionPort,
        data_update_planner: _DataUpdatePlanner,
    ) -> None:
        """创建 Dashboard 受控写命令服务。

        入参：
            queue：持久化后台任务的队列。
            query：读取实验状态与产物的查询组件。
            registry：更新实验注册状态的持久化组件。
            catalog：提供当前 Canonical 数据身份和质量门禁的只读目录。
            source_root：用于解析规则、锁文件和实验源码身份的源码根目录。
            factor_studies：持久化独立因子研究的仓库。
            experiment_submitter：负责解析配置并原子提交实验的客户端。
            experiment_deletion：负责受审计删除非活动实验及其受控资源的端口。
            data_update_planner：负责生成可预览并固化执行的数据更新计划。
        返回值：
            无。
        异常：
            ``source_root``、``factor_studies``、``experiment_submitter`` 或
            ``experiment_deletion`` 或 ``data_update_planner`` 不满足
            构造契约时抛出 ``TypeError``。
        """
        from pathlib import Path

        if not isinstance(source_root, Path):
            raise TypeError("source_root must be a Path")
        self._queue = queue
        self._query = query
        self._registry = registry
        self._catalog = catalog
        self._source_root = source_root
        if factor_studies is None:
            raise TypeError("factor_studies must be supplied")
        self._factor_studies = factor_studies
        if experiment_submitter is None:
            raise TypeError("experiment_submitter must be supplied")
        self._experiment_submitter = experiment_submitter
        if experiment_deletion is None:
            raise TypeError("experiment_deletion must be supplied")
        self._experiment_deletion = experiment_deletion
        if data_update_planner is None:
            raise TypeError("data_update_planner must be supplied")
        self._data_update_planner = data_update_planner

    def submit_experiment(
        self,
        config_yaml: str,
        *,
        request_id: str,
    ) -> dict[str, object]:
        """校验 YAML，并原子创建实验和默认优先级后台任务。

        入参：
            config_yaml：用户提交的实验 YAML 原文；仅从受信配置根或内存文本解析。
            request_id：用于关联一次跨边界调用及其日志的请求标识。
        返回值：
            返回实验（``dict[str, object]``）。
        异常：
            无。
        """
        experiment, task = self._experiment_submitter.create_and_submit_from_yaml_text(
            config_yaml,
            priority=0,
            actor="dashboard",
            request_id=request_id,
        )
        return {
            "experiment_id": experiment.id,
            "task_id": task.id,
            "status": task.status.value,
        }

    def create_factor_study(
        self, name: str, config: FactorStudyConfig
    ) -> dict[str, object]:
        """创建独立因子研究并返回完整研究记录。

        入参：
            name：供用户识别研究、任务或数据对象的非空名称。
            config：已通过严格字段校验并参与身份计算的业务配置。
        返回值：
            返回创建因子因子研究后的因子因子研究（``dict[str, object]``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if config.industry is not None:
            record = self._catalog.get_canonical_dataset(
                DatasetKind.INDUSTRY_CLASSIFICATION
            )
            if (
                record.start_date is None
                or record.end_date is None
                or config.start_date < record.start_date
                or config.end_date > record.end_date
            ):
                raise ValueError(
                    "factor study date range is outside industry coverage"
                )
        study_id = self._factor_studies.create_study(name, config)
        return self._factor_studies.get_study(study_id)

    def enqueue_factor_run(
        self, study_id: str, *, request_id: str
    ) -> dict[str, object]:
        """绑定当前数据和源码身份，创建运行并提交后台任务。

        入参：
            study_id：因子研究定义的 UUID 标识。
            request_id：用于关联一次跨边界调用及其日志的请求标识。
        返回值：
            返回因子运行（``dict[str, object]``）。
        异常：
            无。
        """
        state = self._catalog.require_validated_catalog()
        environment = capture_environment(
            self._source_root, self._source_root / "uv.lock"
        )
        source_hash = cast(str, environment["source_hash"])
        run_id = self._factor_studies.create_run(
            study_id, state.catalog_hash, source_hash
        )
        run = self._factor_studies.get_run(run_id)
        payload: dict[str, JsonValue] = {
            "run_id": run_id,
            "config_hash": cast(str, run["config_hash"]),
        }
        task_id = self._queue.enqueue(
            "FACTOR_ANALYSIS",
            payload,
            0,
            idempotency_key=f"factor-run-{run_id}",
            actor="dashboard",
            request_id=request_id,
        )
        self._factor_studies.bind_task(run_id, task_id)
        return {"run_id": run_id, "task_id": task_id, "status": "QUEUED"}

    def enqueue_data_update(
        self,
        *,
        start: date | None,
        end: date | None,
        datasets: tuple[DatasetKind, ...] | None = None,
        expected_plan_hash: str,
        request_id: str,
    ) -> dict[str, object]:
        """幂等入队数据``update``。

        入参：
            start：限定本次业务操作覆盖范围的开始日期（含边界）。
            end：限定本次业务操作覆盖范围的结束日期（含边界）。
            datasets：可选的非空数据集子集；为空表示全部可执行数据集。
            expected_plan_hash：用户已确认的计划内容身份。
            request_id：用于关联一次跨边界调用及其日志的请求标识。
        返回值：
            返回数据``update``（``dict[str, object]``）。
        异常：
            ``QuantError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        plan = self._data_update_planner.plan(
            start=start,
            end=end,
            datasets=datasets,
        )
        if plan.plan_hash != expected_plan_hash:
            raise QuantError(
                ErrorDetail(
                    code="DATA_UPDATE_PLAN_STALE",
                    severity=Severity.WARNING,
                    message="data update plan changed after preview",
                    context={"current_plan_hash": plan.plan_hash},
                    remediation="refresh the update plan preview and confirm it again",
                    retryable=True,
                )
            )
        if not plan.dataset_windows:
            raise QuantError(
                ErrorDetail(
                    code="DATA_UPDATE_NOT_REQUIRED",
                    severity=Severity.INFO,
                    message="selected datasets do not require an update",
                    context={
                        "skipped_datasets": [
                            item.dataset.value for item in plan.skipped_datasets
                        ]
                    },
                    remediation=(
                        "wait until the financial disclosure deadline has passed"
                    ),
                    retryable=False,
                )
            )
        payload = plan.to_payload()
        key = "dashboard-data-update-" + plan.plan_hash[:24]
        task_id = self._queue.enqueue(
            "DATA_UPDATE",
            payload,
            0,
            idempotency_key=key,
            actor="dashboard",
            request_id=request_id,
        )
        task = self._queue.get(task_id)
        return {
            "task_id": task.id,
            "request_id": request_id,
            "status": task.status.value,
            "plan_hash": plan.plan_hash,
        }

    def enqueue_data_validation(
        self,
        *,
        dataset: DatasetKind | None,
        request_id: str,
    ) -> dict[str, object]:
        """创建全目录或单数据集后台质量运行任务。

        入参：
            dataset：为空时执行 ``validate-all``，否则仅诊断指定数据集。
            request_id：用于关联 Dashboard 请求、任务审计和日志的标识。
        返回值：
            返回任务 ID、状态、质量范围及可选数据集名称。
        异常：
            ``TypeError``：数据集不是 ``DatasetKind`` 或空值时抛出。
        """
        if dataset is not None and not isinstance(dataset, DatasetKind):
            raise TypeError("dataset must be a DatasetKind or None")
        scope = "ALL" if dataset is None else "DATASET"
        payload: dict[str, JsonValue] = {"scope": scope}
        key = "dashboard-data-validation-all"
        if dataset is not None:
            payload["dataset"] = dataset.value
            key = f"dashboard-data-validation-{dataset.value}"
        task_id = self._queue.enqueue(
            "DATA_VALIDATION",
            payload,
            0,
            idempotency_key=key,
            actor="dashboard",
            request_id=request_id,
        )
        task = self._queue.get(task_id)
        result: dict[str, object] = {
            "task_id": task.id,
            "request_id": request_id,
            "status": task.status.value,
            "scope": scope,
        }
        if dataset is not None:
            result["dataset"] = dataset.value
        return result

    def preview_data_update(
        self,
        *,
        start: date | None,
        end: date | None,
        datasets: tuple[DatasetKind, ...] | None = None,
    ) -> dict[str, JsonValue]:
        """生成不会写入任务队列的数据更新计划预览。

        入参：同时为空或同时有值的日期闭区间，以及可选数据集子集。
        返回值：完整计划 payload。
        异常：日期、供应商日历或当前水位无法解析时传播对应异常。
        """
        return self._data_update_planner.plan(
            start=start,
            end=end,
            datasets=datasets,
        ).to_payload()

    def update_research(
        self,
        experiment_id: str,
        *,
        mark: ResearchMark,
        tags: tuple[str, ...],
        note: str,
        request_id: str,
    ) -> dict[str, object]:
        """更新``research``。

        入参：
            experiment_id：持久化实验的 UUID 标识。
            mark：需要写入实验记录的研究标记。
            tags：参与本次处理的标签集合；调用方不得依赖未声明的顺序。
            note：不参与研究身份计算的可选人工备注。
            request_id：用于关联一次跨边界调用及其日志的请求标识。
        返回值：
            返回``research``（``dict[str, object]``）。
        异常：
            无。
        """
        self._registry.update_research(
            experiment_id,
            mark,
            tags,
            note,
            "dashboard",
            request_id=request_id,
        )
        detail = self._query.get(experiment_id)
        return {
            "experiment_id": experiment_id,
            "research_mark": detail.record.research_mark.value,
            "tags": list(detail.tags),
            "note": detail.note,
        }

    def clone_experiment(
        self,
        experiment_id: str,
        *,
        submit: bool,
        priority: int,
        request_id: str,
    ) -> dict[str, object]:
        """处理应用用例中的克隆实验。

        入参：
            experiment_id：持久化实验的 UUID 标识。
            submit：控制是否启用``submit``规则的布尔开关。
            priority：任务在同一可运行集合中的调度优先级。
            request_id：用于关联一次跨边界调用及其日志的请求标识。
        返回值：
            返回实验（``dict[str, object]``）。
        异常：
            无。
        """
        original = self._query.get(experiment_id).record
        state = self._catalog.require_validated_catalog()
        environment = capture_environment(
            self._source_root, self._source_root / "uv.lock"
        )
        source_hash = cast(str, environment["source_hash"])
        lockfile_hash = cast(str, environment["lockfile_hash"])
        fingerprint = compute_fingerprint(
            ExperimentFingerprintInput(
                strategy_id=original.strategy_id,
                resolved_config=original.config,
                data_hash=state.catalog_hash,
                source_hash=source_hash,
                lockfile_hash=lockfile_hash,
                rulebook_hash=original.rulebook_hash,
            )
        )
        spec = ExperimentSpec(
            strategy_id=original.strategy_id,
            config=dict(original.config),
            config_hash=hashlib.sha256(
                canonical_json_bytes(original.config)
            ).hexdigest(),
            data_hash=state.catalog_hash,
            source_tree_hash=cast(str | None, environment["source_tree_hash"]),
            git_commit_hash=cast(str | None, environment["git_commit"]),
            lockfile_hash=lockfile_hash,
            rulebook_hash=original.rulebook_hash,
            fingerprint=fingerprint,
            created_at=datetime.now(UTC),
        )
        new_id = self._registry.create(
            spec,
            spec.fingerprint,
            actor="dashboard",
            request_id=request_id,
        )
        task_id: str | None = None
        if submit:
            task_id = self._queue.submit_backtest(
                new_id,
                spec.config_hash,
                priority=priority,
                actor="dashboard",
                request_id=request_id,
            )
        return {"experiment_id": new_id, "task_id": task_id}

    def cancel_task(self, task_id: str, *, request_id: str) -> dict[str, object]:
        """请求取消任务。

        入参：
            task_id：持久化任务的 UUID 标识。
            request_id：用于关联一次跨边界调用及其日志的请求标识。
        返回值：
            返回任务（``dict[str, object]``）。
        异常：
            无。
        """
        self._queue.request_cancel(
            task_id,
            "dashboard",
            request_id=request_id,
            strict=True,
        )
        task = self._queue.get(task_id)
        return {"task_id": task.id, "status": task.status.value}

    def retry_task(
        self,
        task_id: str,
        *,
        confirm_orphaned: bool,
        request_id: str,
    ) -> dict[str, object]:
        """从终态任务创建全新重试任务。

        入参：
            task_id：持久化任务的 UUID 标识。
            返回任务（``dict[str, object]``）。
            request_id：用于关联一次跨边界调用及其日志的请求标识。
        返回值：
            返回任务（``dict[str, object]``）。
        异常：
            ``QuantError``、``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        task = self._queue.get(task_id)
        if task.status is TaskStatus.ORPHANED and not confirm_orphaned:
            raise ValueError("orphaned task retry requires explicit confirmation")
        if task.task_type == "DATA_UPDATE":
            try:
                DataUpdatePlan.from_payload(task.payload)
            except (TypeError, ValueError) as error:
                raise QuantError(
                    ErrorDetail(
                        code="DATA_UPDATE_LEGACY_PLAN",
                        severity=Severity.WARNING,
                        message="legacy data update task has no frozen dataset windows",
                        context={"task_id": task.id},
                        remediation="create a new update task from the data center",
                        retryable=False,
                    )
                ) from error
        if task.task_type == "BACKTEST":
            experiment_id, new_task_id = self._queue.clone_for_retry(
                task_id,
                actor="dashboard",
                request_id=request_id,
            )
            return {"task_id": new_task_id, "experiment_id": experiment_id}
        if task.task_type == "FACTOR_ANALYSIS":
            run = self._factor_studies.get_run_by_task(task_id)
            return self.enqueue_factor_run(
                cast(str, run["study_id"]), request_id=request_id
            )
        retried = self._queue.retry(
            task_id,
            "dashboard",
            request_id=request_id,
        )
        return {"task_id": retried, "experiment_id": task.experiment_id}

    def delete_task(self, task_id: str, *, request_id: str) -> dict[str, object]:
        """从运行中心删除一个终态任务记录。

        入参：
            task_id：目标任务标识。
            request_id：Dashboard 写请求的关联标识。
        返回值：
            返回已删除任务标识与固定 ``DELETED`` 状态。
        异常：
            任务不存在或尚未进入终态时传播任务队列的结构化异常。
        """
        self._queue.delete(
            task_id,
            "dashboard",
            request_id=request_id,
        )
        return {"task_id": task_id, "status": "DELETED"}

    def delete_experiment(
        self, experiment_id: str, *, request_id: str
    ) -> dict[str, object]:
        """删除一个非活动实验及其数据库、产物和日志资源。

        入参：
            experiment_id：目标实验标识。
            request_id：Dashboard 写请求的关联标识。
        返回值：
            返回已删除实验标识与固定 ``DELETED`` 状态。
        异常：
            实验不存在、仍在活动状态或受控文件清理失败时传播结构化异常。
        """
        self._experiment_deletion.delete(
            experiment_id,
            "dashboard",
            request_id=request_id,
        )
        return {"experiment_id": experiment_id, "status": "DELETED"}
