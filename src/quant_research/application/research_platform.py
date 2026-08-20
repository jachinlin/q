"""提供研究族提交、异步推进和自动选型应用用例。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, cast

import yaml

from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.experiments.research import (
    FamilyExecutionRecord,
    ResearchFamilyRecord,
    ResearchMetricRecord,
    ResearchPhase,
    ResearchRunRecord,
    ResearchStage,
    ResearchStatus,
    ResearchVariantRecord,
)
from quant_research.research_protocols import (
    CandidateEvaluation,
    CandidateSelection,
    ExpandedVariant,
    ResearchConfigResolver,
    ResearchFamilyConfig,
    ResearchSelector,
    ResolvedResearchFamily,
)
from quant_research.strategies.definitions import ComponentRegistry, StrategyTemplate
from quant_research.tasks.handlers import CancellationToken, ProgressSink
from quant_research.tasks.models import (
    ClaimedTask,
    TaskOutcome,
    TaskProgress,
    TaskStatus,
)


@dataclass(frozen=True, slots=True)
class ResearchExecutionIdentity:
    """绑定研究提交时的数据、源码、依赖、规则和环境身份。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    catalog_hash: str
    source_hash: str
    lockfile_hash: str
    rulebook_hash: str
    environment_hash: str


@dataclass(frozen=True, slots=True)
class ResearchRunResult:
    """表示运行时发布并验证成功的 Manifest 和指标。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    manifest_path: str
    manifest_hash: str
    metrics: tuple[ResearchMetricRecord, ...]
    stage_status: Mapping[str, JsonValue]


class ResearchIdentityProvider(Protocol):
    """捕获提交时所有研究身份。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def capture(self) -> ResearchExecutionIdentity:
        """定义 capture 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...



class ResearchRuntime(Protocol):
    """使用现有 Canonical Repository 执行一个研究运行。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def execute(
        self,
        family: ResearchFamilyRecord,
        execution: FamilyExecutionRecord,
        variant: ResearchVariantRecord,
        run: ResearchRunRecord,
        progress: ProgressSink,
        cancellation: CancellationToken,
    ) -> ResearchRunResult:
        """定义 execute 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...



class ResearchSelectionPublisher(Protocol):
    """发布并复核执行级不可变选型证据。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def publish_selection(
        self,
        *,
        family_id: str,
        execution_id: str,
        document: Mapping[str, JsonValue],
    ) -> tuple[Path, str, int]:
        """定义 publish_selection 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...



class ResearchRegistryPort(Protocol):
    """定义研究应用用例所需的持久化操作集合。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def create_family(
        self,
        resolved: ResolvedResearchFamily,
        *,
        catalog_hash: str,
        source_hash: str,
        lockfile_hash: str,
        rulebook_hash: str,
        environment_hash: str,
    ) -> tuple[ResearchFamilyRecord, FamilyExecutionRecord]:
        """定义 create_family 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def create_execution(
        self,
        family_id: str,
        *,
        catalog_hash: str,
        source_hash: str,
        lockfile_hash: str,
        rulebook_hash: str,
        environment_hash: str,
    ) -> FamilyExecutionRecord:
        """定义 create_execution 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def expand(
        self, execution_id: str, variants: Sequence[ExpandedVariant]
    ) -> tuple[ResearchRunRecord, ...]:
        """定义 expand 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def create_test_run(
        self, execution_id: str, variant_id: str, reason: str
    ) -> ResearchRunRecord:
        """定义 create_test_run 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def update_run(
        self,
        run_id: str,
        *,
        status: ResearchStatus,
        stage: ResearchStage,
        stage_status: Mapping[str, JsonValue],
        manifest_path: str | None = None,
        manifest_hash: str | None = None,
        error: Mapping[str, JsonValue] | None = None,
    ) -> ResearchRunRecord:
        """定义 update_run 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def register_metrics(
        self, run_id: str, metrics: Sequence[ResearchMetricRecord]
    ) -> None:
        """定义 register_metrics 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def register_run_artifacts(
        self, run_id: str, *, manifest_path: str, manifest_hash: str
    ) -> None:
        """定义 register_run_artifacts 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def complete_execution(
        self,
        execution_id: str,
        status: ResearchStatus,
        error: Mapping[str, JsonValue] | None = None,
    ) -> FamilyExecutionRecord:
        """定义 complete_execution 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def record_selection_evidence(
        self,
        execution_id: str,
        selection: CandidateSelection,
        *,
        artifact_hash: str,
        artifact_bytes: int,
    ) -> None:
        """定义 record_selection_evidence 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def get_family(self, family_id: str) -> ResearchFamilyRecord:
        """定义 get_family 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def get_execution(self, execution_id: str) -> FamilyExecutionRecord:
        """定义 get_execution 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def list_variants(
        self, execution_id: str
    ) -> tuple[ResearchVariantRecord, ...]:
        """定义 list_variants 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def get_run(self, run_id: str) -> ResearchRunRecord:
        """定义 get_run 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def list_runs(self, execution_id: str) -> tuple[ResearchRunRecord, ...]:
        """定义 list_runs 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def list_metrics(self, run_id: str) -> tuple[ResearchMetricRecord, ...]:
        """定义 list_metrics 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...



class ResearchTaskQueuePort(Protocol):
    """定义研究工作流所需的通用任务入队操作。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def enqueue(
        self,
        task_type: str,
        payload: Mapping[str, JsonValue],
        *,
        subject_kind: str,
        subject_id: str,
        priority: int = 0,
        idempotency_key: str,
        actor: str = "system",
        request_id: str | None = None,
    ) -> str:
        """定义 enqueue 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...



class ResearchCommandService:
    """校验研究 YAML，创建研究族并提交非阻塞任务链。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def __init__(
        self,
        *,
        resolver: ResearchConfigResolver,
        components: ComponentRegistry,
        registry: ResearchRegistryPort,
        queue: ResearchTaskQueuePort,
        identities: ResearchIdentityProvider,
    ) -> None:
        self._resolver = resolver
        self._components = components
        self._registry = registry
        self._queue = queue
        self._identities = identities

    def validate_yaml(self, config_yaml: str) -> dict[str, JsonValue]:
        """返回规范配置、候选预览和必需数据集。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        resolved = self._resolver.resolve_yaml(config_yaml)
        template = self._validate_compositions(resolved)
        required = sorted(
            {
                dataset
                for component_id in template.components
                for dataset in self._components.descriptor(component_id).required_datasets
            }
        )
        return {
            "config_hash": resolved.config_hash,
            "normalized_yaml": yaml.safe_dump(
                resolved.normalized,
                allow_unicode=True,
                sort_keys=False,
            ),
            "variant_count": len(resolved.variants),
            "variants": [
                {
                    "variant_id": item.variant_id,
                    "composition_hash": item.composition_hash,
                    "parameters": dict(item.parameters),
                }
                for item in resolved.variants[:20]
            ],
            "required_datasets": cast(list[JsonValue], required),
            "signal_kind": template.signal_kind.value,
        }

    def submit(
        self, config_yaml: str, *, request_id: str, actor: str = "dashboard"
    ) -> dict[str, JsonValue]:
        """创建研究族和首次执行，并入队候选展开任务。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        resolved = self._resolver.resolve_yaml(config_yaml)
        self._validate_compositions(resolved)
        identity = self._identities.capture()
        family, execution = self._registry.create_family(
            resolved,
            catalog_hash=identity.catalog_hash,
            source_hash=identity.source_hash,
            lockfile_hash=identity.lockfile_hash,
            rulebook_hash=identity.rulebook_hash,
            environment_hash=identity.environment_hash,
        )
        task_id = self._enqueue_expand(execution.id, actor=actor, request_id=request_id)
        return {
            "family_id": family.id,
            "execution_id": execution.id,
            "task_id": task_id,
            "status": ResearchStatus.QUEUED.value,
        }

    def rerun(
        self, family_id: str, *, request_id: str, actor: str = "dashboard"
    ) -> dict[str, JsonValue]:
        """捕获当前身份并为同一不可变研究族创建新执行。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        identity = self._identities.capture()
        execution = self._registry.create_execution(
            family_id,
            catalog_hash=identity.catalog_hash,
            source_hash=identity.source_hash,
            lockfile_hash=identity.lockfile_hash,
            rulebook_hash=identity.rulebook_hash,
            environment_hash=identity.environment_hash,
        )
        task_id = self._enqueue_expand(execution.id, actor=actor, request_id=request_id)
        return {
            "family_id": family_id,
            "execution_id": execution.id,
            "task_id": task_id,
            "status": ResearchStatus.QUEUED.value,
        }

    def rerun_subject(
        self,
        subject_kind: str,
        subject_id: str,
        *,
        request_id: str,
    ) -> dict[str, JsonValue]:
        """把任一研究关联对象解析为研究族并创建新 execution。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        if subject_kind == "RESEARCH_FAMILY":
            family_id = subject_id
        elif subject_kind == "RESEARCH_EXECUTION":
            family_id = self._registry.get_execution(subject_id).family_id
        elif subject_kind == "RESEARCH_RUN":
            run = self._registry.get_run(subject_id)
            family_id = self._registry.get_execution(run.execution_id).family_id
        else:
            raise ValueError(f"unsupported research task subject: {subject_kind}")
        return self.rerun(family_id, request_id=request_id)

    def _enqueue_expand(self, execution_id: str, *, actor: str, request_id: str) -> str:
        return self._queue.enqueue(
            "RESEARCH_EXPAND",
            {"execution_id": execution_id},
            subject_kind="RESEARCH_EXECUTION",
            subject_id=execution_id,
            idempotency_key=f"research-expand:{execution_id}",
            actor=actor,
            request_id=request_id,
        )

    def _validate_compositions(
        self, resolved: ResolvedResearchFamily
    ) -> StrategyTemplate:
        """校验族定义和每个展开候选的完整组件组装。"""
        template = self._components.validate(resolved.config)
        for variant in resolved.variants:
            config = ResearchFamilyConfig.model_validate_json(
                canonical_json_bytes(variant.config)
            )
            self._components.validate(config)
        return template


class ResearchExpandHandler:
    """展开候选并为每个候选入队开发区间运行。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    task_type = "RESEARCH_EXPAND"

    def __init__(
        self, registry: ResearchRegistryPort, queue: ResearchTaskQueuePort
    ) -> None:
        self._registry = registry
        self._queue = queue
        self._resolver = ResearchConfigResolver()

    def run(
        self, task: ClaimedTask, progress: ProgressSink, cancellation: CancellationToken
    ) -> TaskOutcome:
        """幂等展开并提交 TRAIN_VALIDATION 运行。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        execution_id = _WorkflowSupport.payload_id(task, "execution_id")
        execution = self._registry.get_execution(execution_id)
        family = self._registry.get_family(execution.family_id)
        resolved = self._resolver.resolve_normalized(family.config)
        progress.update(TaskProgress(stage="EXPAND", completed=0, total=len(resolved.variants), message="展开候选"))
        if cancellation.is_cancelled():
            self._registry.complete_execution(execution_id, ResearchStatus.CANCELLED)
            return TaskOutcome(status=TaskStatus.CANCELLED)
        runs = self._registry.expand(execution_id, resolved.variants)
        for index, run in enumerate(runs, start=1):
            self._queue.enqueue(
                "RESEARCH_RUN",
                {"run_id": run.id},
                subject_kind="RESEARCH_RUN",
                subject_id=run.id,
                idempotency_key=f"research-run:{run.id}",
            )
            progress.update(TaskProgress(stage="EXPAND", completed=index, total=len(runs), message=f"已提交候选 {index}/{len(runs)}"))
        return TaskOutcome(status=TaskStatus.SUCCEEDED, result={"execution_id": execution_id, "run_count": len(runs)})


class ResearchRunHandler:
    """执行单个候选并在全部开发运行完成后推进选型。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    task_type = "RESEARCH_RUN"

    def __init__(
        self,
        registry: ResearchRegistryPort,
        queue: ResearchTaskQueuePort,
        runtime: ResearchRuntime,
    ) -> None:
        self._registry = registry
        self._queue = queue
        self._runtime = runtime

    def run(
        self, task: ClaimedTask, progress: ProgressSink, cancellation: CancellationToken
    ) -> TaskOutcome:
        """运行固定七阶段，并根据 phase 推进 SELECT 或 REGISTER。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        run_id = _WorkflowSupport.payload_id(task, "run_id")
        run = self._registry.get_run(run_id)
        execution = self._registry.get_execution(run.execution_id)
        family = self._registry.get_family(execution.family_id)
        variant = next(item for item in self._registry.list_variants(execution.id) if item.id == run.variant_id)
        if cancellation.is_cancelled():
            self._registry.update_run(run_id, status=ResearchStatus.CANCELLED, stage=run.stage, stage_status={"cancelled": True})
            return TaskOutcome(status=TaskStatus.CANCELLED)
        try:
            self._registry.update_run(run_id, status=ResearchStatus.RUNNING, stage=ResearchStage.VALIDATE, stage_status={})
            result = self._runtime.execute(family, execution, variant, run, progress, cancellation)
            self._registry.register_metrics(run_id, result.metrics)
            self._registry.register_run_artifacts(
                run_id,
                manifest_path=result.manifest_path,
                manifest_hash=result.manifest_hash,
            )
            completed = self._registry.update_run(
                run_id,
                status=ResearchStatus.SUCCEEDED,
                stage=ResearchStage.REGISTER,
                stage_status=result.stage_status,
                manifest_path=result.manifest_path,
                manifest_hash=result.manifest_hash,
            )
        except Exception as error:
            self._registry.update_run(run_id, status=ResearchStatus.FAILED, stage=run.stage, stage_status={}, error={"code": "RESEARCH_RUN_FAILED", "message": str(error)[:2000]})
            self._registry.complete_execution(execution.id, ResearchStatus.FAILED, {"code": "RESEARCH_RUN_FAILED", "run_id": run_id})
            raise
        if completed.phase is ResearchPhase.TEST:
            self._queue.enqueue("RESEARCH_REGISTER", {"execution_id": execution.id}, subject_kind="RESEARCH_EXECUTION", subject_id=execution.id, idempotency_key=f"research-register:{execution.id}")
        else:
            development = tuple(item for item in self._registry.list_runs(execution.id) if item.phase is ResearchPhase.TRAIN_VALIDATION)
            if development and all(item.status is ResearchStatus.SUCCEEDED for item in development):
                self._queue.enqueue("RESEARCH_SELECT", {"execution_id": execution.id}, subject_kind="RESEARCH_EXECUTION", subject_id=execution.id, idempotency_key=f"research-select:{execution.id}")
        return TaskOutcome(status=TaskStatus.SUCCEEDED, result={"run_id": run_id})


class ResearchSelectHandler:
    """只读取 VALIDATION 指标，锁定候选并自动提交 TEST。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    task_type = "RESEARCH_SELECT"

    def __init__(
        self,
        registry: ResearchRegistryPort,
        queue: ResearchTaskQueuePort,
        publisher: ResearchSelectionPublisher,
    ) -> None:
        self._registry = registry
        self._queue = queue
        self._publisher = publisher
        self._selector = ResearchSelector()
        self._resolver = ResearchConfigResolver()

    def run(
        self, task: ClaimedTask, progress: ProgressSink, cancellation: CancellationToken
    ) -> TaskOutcome:
        """生成不可变选择理由，然后创建唯一 TEST 运行。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        execution_id = _WorkflowSupport.payload_id(task, "execution_id")
        execution = self._registry.get_execution(execution_id)
        family = self._registry.get_family(execution.family_id)
        policy = self._resolver.resolve_normalized(family.config).config.research_protocol.selection
        runs = tuple(item for item in self._registry.list_runs(execution_id) if item.phase is ResearchPhase.TRAIN_VALIDATION)
        variants = {item.id: item for item in self._registry.list_variants(execution_id)}
        evaluations: list[CandidateEvaluation] = []
        for run in runs:
            metrics = [item for item in self._registry.list_metrics(run.id) if item.split == "VALIDATION"]
            metric_map = {item.name: item.value for item in metrics}
            primary = next((item for item in metrics if item.name == policy.primary_metric), None)
            evaluations.append(CandidateEvaluation(run.variant_id, metric_map, primary.p_value if primary else None))
        progress.update(TaskProgress(stage="SELECT", completed=0, total=1, message="仅使用验证集选择候选"))
        if cancellation.is_cancelled():
            self._registry.complete_execution(execution_id, ResearchStatus.CANCELLED)
            return TaskOutcome(status=TaskStatus.CANCELLED)
        try:
            selection = self._selector.select(tuple(evaluations), policy)
        except ValueError as error:
            self._registry.complete_execution(
                execution_id,
                ResearchStatus.FAILED,
                {"code": "NO_ELIGIBLE_CANDIDATE", "message": str(error)},
            )
            raise
        selected = variants[selection.selected_variant_id]
        selection_document: dict[str, JsonValue] = {
            "execution_id": execution_id,
            "selected_variant_id": selected.id,
            "composition_hash": selected.composition_hash,
            "reason": selection.reason,
            "source_split": "VALIDATION",
            "adjusted_p_values": dict(selection.adjusted_p_values),
            "rejected": {
                variant_id: list(reasons)
                for variant_id, reasons in sorted(selection.rejected.items())
            },
        }
        _, artifact_hash, artifact_bytes = self._publisher.publish_selection(
            family_id=family.id,
            execution_id=execution_id,
            document=selection_document,
        )
        self._registry.record_selection_evidence(
            execution_id,
            selection,
            artifact_hash=artifact_hash,
            artifact_bytes=artifact_bytes,
        )
        test_run = self._registry.create_test_run(execution_id, selected.id, selection.reason)
        self._queue.enqueue("RESEARCH_RUN", {"run_id": test_run.id}, subject_kind="RESEARCH_RUN", subject_id=test_run.id, idempotency_key=f"research-run:{test_run.id}")
        progress.update(TaskProgress(stage="SELECT", completed=1, total=1, message="已锁定候选并提交 TEST"))
        return TaskOutcome(status=TaskStatus.SUCCEEDED, result={"selected_variant_id": selected.id, "test_run_id": test_run.id})


class ResearchRegisterHandler:
    """确认唯一 TEST 成功后登记研究族执行成功。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    task_type = "RESEARCH_REGISTER"

    def __init__(self, registry: ResearchRegistryPort) -> None:
        self._registry = registry

    def run(
        self, task: ClaimedTask, progress: ProgressSink, cancellation: CancellationToken
    ) -> TaskOutcome:
        """检查锁定 TEST 运行和 Manifest 后关闭执行。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        execution_id = _WorkflowSupport.payload_id(task, "execution_id")
        tests = tuple(item for item in self._registry.list_runs(execution_id) if item.phase is ResearchPhase.TEST)
        if len(tests) != 1 or tests[0].status is not ResearchStatus.SUCCEEDED or not tests[0].manifest_hash:
            raise ValueError("research execution requires exactly one verified successful TEST run")
        progress.update(TaskProgress(stage="REGISTER", completed=1, total=1, message="研究执行已登记"))
        self._registry.complete_execution(execution_id, ResearchStatus.SUCCEEDED)
        return TaskOutcome(status=TaskStatus.SUCCEEDED, result={"execution_id": execution_id})


class _WorkflowSupport:
    @staticmethod
    def payload_id(task: ClaimedTask, field: str) -> str:
        value = task.payload.get(field)
        if not isinstance(value, str) or not value:
            raise ValueError(f"research task payload requires {field}")
        return value
