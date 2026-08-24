"""提供策略实验创建、派生 Run、重跑、标记和查询用例。"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from quant_research.data.contracts import JsonValue
from quant_research.experiments.config import (
    ExperimentConfigParser,
    ResolvedExperiment,
    ResolvedRun,
)
from quant_research.experiments.models import (
    ExperimentAggregate,
    ExperimentDefinition,
    ExperimentRecord,
    ResearchMark,
    RunRecord,
    StrategyBacktestRunConfig,
)


class CatalogIdentity(Protocol):
    """提供通过全量质量门禁的当前 Canonical 目录身份。

    入参：
        实现方应暴露稳定的 ``catalog_hash`` 属性。
    返回值：
        Protocol 仅约束目录身份读取能力，不创建实例结果。
    异常：
        实现方可在门禁未开放时抛出数据目录异常。
    """

    @property
    def catalog_hash(self) -> str:
        """读取当前目录哈希。

        入参：
            无。
        返回值：
            全部当前 Canonical 数据集组成的 SHA-256 身份。
        异常：
            门禁未开放时由实现方抛出数据目录异常。
        """
        ...


class ValidatedCatalog(Protocol):
    """提供经过全量质量门禁校验的当前 Canonical 目录身份。

    入参：
        实现方负责读取当前目录状态并执行全局质量门禁。
    返回值：
        Protocol 仅约束实验提交所需的目录门禁能力。
    异常：
        门禁未开放或目录状态损坏时由实现方抛出对应数据错误。
    """

    def require_validated_catalog(self) -> CatalogIdentity:
        """读取当前已验证的 Canonical 目录身份。

        入参：
            无。
        返回值：
            包含 SHA-256 ``catalog_hash`` 的不可变目录状态。
        异常：
            当前目录未通过 ``validate-all`` 时抛出数据门禁错误。
        """
        ...


class ExperimentRegistry(Protocol):
    """定义统一实验应用服务所需的持久化端口。

    入参：
        实现方负责 Experiment、Run、任务和研究标记的原子持久化。
    返回值：
        Protocol 描述应用层可消费的记录与标识。
    异常：
        冲突、不存在或非法状态由实现方以领域错误报告。
    """

    def create(
        self, definition: ExperimentDefinition, catalog_hash: str, *, actor: str
    ) -> tuple[str, str, str]:
        """原子创建实验、首个 Run 和任务。

        入参：实验定义、提交时目录哈希和审计主体。
        返回值：实验 ID、Run ID 与任务 ID。
        异常：定义冲突或持久化失败时抛出对应异常。
        """
        ...

    def add_run(
        self,
        experiment_id: str,
        config: StrategyBacktestRunConfig,
        catalog_hash: str,
        *,
        actor: str,
    ) -> tuple[str, str]:
        """为实验追加显式 Run。

        入参：实验 ID、冻结配置、目录哈希与审计主体。
        返回值：新 Run ID 与任务 ID。
        异常：实验不存在或配置不符合协议时抛出对应异常。
        """
        ...

    def rerun(self, run_id: str, catalog_hash: str, *, actor: str) -> tuple[str, str]:
        """复制历史冻结配置并创建新 Run。

        入参：原 Run ID、当前目录哈希与审计主体。
        返回值：新 Run ID 与任务 ID。
        异常：原 Run 不存在或不可重跑时抛出对应异常。
        """
        ...

    def get_experiment(self, experiment_id: str) -> ExperimentAggregate:
        """读取实验聚合。

        入参：实验 ID。
        返回值：实验定义及其全部 Run。
        异常：实验不存在时抛出对应异常。
        """
        ...

    def get_run(self, run_id: str) -> RunRecord:
        """读取 Run 快照。

        入参：Run ID。
        返回值：冻结的 Run 记录。
        异常：Run 不存在时抛出对应异常。
        """
        ...

    def list_experiments(
        self, *, limit: int, offset: int
    ) -> tuple[ExperimentRecord, ...]:
        """稳定分页读取实验摘要。

        入参：页大小和偏移量。
        返回值：按创建时间倒序的实验记录元组。
        异常：分页参数非法时抛出值错误。
        """
        ...

    def mark(self, run_id: str, mark: ResearchMark, *, actor: str) -> None:
        """审计修改 Run 的研究标记。

        入参：Run ID、新标记和审计主体。
        返回值：无。
        异常：Run 不存在或 baseline 冲突时抛出对应异常。
        """
        ...

    def delete_run(self, run_id: str, *, actor: str) -> None:
        """删除一个终态 Run 的聚合记录。

        入参：Run ID 和审计主体。
        返回值：删除完成后无返回。
        异常：Run 不存在或仍处于活动状态时抛出对应异常。
        """
        ...

    def delete_experiment(self, experiment_id: str, *, actor: str) -> None:
        """删除一个不存在活动 Run 的实验聚合。

        入参：实验 ID 和审计主体。
        返回值：实验及其全部终态 Run 删除完成后无返回。
        异常：实验不存在或包含活动 Run 时抛出对应异常。
        """
        ...


class StrategyValidator(Protocol):
    """定义实验提交前校验策略标识和参数的消费者侧端口。

    入参：
        实现方接收策略标识和 JSON 参数映射。
    返回值：
        参数可构造策略时不返回业务数据。
    异常：
        ValueError、TypeError：策略未知、字段多余或参数违反策略约束时抛出。
    """

    def validate(self, strategy_id: str, params: Mapping[str, JsonValue]) -> None:
        """校验一个策略配置能够由注册工厂确定性构造。

        入参：
            strategy_id：策略标识；params：冻结 JSON 参数。
        返回值：
            配置有效时返回 None。
        异常：
            ValueError、TypeError：标识或参数不满足策略契约时抛出。
        """
        ...


class ExperimentService:
    """协调严格配置解析、数据门禁和 Experiment/Run 事务。

    入参：
        构造时接收持久化端口、Canonical 门禁和可选配置解析器。
    返回值：
        方法返回规范配置、实验聚合或 Run 记录。
    异常：
        YAML、目录门禁或持久化失败按边界语义抛出。
    """

    def __init__(
        self,
        registry: ExperimentRegistry,
        catalog: ValidatedCatalog,
        strategies: StrategyValidator,
        parser: ExperimentConfigParser | None = None,
    ) -> None:
        self._registry = registry
        self._catalog = catalog
        self._strategies = strategies
        self._parser = parser or ExperimentConfigParser()

    def validate_experiment(self, yaml_text: str) -> ResolvedExperiment:
        """只解析和校验实验 YAML，不写数据库。

        入参：实验 YAML 文本。
        返回值：规范化实验及配置哈希。
        异常：Schema 或领域约束不满足时抛出值错误。
        """
        resolved = self._parser.parse_experiment(yaml_text)
        self._validate_strategy(resolved.definition.initial_run)
        return resolved

    def submit(self, yaml_text: str, *, actor: str = "user") -> ExperimentAggregate:
        """创建实验、首个 Run 并立即入队。

        入参：实验 YAML 文本和审计主体。
        返回值：包含首个 Run 的实验聚合。
        异常：配置非法、数据门禁关闭或事务失败时抛出对应异常。
        """
        resolved = self.validate_experiment(yaml_text)
        catalog_hash = self._catalog_hash()
        experiment_id, _, _ = self._registry.create(
            resolved.definition, catalog_hash, actor=actor
        )
        return self._registry.get_experiment(experiment_id)

    def add_run(
        self, experiment_id: str, yaml_text: str, *, actor: str = "user"
    ) -> ExperimentAggregate:
        """在既有实验下创建一个显式参数 Run。

        入参：实验 ID、Run YAML 文本和审计主体。
        返回值：追加 Run 后的实验聚合。
        异常：配置越界、kind 不一致或实验不存在时抛出对应异常。
        """
        resolved: ResolvedRun = self._parser.parse_run(yaml_text)
        self._validate_strategy(resolved.config)
        self._registry.add_run(
            experiment_id, resolved.config, self._catalog_hash(), actor=actor
        )
        return self._registry.get_experiment(experiment_id)

    def rerun(self, run_id: str, *, actor: str = "user") -> ExperimentAggregate:
        """复制冻结配置创建新 Run，且不覆盖旧产物。

        入参：历史 Run ID 和审计主体。
        返回值：包含新 Run 的实验聚合。
        异常：原 Run 不存在或目录门禁关闭时抛出对应异常。
        """
        run_id, _ = self._registry.rerun(run_id, self._catalog_hash(), actor=actor)
        return self._registry.get_experiment(
            self._registry.get_run(run_id).experiment_id
        )

    def mark(
        self, run_id: str, mark: ResearchMark, *, actor: str = "user"
    ) -> ExperimentAggregate:
        """修改研究标记并返回所属实验最新状态。

        入参：Run ID、目标研究标记和审计主体。
        返回值：更新后的实验聚合。
        异常：Run 不存在或 baseline 更新冲突时抛出对应异常。
        """
        self._registry.mark(run_id, mark, actor=actor)
        return self._registry.get_experiment(
            self._registry.get_run(run_id).experiment_id
        )

    def list(
        self, *, limit: int = 100, offset: int = 0
    ) -> tuple[ExperimentRecord, ...]:
        """分页列出实验摘要。

        入参：页大小和零基偏移量。
        返回值：稳定排序的实验记录元组。
        异常：分页参数非法时抛出值错误。
        """
        return self._registry.list_experiments(limit=limit, offset=offset)

    def show(self, experiment_id: str) -> ExperimentAggregate:
        """返回实验定义和全部 Run。

        入参：实验 ID。
        返回值：实验聚合。
        异常：实验不存在时抛出对应异常。
        """
        return self._registry.get_experiment(experiment_id)

    def get_run(self, run_id: str) -> RunRecord:
        """按 ID 返回一个 Run 快照。

        入参：Run ID。
        返回值：冻结的 Run 记录。
        异常：Run 不存在时抛出对应异常。
        """
        return self._registry.get_run(run_id)

    def delete_run(self, run_id: str, *, actor: str = "user") -> None:
        """删除一个终态 Run，保留独立任务和审计历史。

        入参：Run ID 和审计主体。
        返回值：删除完成后无返回。
        异常：Run 不存在或仍处于活动状态时传播持久化边界异常。
        """
        self._registry.delete_run(run_id, actor=actor)

    def delete_experiment(
        self, experiment_id: str, *, actor: str = "user"
    ) -> None:
        """删除一个不存在活动 Run 的实验及其全部 Run。

        入参：实验 ID 和审计主体。
        返回值：删除完成后无返回。
        异常：实验不存在或任一 Run 仍活动时传播持久化边界异常。
        """
        self._registry.delete_experiment(experiment_id, actor=actor)

    def _catalog_hash(self) -> str:
        value = self._catalog.require_validated_catalog().catalog_hash
        if len(value) != 64:
            raise ValueError("validated catalog_hash must be a SHA-256 digest")
        return value

    def _validate_strategy(self, config: StrategyBacktestRunConfig) -> None:
        strategy = config.strategy
        self._strategies.validate(
            strategy.strategy_id,
            strategy.parameters,
        )


__all__ = ["ExperimentService"]
