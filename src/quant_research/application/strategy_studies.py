"""提供单一策略研究的校验、提交、查询和删除用例。"""

from __future__ import annotations

from typing import Protocol

from quant_research.data.contracts import JsonValue
from quant_research.strategy_studies.config import (
    ResolvedStrategyStudy,
    StrategyStudyConfigParser,
)
from quant_research.strategy_studies.models import (
    StrategyStudyArtifactRecord,
    StrategyStudyDefinition,
    StrategyStudyMetricRecord,
    StrategyStudyRecord,
    StrategyStudyStage,
    StrategyStudyStatus,
)


class CatalogIdentity(Protocol):
    """暴露目录身份。入参：实现依赖。返回值：身份端口。异常：实现保留依赖异常。"""

    @property
    def catalog_hash(self) -> str:
        """读取身份。入参：无。返回值：目录哈希。异常：读取失败时传播。"""
        ...


class ValidatedCatalog(Protocol):
    """提供已验证目录。入参：实现依赖。返回值：门禁端口。异常：实现保留依赖异常。"""

    def require_validated_catalog(self) -> CatalogIdentity:
        """读取目录。入参：无。返回值：目录身份。异常：门禁关闭时抛出领域错误。"""
        ...


class StrategyValidator(Protocol):
    """校验策略配置。入参：实现依赖。返回值：校验端口。异常：实现保留校验异常。"""

    def validate(self, strategy_id: str, params: dict[str, JsonValue]) -> None:
        """校验参数。入参：策略 ID 和参数。返回值：无。异常：无法构造时抛出值错误。"""
        ...


class StrategyStudyRegistry(Protocol):
    """定义持久化端口。入参：实现依赖。返回值：登记端口。异常：实现保留持久化异常。"""

    def create(
        self,
        definition: StrategyStudyDefinition,
        config_hash: str,
        catalog_hash: str,
        *,
        actor: str,
    ) -> tuple[str, str]:
        """原子创建研究和任务。入参：定义、身份和操作者。返回值：研究与任务 ID。异常：事务失败时传播。"""
        ...

    def get(self, study_id: str) -> StrategyStudyRecord:
        """读取研究。入参：研究 ID。返回值：完整快照。异常：不存在时抛出键错误。"""
        ...

    def list(
        self,
        *,
        limit: int,
        offset: int,
        status: StrategyStudyStatus | None = None,
    ) -> tuple[StrategyStudyRecord, ...]:
        """分页列出研究。入参：分页和状态。返回值：快照元组。异常：分页非法时抛出值错误。"""
        ...

    def delete(self, study_id: str, *, actor: str) -> None:
        """删除研究。入参：研究 ID 和操作者。返回值：无。异常：活动研究或持久化失败时传播。"""
        ...


class StrategyStudyService:
    """协调研究用例。入参：登记簿、目录门禁、策略目录和解析器。返回值：服务实例。异常：依赖非法时传播。"""

    def __init__(
        self,
        registry: StrategyStudyRegistry,
        catalog: ValidatedCatalog,
        strategies: StrategyValidator,
        parser: StrategyStudyConfigParser | None = None,
    ) -> None:
        self._registry = registry
        self._catalog = catalog
        self._strategies = strategies
        self._parser = parser or StrategyStudyConfigParser()

    def validate(self, yaml_text: str) -> ResolvedStrategyStudy:
        """校验研究。入参：YAML 文本。返回值：规范配置。异常：配置或策略参数非法时传播。"""

        resolved = self._parser.parse(yaml_text)
        strategy = resolved.definition.strategy
        self._strategies.validate(strategy.strategy_id, strategy.parameters)
        return resolved

    def submit(
        self, yaml_text: str, *, actor: str = "user"
    ) -> StrategyStudyRecord:
        """提交研究。入参：YAML 文本和操作者。返回值：已入队快照。异常：门禁或事务失败时传播。"""

        resolved = self.validate(yaml_text)
        catalog_hash = self._catalog.require_validated_catalog().catalog_hash
        if len(catalog_hash) != 64:
            raise ValueError("validated catalog_hash must be a SHA-256 digest")
        study_id, _ = self._registry.create(
            resolved.definition,
            resolved.config_hash,
            catalog_hash,
            actor=actor,
        )
        return self._registry.get(study_id)

    def show(self, study_id: str) -> StrategyStudyRecord:
        """读取研究。入参：研究 ID。返回值：完整快照。异常：不存在时抛出键错误。"""

        return self._registry.get(study_id)

    def list(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        status: StrategyStudyStatus | None = None,
    ) -> tuple[StrategyStudyRecord, ...]:
        """列出研究。入参：分页和状态。返回值：稳定快照元组。异常：分页非法时传播。"""

        return self._registry.list(limit=limit, offset=offset, status=status)

    def delete(self, study_id: str, *, actor: str = "user") -> None:
        """删除研究。入参：研究 ID 和操作者。返回值：无。异常：活动研究由登记簿拒绝。"""

        self._registry.delete(study_id, actor=actor)


__all__ = [
    "StrategyStudyArtifactRecord",
    "StrategyStudyMetricRecord",
    "StrategyStudyRegistry",
    "StrategyStudyService",
    "StrategyStudyStage",
    "StrategyStudyStatus",
]
