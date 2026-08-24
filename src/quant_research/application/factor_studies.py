"""提供独立因子研究校验、提交、查询和人工结论用例。"""

from __future__ import annotations

from typing import Protocol

from quant_research.factor_studies.config import (
    FactorStudyConfigParser,
    ResolvedFactorStudy,
)
from quant_research.factor_studies.models import (
    FactorDecisionMark,
    FactorStudyDecisionKey,
    FactorStudyDefinition,
    FactorStudyRecord,
    FactorStudyStatus,
)


class CatalogIdentity(Protocol):
    """暴露目录身份。入参：实现实例。返回值：身份端口。异常：实现不满足协议时类型检查失败。"""

    @property
    def catalog_hash(self) -> str:
        """读取目录身份。入参：无。返回值：SHA-256。异常：目录不可用时由实现抛出。"""
        ...


class ValidatedCatalog(Protocol):
    """提供验证目录。入参：实现实例。返回值：目录端口。异常：实现不满足协议时类型检查失败。"""

    def require_validated_catalog(self) -> CatalogIdentity:
        """读取验证目录。入参：无。返回值：目录身份。异常：门禁关闭时抛出领域错误。"""
        ...


class FactorCatalog(Protocol):
    """提供因子目录。入参：实现实例。返回值：目录端口。异常：实现不满足协议时类型检查失败。"""

    def resolve(self, reference: str) -> str:
        """解析因子引用。入参：引用。返回值：规范引用。异常：未知引用时抛出值错误。"""
        ...

    def registered_references(self) -> tuple[str, ...]:
        """列出因子引用。入参：无。返回值：有序引用。异常：目录不可用时由实现抛出。"""
        ...


class FactorStudyRegistry(Protocol):
    """定义研究持久化端口。入参：实现实例。返回值：仓储端口。异常：实现不满足协议时类型检查失败。"""

    def create(
        self,
        definition: FactorStudyDefinition,
        config_hash: str,
        catalog_hash: str,
        *,
        actor: str,
    ) -> tuple[str, str]:
        """原子创建研究和任务。入参：冻结配置、身份和操作者。返回值：研究与任务 ID。异常：事务失败时抛出。"""
        ...

    def get(self, study_id: str) -> FactorStudyRecord:
        """读取研究。入参：研究 ID。返回值：研究快照。异常：研究不存在时抛出值错误。"""
        ...

    def list(
        self,
        *,
        limit: int,
        offset: int,
        status: FactorStudyStatus | None = None,
        decision: FactorDecisionMark | None = None,
    ) -> tuple[FactorStudyRecord, ...]:
        """分页列出研究。入参：分页与筛选条件。返回值：有序快照。异常：参数非法时抛出值错误。"""
        ...

    def decide(
        self,
        study_id: str,
        key: FactorStudyDecisionKey,
        mark: FactorDecisionMark,
        note: str,
        *,
        actor: str,
    ) -> None:
        """写入人工结论。入参：研究、决策键、结论和审计字段。返回值：无。异常：状态或键非法时抛出。"""
        ...

    def delete(self, study_id: str, *, actor: str) -> None:
        """删除终态研究。入参：研究 ID 和操作者。返回值：无。异常：非终态或不存在时抛出。"""
        ...

    def retry(self, study_id: str, *, actor: str) -> str:
        """重新排队研究。入参：研究 ID 和操作者。返回值：任务 ID。异常：状态非法时抛出。"""
        ...


class FactorStudyService:
    """协调研究用例。入参：仓储、数据门禁、因子目录和解析器。返回值：服务实例。异常：依赖非法时抛出。"""

    def __init__(
        self,
        registry: FactorStudyRegistry,
        catalog: ValidatedCatalog,
        factors: FactorCatalog,
        parser: FactorStudyConfigParser | None = None,
    ) -> None:
        self._registry = registry
        self._catalog = catalog
        self._factors = factors
        self._parser = parser or FactorStudyConfigParser()

    def validate(self, yaml_text: str) -> ResolvedFactorStudy:
        """校验研究配置。入参：YAML 文本。返回值：规范结果。异常：配置或因子未知时抛出值错误。"""
        resolved = self._parser.parse(yaml_text)
        for factor_id in resolved.definition.factor_ids:
            self._factors.resolve(factor_id)
        return resolved

    def submit(self, yaml_text: str, *, actor: str = "user") -> FactorStudyRecord:
        """提交研究。入参：YAML 文本和操作者。返回值：已排队研究。异常：门禁或事务失败时抛出。"""
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

    def show(self, study_id: str) -> FactorStudyRecord:
        """读取研究。入参：研究 ID。返回值：完整快照。异常：研究不存在时抛出值错误。"""
        return self._registry.get(study_id)

    def list(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        status: FactorStudyStatus | None = None,
        decision: FactorDecisionMark | None = None,
    ) -> tuple[FactorStudyRecord, ...]:
        """列出研究。入参：分页和筛选条件。返回值：有序快照。异常：参数非法时抛出值错误。"""
        return self._registry.list(
            limit=limit, offset=offset, status=status, decision=decision
        )

    def decide(
        self,
        study_id: str,
        key: FactorStudyDecisionKey,
        mark: FactorDecisionMark,
        note: str,
        *,
        actor: str = "user",
    ) -> FactorStudyRecord:
        """保存人工结论。入参：研究、决策键、结论、备注和操作者。返回值：最新快照。异常：键或状态非法时抛出。"""
        self._registry.decide(study_id, key, mark, note, actor=actor)
        return self._registry.get(study_id)

    def delete(self, study_id: str, *, actor: str = "user") -> None:
        """删除研究。入参：研究 ID 和操作者。返回值：无。异常：研究非终态或不存在时抛出。"""
        self._registry.delete(study_id, actor=actor)

    def retry(self, study_id: str, *, actor: str = "user") -> FactorStudyRecord:
        """重试研究。入参：研究 ID 和操作者。返回值：重新排队快照。异常：状态非法时抛出。"""
        self._registry.retry(study_id, actor=actor)
        return self._registry.get(study_id)

    def catalog(self) -> tuple[str, ...]:
        """读取因子目录。入参：无。返回值：有序引用。异常：目录不可用时由依赖抛出。"""
        return self._factors.registered_references()


__all__ = ["FactorStudyRegistry", "FactorStudyService"]
