"""装配研究平台身份、注册表、运行时和任务处理器。"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import cast

from sqlalchemy import Engine

from quant_research.application.canonical_research_runtime import (
    CanonicalResearchRuntime,
)
from quant_research.application.research_platform import (
    ResearchCommandService,
    ResearchExecutionIdentity,
    ResearchExpandHandler,
    ResearchIdentityProvider,
    ResearchRegisterHandler,
    ResearchRunHandler,
    ResearchSelectHandler,
)
from quant_research.backtest.rulebook import MarketRuleBook
from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.data.repository import CanonicalCatalog, ResearchDataRepository
from quant_research.experiments.fingerprint import capture_environment
from quant_research.experiments.research_artifacts import ResearchArtifactPublisher
from quant_research.infrastructure.persistence.research_registry import ResearchRegistry
from quant_research.infrastructure.persistence.research_task_queue import (
    ResearchTaskQueue,
)
from quant_research.infrastructure.persistence.task_queue import TaskQueue
from quant_research.research_protocols import ResearchConfigResolver
from quant_research.strategies.definitions import ComponentRegistry
from quant_research.tasks.handlers import TaskHandler


class LocalResearchIdentityProvider(ResearchIdentityProvider):
    """从当前验证目录、源码树、锁文件和唯一规则文件捕获身份。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def __init__(
        self,
        catalog: CanonicalCatalog,
        source_root: Path,
        rulebook: MarketRuleBook,
    ) -> None:
        self._catalog = catalog
        self._source_root = source_root
        self._rulebook = rulebook

    def capture(self) -> ResearchExecutionIdentity:
        """返回一次提交使用的完整不可变身份。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        state = self._catalog.require_validated_catalog()
        environment = capture_environment(
            self._source_root, self._source_root / "uv.lock"
        )
        return ResearchExecutionIdentity(
            catalog_hash=state.catalog_hash,
            source_hash=cast(str, environment["source_hash"]),
            lockfile_hash=cast(str, environment["lockfile_hash"]),
            rulebook_hash=self._rulebook.content_hash,
            environment_hash=hashlib.sha256(
                canonical_json_bytes(cast(JsonValue, environment))
            ).hexdigest(),
        )


class ResearchPlatform:
    """聚合研究命令、查询依赖和 Worker 处理器。

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
        commands: ResearchCommandService,
        registry: ResearchRegistry,
        components: ComponentRegistry,
        handlers: tuple[TaskHandler, ...],
    ) -> None:
        self.commands = commands
        self.registry = registry
        self.components = components
        self.handlers = handlers


def build_research_platform(
    *,
    engine: Engine,
    queue: TaskQueue,
    repository: ResearchDataRepository,
    source_root: Path,
    artifact_root: Path,
    rulebook: MarketRuleBook,
) -> ResearchPlatform:
    """装配目标研究平台且复用现有 Canonical Repository 和任务队列。

该函数作为模块级确定性辅助或框架入口保留。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """
    registry = ResearchRegistry(engine)
    research_queue = ResearchTaskQueue(engine, queue)
    components = ComponentRegistry()
    commands = ResearchCommandService(
        resolver=ResearchConfigResolver(),
        components=components,
        registry=registry,
        queue=research_queue,
        identities=LocalResearchIdentityProvider(
            repository.catalog(), source_root, rulebook
        ),
    )
    publisher = ResearchArtifactPublisher(artifact_root)
    runtime = CanonicalResearchRuntime(repository, publisher, rulebook)
    handlers: tuple[TaskHandler, ...] = (
        ResearchExpandHandler(registry, research_queue),
        ResearchRunHandler(registry, research_queue, runtime),
        ResearchSelectHandler(registry, research_queue, publisher),
        ResearchRegisterHandler(registry),
    )
    return ResearchPlatform(
        commands=commands,
        registry=registry,
        components=components,
        handlers=handlers,
    )
