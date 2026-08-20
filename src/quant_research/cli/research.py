"""注册目标研究族和组件目录 CLI 命令。"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Protocol

import typer

from quant_research.application.research_platform import ResearchCommandService
from quant_research.cli.app import ApplicationServices, _CliSupport


class ResearchQueryPort(Protocol):
    """定义 CLI 查询研究族和组件目录所需的只读操作。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def families(self, *, page: int, page_size: int) -> object:
        """定义 families 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def family(self, family_id: str) -> object:
        """定义 family 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...


    def component_catalog(self) -> object:
        """定义 component_catalog 端口操作。

        入参：参数含义由端口签名及类型声明给出。
        返回值：返回端口声明的不可变领域结果。
        异常：实现不满足契约时传播对应领域或依赖异常。
        """
        ...



class LocalResearchCommands:
    """从受信配置目录提交研究，并复用 Dashboard 查询 DTO。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def __init__(
        self,
        service: ResearchCommandService,
        views: ResearchQueryPort,
        config_root: Path,
    ) -> None:
        self._service = service
        self._views = views
        self._config_root = config_root.resolve()

    def validate(self, config: str) -> object:
        """解析并预览研究 YAML。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        return self._service.validate_yaml(self._read(config))

    def submit(self, config: str) -> object:
        """创建研究族并提交首次执行。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        return self._service.submit(self._read(config), request_id="cli-research-submit", actor="cli")

    def list(self) -> object:
        """列出最近研究族。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        return self._views.families(page=1, page_size=100)

    def show(self, family_id: str) -> object:
        """显示研究族、候选、运行和指标。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        return self._views.family(family_id)

    def rerun(self, family_id: str) -> object:
        """以当前数据和环境身份创建新执行。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        return self._service.rerun(family_id, request_id="cli-research-rerun", actor="cli")

    def components(self) -> object:
        """列出可组装组件和参考策略。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        return self._views.component_catalog()

    def _read(self, value: str) -> str:
        path = Path(value)
        candidate = path.resolve() if path.is_absolute() else (self._config_root / path).resolve()
        if not candidate.is_relative_to(self._config_root) or not candidate.is_file():
            raise ValueError("research config must be a file inside configs")
        return candidate.read_text(encoding="utf-8")


class _ResearchCommands:
    """把 Typer 命令绑定到可注入研究命令端口。"""

    @staticmethod
    def register(
        research: typer.Typer,
        components: typer.Typer,
        services_factory: Callable[[], ApplicationServices],
    ) -> None:
        @research.command("validate")
        def validate(config: str) -> None:
            _CliSupport._invoke_command(lambda services: _CliSupport._research_commands(services).validate(config), services_factory)

        @research.command("submit")
        def submit(config: str) -> None:
            _CliSupport._invoke_command(lambda services: _CliSupport._research_commands(services).submit(config), services_factory)

        @research.command("list")
        def list_families() -> None:
            _CliSupport._invoke_command(lambda services: _CliSupport._research_commands(services).list(), services_factory)

        @research.command("show")
        def show(family_id: str) -> None:
            _CliSupport._invoke_command(lambda services: _CliSupport._research_commands(services).show(family_id), services_factory)

        @research.command("rerun")
        def rerun(family_id: str) -> None:
            _CliSupport._invoke_command(lambda services: _CliSupport._research_commands(services).rerun(family_id), services_factory)

        @components.command("list")
        def list_components() -> None:
            _CliSupport._invoke_command(lambda services: _CliSupport._research_commands(services).components(), services_factory)
