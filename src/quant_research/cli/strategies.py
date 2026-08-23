"""注册策略目录 CLI 命令。"""

from __future__ import annotations

from collections.abc import Callable

import typer

from quant_research.cli.app import ApplicationServices, _CliSupport


class _StrategyCommands:
    """把策略目录查询绑定到 Typer。"""

    @staticmethod
    def register(
        group: typer.Typer,
        services_factory: Callable[[], ApplicationServices],
    ) -> None:
        """登记 ``quant strategies list`` 命令。"""

        @group.command("list")
        def list_strategies() -> None:
            _CliSupport._invoke_command(
                lambda services: _CliSupport._strategy_commands(services).list(),
                services_factory,
            )


__all__ = ["_StrategyCommands"]
