"""提供可注入的命令行接口与展示适配器。"""

from quant_research.cli.app import ApplicationServices, create_app, run

__all__ = ["ApplicationServices", "create_app", "run"]
