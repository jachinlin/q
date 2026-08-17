"""定义因子研究用例所需的持久化端口。"""

from pathlib import Path
from typing import Protocol

from quant_research.factor_studies.models import FactorRunStatus, FactorStudyConfig


class FactorStudyStore(Protocol):
    """约束因子研究定义、运行记录和状态转换的持久化能力。

    入参：
        无；实现类按各方法契约接收参数。
    返回值：
        构造并返回满足协议的对象。
    异常：
        具体实现传播校验、并发或持久化异常。
    """

    def create_study(self, name: str, config: FactorStudyConfig) -> str:
        """创建研究定义。入参：名称与配置。返回值：研究标识。异常：校验失败。"""
        ...

    def create_run(self, study_id: str, catalog_hash: str, source_hash: str) -> str:
        """创建研究运行。入参：研究及哈希。返回值：运行标识。异常：研究不存在。"""
        ...

    def bind_task(self, run_id: str, task_id: str) -> None:
        """绑定后台任务。入参：运行和任务标识。返回值：无。异常：状态冲突。"""
        ...

    def transition(
        self,
        run_id: str,
        expected: FactorRunStatus,
        target: FactorRunStatus,
        *,
        manifest_path: Path | None = None,
        manifest_hash: str | None = None,
        error: dict[str, object] | None = None,
    ) -> None:
        """转换运行状态。入参：状态及终态信息。返回值：无。异常：状态冲突。"""
        ...

    def get_study(self, study_id: str) -> dict[str, object]:
        """读取研究。入参：研究标识。返回值：研究映射。异常：标识不存在。"""
        ...

    def get_run(self, run_id: str) -> dict[str, object]:
        """读取运行。入参：运行标识。返回值：运行映射。异常：标识不存在。"""
        ...

    def get_run_by_task(self, task_id: str) -> dict[str, object]:
        """按任务读取运行。入参：任务标识。返回值：运行映射。异常：不存在。"""
        ...

    def list_studies(self, page: int, page_size: int) -> dict[str, object]:
        """列出研究。入参：分页参数。返回值：列表映射。异常：分页非法。"""
        ...


__all__ = ["FactorStudyStore"]
