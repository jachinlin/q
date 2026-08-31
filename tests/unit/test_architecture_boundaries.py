"""验证 ``quant_research`` 的入口、应用与基础设施依赖方向。"""

from __future__ import annotations

import ast
from pathlib import Path

_PACKAGE_ROOT = Path(__file__).resolve().parents[2] / "src" / "quant_research"
_OUTER_LAYERS = frozenset(
    {"application", "bootstrap", "cli", "dashboard", "infrastructure"}
)
_PURE_CAPABILITIES = frozenset(
    {
        "analytics",
        "backtest",
        "domain",
        "factor_studies",
        "factors",
        "portfolio",
        "strategies",
        "strategy_studies",
        "tasks",
        "universe",
    }
)


class ArchitectureInspector:
    """读取 Python AST 并报告跨越既定层次的反向导入。"""

    def __init__(self, package_root: Path) -> None:
        self._package_root = package_root

    def inspect(self) -> tuple[str, ...]:
        violations: list[str] = []
        for path in sorted(self._package_root.rglob("*.py")):
            relative = path.relative_to(self._package_root)
            owner = relative.parts[0]
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                imported = self._imported_module(node)
                if imported is None or not imported.startswith("quant_research."):
                    continue
                target = imported.split(".", 2)[1]
                if self._forbidden(owner, relative, target):
                    violations.append(
                        f"{relative.as_posix()}:{node.lineno}: "
                        f"{owner} must not import {target}"
                    )
        return tuple(violations)

    @staticmethod
    def _imported_module(node: ast.AST) -> str | None:
        if isinstance(node, ast.ImportFrom):
            return node.module
        if isinstance(node, ast.Import) and node.names:
            return node.names[0].name
        return None

    @staticmethod
    def _forbidden(owner: str, relative: Path, target: str) -> bool:
        if owner in _PURE_CAPABILITIES:
            return target in _OUTER_LAYERS
        if owner == "application":
            return target in {"bootstrap", "cli", "dashboard", "infrastructure"}
        if owner == "cli":
            return target in {"bootstrap", "dashboard", "infrastructure"}
        if owner == "infrastructure":
            return target in {"bootstrap", "cli", "dashboard"}
        if owner == "dashboard":
            if target in {"bootstrap", "cli"}:
                return True
            interface_files = {"app.py", "models.py"}
            is_route = relative.parts[0:2] == ("dashboard", "routes")
            return target == "infrastructure" and (
                relative.name in interface_files or is_route
            )
        return False


def test_package_dependency_direction() -> None:
    """验证能力、应用和接口层不存在反向导入。"""
    assert ArchitectureInspector(_PACKAGE_ROOT).inspect() == ()


def test_legacy_package_is_absent() -> None:
    """验证生产源码不再声明或导入旧 ``quant_core`` 包。"""
    legacy = [
        path
        for path in _PACKAGE_ROOT.rglob("*.py")
        if "quant_core" in path.read_text(encoding="utf-8")
    ]
    assert legacy == []


def test_legacy_strategy_experiment_surface_is_absent() -> None:
    """锁定单一 StrategyStudy 包、任务和接口，不允许旧策略实验语义回流。"""
    assert (_PACKAGE_ROOT / "strategy_studies" / "models.py").is_file()
    legacy_package = _PACKAGE_ROOT / "experiments"
    assert not legacy_package.exists() or not tuple(legacy_package.glob("*.py"))
    current_sources = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted(_PACKAGE_ROOT.rglob("*.py"))
        if "migrations" not in path.parts
    )
    for removed in (
        "EXPERIMENT_RUN",
        "/api/v1/experiments",
        "ExperimentRunRegistry",
        "baseline_run_id",
        "sample_windows",
        "test_budget",
    ):
        assert removed not in current_sources


def test_data_layer_package_layout() -> None:
    """锁定数据层子包职责，并确保供应商适配器只位于基础设施层。"""
    data_root = _PACKAGE_ROOT / "data"
    expected_packages = {"canonical", "pipeline", "quality", "sources", "storage"}
    assert {
        path.name
        for path in data_root.iterdir()
        if path.is_dir() and (path / "__init__.py").is_file()
    } >= expected_packages

    obsolete_paths = (
        data_root / "pipelines",
        data_root / "schemas.py",
        data_root / "adjustments.py",
        data_root / "routing.py",
        data_root / "financials.py",
        data_root / "partitions.py",
        data_root / "storage.py",
        data_root / "safe_files.py",
    )
    assert not any(path.exists() for path in obsolete_paths)
    assert not any(
        "baostock" in path.name.lower()
        for path in (data_root / "sources").rglob("*.py")
    )
    assert (_PACKAGE_ROOT / "infrastructure" / "tushare").is_dir()
    assert not (_PACKAGE_ROOT / "infrastructure" / "baostock").exists()


def test_builtin_strategies_use_independent_documented_packages() -> None:
    """锁定每个内置策略的独立代码与结构性说明目录。"""
    strategy_root = _PACKAGE_ROOT / "strategies"
    for strategy_id in ("dual_ma_trend", "etf_rotation", "stock_multifactor"):
        package = strategy_root / strategy_id
        assert (package / "__init__.py").is_file()
        assert (package / "strategy.py").is_file()
        assert (package / "README.md").is_file()
    assert not any(
        (strategy_root / name).exists()
        for name in ("dual_ma.py", "etf_rotation.py", "multifactor.py")
    )
