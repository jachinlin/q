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
