"""验证生产源码的中文公开文档与模块级入口约束。"""

from __future__ import annotations

import ast
import re
from collections.abc import Mapping
from pathlib import Path

_SOURCE_ROOT = Path(__file__).resolve().parents[2] / "src" / "quant_research"
_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_CHINESE = re.compile(r"[\u4e00-\u9fff]")
_REQUIRED_SECTIONS = ("入参", "返回值", "异常")
_MEANINGLESS_DOCSTRING_PHRASES = (
    "公开职责、状态与不变量",
    "调用所需的",
    "返回职责所述结果",
    "无主动抛出的异常；依赖调用异常按原契约传播",
    "定义的公开操作",
    "约定结果",
    "约定内容",
    "补充说明：",
    "的业务取值",
    "所得的",
    "外部仓储、文件系统或供应商失败时保留原异常类型和语义",
    "用于“",
    "保存量化研究流程使用的",
    "保存回测流程使用的",
    "保存实验流程使用的",
    "保存因子计算流程使用的",
)
_PUBLIC_SPECIAL_METHODS = frozenset(
    {
        "__call__",
        "__enter__",
        "__exit__",
        "__getitem__",
        "__iter__",
        "__next__",
        "__len__",
        "__contains__",
        "__str__",
    }
)
_ALLOWED_REASONS = frozenset({"stable_public_api", "framework_entry"})
_MODULE_FUNCTION_ALLOWLIST: Mapping[str, str] = {
    "quant_research.backtest.__getattr__": "framework_entry",
    "quant_research.infrastructure.persistence.migrations.versions.0005_research_platform.upgrade": "framework_entry",
    "quant_research.infrastructure.persistence.migrations.versions.0005_research_platform.downgrade": "framework_entry",
    "quant_research.infrastructure.persistence.migrations.versions.0006_data_initialization.upgrade": "framework_entry",
    "quant_research.infrastructure.persistence.migrations.versions.0006_data_initialization.downgrade": "framework_entry",
    "quant_research.infrastructure.persistence.migrations.versions.0007_experiment_runs.upgrade": "framework_entry",
    "quant_research.infrastructure.persistence.migrations.versions.0007_experiment_runs.downgrade": "framework_entry",
    "quant_research.infrastructure.persistence.migrations.versions.0008_independent_factor_studies.upgrade": "framework_entry",
    "quant_research.infrastructure.persistence.migrations.versions.0008_independent_factor_studies.downgrade": "framework_entry",
    "quant_research.infrastructure.persistence.migrations.versions.0008_independent_factor_studies._strip_strategy_kind": "framework_entry",
    "quant_research.infrastructure.persistence.migrations.versions.0008_independent_factor_studies._restore_strategy_kind": "framework_entry",
    "quant_research.infrastructure.persistence.migrations.versions.0008_independent_factor_studies._backup_experiment_dependents": "framework_entry",
    "quant_research.infrastructure.persistence.migrations.versions.0008_independent_factor_studies._restore_experiment_dependents": "framework_entry",
    "quant_research.signals.builtin.signal_component_hash": "stable_public_api",
    "quant_research.signals.models._validate_rows": "stable_public_api",
    "quant_research.analytics.attribution.calculate_attribution": "stable_public_api",
    "quant_research.analytics.performance.calculate_performance": "stable_public_api",
    "quant_research.cli.app.create_app": "stable_public_api",
    "quant_research.cli.app.run": "stable_public_api",
    "quant_research.bootstrap.cli.main": "framework_entry",
    "quant_research.bootstrap.dashboard.create_dashboard_app": "framework_entry",
    "quant_research.dashboard.app.create_dashboard_app": "stable_public_api",
    "quant_research.data.contracts.canonical_json_bytes": "stable_public_api",
    "quant_research.data.sources.financials.financial_report_period_end": "stable_public_api",
    "quant_research.data.sources.financials.financial_disclosure_deadline": "stable_public_api",
    "quant_research.data.sources.financials.financial_request_is_eligible": "stable_public_api",
    "quant_research.data.pipeline.ingest.partition_to_json": "stable_public_api",
    "quant_research.data.pipeline.ingest.partition_from_json": "stable_public_api",
    "quant_research.data.quality.models.freeze_json": "stable_public_api",
    "quant_research.data.quality.models.thaw_json": "stable_public_api",
    "quant_research.data.quality.rules.required_dataset_issues": "stable_public_api",
    "quant_research.data.quality.rules.cross_partition_schema_issues": "stable_public_api",
    "quant_research.data.quality.rules.canonical_schema_issues": "stable_public_api",
    "quant_research.data.quality.rules.canonical_conforming_partitions": "stable_public_api",
    "quant_research.data.quality.rules.primary_key_issues": "stable_public_api",
    "quant_research.data.quality.rules.required_value_issues": "stable_public_api",
    "quant_research.data.quality.rules.daily_bar_value_issues": "stable_public_api",
    "quant_research.data.quality.rules.coverage_issues": "stable_public_api",
    "quant_research.data.quality.rules.financial_availability_issues": "stable_public_api",
    "quant_research.data.quality.rules.dividend_event_issues": "stable_public_api",
    "quant_research.data.quality.rules.industry_state_issues": "stable_public_api",
    "quant_research.data.storage.verified_files.open_verified_file": "stable_public_api",
    "quant_research.data.storage.paths.resolved_storage_root": "stable_public_api",
    "quant_research.data.storage.paths.validate_storage_path": "stable_public_api",
    "quant_research.experiments.fingerprint.compute_fingerprint": "stable_public_api",
    "quant_research.experiments.fingerprint.resolve_source_identity": "stable_public_api",
    "quant_research.experiments.fingerprint.hash_lockfile": "stable_public_api",
    "quant_research.experiments.fingerprint.capture_environment": "stable_public_api",
    "quant_research.bootstrap.worker.build_experiment_worker": "stable_public_api",
    "quant_research.bootstrap.worker.build_default_experiment_worker": "stable_public_api",
    "quant_research.factor_studies.analysis.build_future_returns": "stable_public_api",
    "quant_research.factor_studies.analysis.analyze": "stable_public_api",
    "quant_research.factors.analysis.coverage_by_date": "stable_public_api",
    "quant_research.factors.analysis.spearman_rank_ic": "stable_public_api",
    "quant_research.factors.analysis.assign_quantiles": "stable_public_api",
    "quant_research.factors.analysis.quantile_future_returns": "stable_public_api",
    "quant_research.factors.analysis.long_short_returns": "stable_public_api",
    "quant_research.factors.analysis.factor_correlation_matrix": "stable_public_api",
    "quant_research.factors.analysis.factor_rank_correlation_matrix": "stable_public_api",
    "quant_research.factors.base.canonical_factor_ref": "stable_public_api",
    "quant_research.factors.base.validate_sha256": "stable_public_api",
    "quant_research.factors.base.is_available_on_signal_day": "stable_public_api",
    "quant_research.factors.base.validate_factor_output_scope": "stable_public_api",
    "quant_research.factors.base.validate_factor_output": "stable_public_api",
    "quant_research.factors.base.factor_table_content_hash": "stable_public_api",
    "quant_research.factors.base.thaw_json": "stable_public_api",
    "quant_research.factors.builtin.register_builtin": "stable_public_api",
    "quant_research.factors.builtin.register_etf_factors": "stable_public_api",
    "quant_research.factors.builtin.register_stock_factors": "stable_public_api",
    "quant_research.factors.builtin._stock_common.output_frame": "stable_public_api",
    "quant_research.factors.builtin._stock_common.trading_signal_dates": "stable_public_api",
    "quant_research.factors.builtin._stock_common.canonical_scope": "stable_public_api",
    "quant_research.factors.builtin.auxiliary.assert_alpha_eligible": "stable_public_api",
    "quant_research.factors.builtin.code_hash.builtin_source_hash": "stable_public_api",
    "quant_research.factors.transforms.winsorize_mad": "stable_public_api",
    "quant_research.factors.transforms.neutralize_industry": "stable_public_api",
    "quant_research.factors.transforms.zscore": "stable_public_api",
    "quant_research.logging.redact_context": "stable_public_api",
    "quant_research.logging.sensitive_environment_values": "stable_public_api",
    "quant_research.infrastructure.persistence.database.create_sqlite_engine": "stable_public_api",
    "quant_research.infrastructure.persistence.database.upgrade_database": "stable_public_api",
    "quant_research.infrastructure.persistence.migrations.env.run_migrations_offline": "framework_entry",
    "quant_research.infrastructure.persistence.migrations.env.run_migrations_online": "framework_entry",
    "quant_research.infrastructure.persistence.migrations.versions.0001_initial.upgrade": "framework_entry",
    "quant_research.infrastructure.persistence.migrations.versions.0001_initial.downgrade": "framework_entry",
    "quant_research.infrastructure.persistence.migrations.versions.0002_factor_studies.upgrade": "framework_entry",
    "quant_research.infrastructure.persistence.migrations.versions.0002_factor_studies.downgrade": "framework_entry",
    "quant_research.infrastructure.persistence.migrations.versions.0003_data_center_operations.upgrade": "framework_entry",
    "quant_research.infrastructure.persistence.migrations.versions.0003_data_center_operations.downgrade": "framework_entry",
    "quant_research.infrastructure.persistence.migrations.versions.0004_quality_rule_results.upgrade": "framework_entry",
    "quant_research.infrastructure.persistence.migrations.versions.0004_quality_rule_results.downgrade": "framework_entry",
    "quant_research.infrastructure.persistence.repositories.canonical_dataset_hash": "stable_public_api",
    "quant_research.portfolio.constructor.validate_target_portfolio": "stable_public_api",
    "quant_research.strategies.__getattr__": "framework_entry",
}


class SourceConventionInspector:
    """检查源码树的模块文档、公共 API 文档和模块级函数白名单。

    入参：
        source_root：需要检查的 Python 包根目录。
        module_functions：允许保留的模块级函数及其原因。
    返回值：
        构造并返回 SourceConventionInspector 实例。
    异常：
        ValueError：白名单包含未知原因时抛出。
    """

    def __init__(
        self,
        source_root: Path,
        module_functions: Mapping[str, str],
    ) -> None:
        unknown = set(module_functions.values()) - _ALLOWED_REASONS
        if unknown:
            raise ValueError(f"unknown module function reasons: {sorted(unknown)}")
        self._source_root = source_root
        self._module_functions = dict(module_functions)

    def inspect(self) -> tuple[str, ...]:
        """检查整个源码树并返回稳定排序的违规描述。

        入参：
            无。
        返回值：
            返回违规描述元组；没有违规时返回空元组。
        异常：
            SyntaxError：被检查模块不是有效 Python 源码时抛出。
        """
        violations: list[str] = []
        discovered_functions: set[str] = set()
        for path in sorted(self._source_root.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            module_name = self._module_name(path)
            violations.extend(
                self._document_violations(
                    path,
                    1,
                    "<module>",
                    ast.get_docstring(tree, clean=False),
                    structured=False,
                )
            )
            for node in tree.body:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    qualified = f"{module_name}.{node.name}"
                    discovered_functions.add(qualified)
                    if qualified not in self._module_functions:
                        violations.append(
                            self._message(
                                path,
                                node.lineno,
                                node.name,
                                "模块级函数未登记稳定 API 或框架入口原因",
                            )
                        )
                    if not node.name.startswith("_") or node.name == "__getattr__":
                        violations.extend(
                            self._document_violations(
                                path,
                                node.lineno,
                                node.name,
                                ast.get_docstring(node, clean=False),
                                structured=True,
                            )
                        )
                        document = ast.get_docstring(node, clean=False) or ""
                        if not any(
                            marker in document
                            for marker in ("模块级", "模块协议", "框架")
                        ):
                            violations.append(
                                self._message(
                                    path,
                                    node.lineno,
                                    node.name,
                                    "模块级入口文档未说明保留原因",
                                )
                            )
                elif isinstance(node, ast.ClassDef) and not node.name.startswith("_"):
                    violations.extend(self._class_violations(path, node))
        stale = set(self._module_functions) - discovered_functions
        violations.extend(
            f"白名单包含不存在的模块级函数：{name}" for name in sorted(stale)
        )
        return tuple(sorted(violations))

    def _class_violations(
        self,
        path: Path,
        node: ast.ClassDef,
    ) -> list[str]:
        violations = self._document_violations(
            path,
            node.lineno,
            node.name,
            ast.get_docstring(node, clean=False),
            structured=True,
        )
        for child in node.body:
            if not isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if child.name.startswith("_") and child.name not in _PUBLIC_SPECIAL_METHODS:
                continue
            violations.extend(
                self._document_violations(
                    path,
                    child.lineno,
                    f"{node.name}.{child.name}",
                    ast.get_docstring(child, clean=False),
                    structured=True,
                )
            )
        return violations

    def _document_violations(
        self,
        path: Path,
        line: int,
        symbol: str,
        document: str | None,
        *,
        structured: bool,
    ) -> list[str]:
        if not document:
            return [self._message(path, line, symbol, "缺少 docstring")]
        violations: list[str] = []
        if _CHINESE.search(document) is None:
            violations.append(self._message(path, line, symbol, "docstring 不含中文"))
        for phrase in _MEANINGLESS_DOCSTRING_PHRASES:
            if phrase in document:
                violations.append(
                    self._message(
                        path,
                        line,
                        symbol,
                        f"docstring 含空泛模板话术“{phrase}”",
                    )
                )
        if structured:
            for section in _REQUIRED_SECTIONS:
                if section not in document:
                    violations.append(
                        self._message(
                            path,
                            line,
                            symbol,
                            f"docstring 缺少“{section}”部分",
                        )
                    )
        return violations

    def _module_name(self, path: Path) -> str:
        relative = path.relative_to(self._source_root).with_suffix("")
        parts = [self._source_root.name, *relative.parts]
        if parts[-1] == "__init__":
            parts.pop()
        return ".".join(parts)

    def _message(self, path: Path, line: int, symbol: str, detail: str) -> str:
        relative = path.relative_to(self._source_root)
        return f"{relative.as_posix()}:{line}: {symbol}: {detail}"


def test_production_source_conventions() -> None:
    """验证生产源码满足中文文档与模块级入口约束。

    入参：
        无。
    返回值：
        无。
    异常：
        AssertionError：发现任一源码约束违规时抛出。
    """
    violations = SourceConventionInspector(
        _SOURCE_ROOT,
        _MODULE_FUNCTION_ALLOWLIST,
    ).inspect()

    assert not violations, "\n" + "\n".join(violations)


def test_source_convention_inspector_accepts_compliant_module(
    tmp_path: Path,
) -> None:
    """验证检查器接受文档完整且入口已登记的正例。

    入参：
        tmp_path：pytest 提供的隔离临时目录。
    返回值：
        无。
    异常：
        AssertionError：正例被错误拒绝时抛出。
    """
    source_root = tmp_path / "positive"
    source_root.mkdir()
    (source_root / "sample.py").write_text(
        '''"""提供正例模块的公开处理流程。"""

def accepted(value: int) -> int:
    """转换输入值；该函数作为稳定公开 API 保留在模块级。

    入参：
        value：待转换的整数。
    返回值：
        返回转换后的整数。
    异常：
        无。
    """
    return value
''',
        encoding="utf-8",
    )

    violations = SourceConventionInspector(
        source_root,
        {"positive.sample.accepted": "stable_public_api"},
    ).inspect()

    assert violations == ()


def test_source_convention_inspector_reports_invalid_module(
    tmp_path: Path,
) -> None:
    """验证检查器同时报告英文文档、缺失文档和未登记函数。

    入参：
        tmp_path：pytest 提供的隔离临时目录。
    返回值：
        无。
    异常：
        AssertionError：负例未产生预期诊断时抛出。
    """
    source_root = tmp_path / "negative"
    source_root.mkdir()
    (source_root / "broken.py").write_text(
        '''"""English only."""

def visible(value: int) -> int:
    return value

def _hidden() -> None:
    return None
''',
        encoding="utf-8",
    )

    violations = SourceConventionInspector(source_root, {}).inspect()
    message = "\n".join(violations)

    assert "docstring 不含中文" in message
    assert "visible: 缺少 docstring" in message
    assert message.count("模块级函数未登记") == 2


def test_source_convention_inspector_rejects_meaningless_template(
    tmp_path: Path,
) -> None:
    """验证检查器拒绝只有类型复述、没有业务语义的模板文档。

    入参：
        tmp_path：pytest 提供的隔离临时目录。
    返回值：
        无。
    异常：
        AssertionError：空泛模板未被诊断时抛出。
    """
    source_root = tmp_path / "template"
    source_root.mkdir()
    (source_root / "sample.py").write_text(
        '''"""提供模板负例。"""

class Rules:
    """封装规则的公开职责、状态与不变量。

    入参：
        threshold：调用所需的阈值参数。
    返回值：
        返回职责所述结果。
    异常：
        无。
    """
''',
        encoding="utf-8",
    )

    message = "\n".join(SourceConventionInspector(source_root, {}).inspect())

    assert "公开职责、状态与不变量" in message
    assert "调用所需的" in message
    assert "返回职责所述结果" in message


def test_repository_root_does_not_offer_a_source_tree_env_example() -> None:
    """验证运行凭据只能由数据根设置文件承载。

    入参：无。
    返回值：无。
    异常：代码根重新出现 ``.env.example`` 时由断言阻断。
    """
    assert not (_REPOSITORY_ROOT / ".env.example").exists()
