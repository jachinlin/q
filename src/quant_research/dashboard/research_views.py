"""提供研究中心所需的组件、研究族、候选与运行只读视图。"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import cast

import polars as pl

from quant_research.data.contracts import JsonValue
from quant_research.experiments.research import FamilyExecutionRecord, ResearchMark
from quant_research.infrastructure.persistence.research_registry import ResearchRegistry
from quant_research.strategies.definitions import ComponentRegistry


class ResearchDashboardService:
    """组合研究注册表和受信配置模板形成 Dashboard DTO。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def __init__(
        self,
        registry: ResearchRegistry,
        components: ComponentRegistry,
        template_root: Path,
        artifact_root: Path,
    ) -> None:
        self._registry = registry
        self._components = components
        self._template_root = template_root.resolve()
        self._artifact_root = (artifact_root / "research").resolve()

    def component_catalog(self) -> dict[str, JsonValue]:
        """返回组件能力和三个参考策略目录。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        return self._components.as_json()

    def templates(self) -> dict[str, JsonValue]:
        """返回受信配置根中的三个完整 YAML 模板。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        items: list[JsonValue] = []
        for template in self._components.templates():
            path = (self._template_root / f"{template.strategy_id}.yaml").resolve()
            if not path.is_relative_to(self._template_root) or not path.is_file():
                raise ValueError(f"research template is missing: {template.strategy_id}")
            items.append(
                {
                    "strategy_id": template.strategy_id,
                    "label": template.label,
                    "signal_kind": template.signal_kind.value,
                    "yaml": path.read_text(encoding="utf-8"),
                }
            )
        return {"items": items}

    def families(self, *, page: int, page_size: int) -> dict[str, JsonValue]:
        """分页返回研究族和最新执行摘要。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        records = self._registry.list_families(limit=page_size, offset=(page - 1) * page_size)
        items: list[JsonValue] = []
        for family in records:
            executions = self._registry.list_executions(family.id)
            latest = executions[-1] if executions else None
            items.append(
                {
                    "id": family.id,
                    "name": family.name,
                    "hypothesis": family.hypothesis,
                    "strategy_id": family.strategy_id,
                    "research_mode": family.research_mode.value,
                    "config_hash": family.config_hash,
                    "mark": family.mark.value,
                    "tags": list(self._registry.list_tags(family.id)),
                    "created_at": family.created_at.isoformat(),
                    "latest_execution": None if latest is None else self._execution(latest),
                }
            )
        return {"items": items, "page": page, "page_size": page_size}

    def family(self, family_id: str) -> dict[str, JsonValue]:
        """返回研究协议、执行、候选、运行和分区指标。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        family = self._registry.get_family(family_id)
        executions = self._registry.list_executions(family_id)
        execution_items: list[JsonValue] = []
        for execution in executions:
            variants = self._registry.list_variants(execution.id)
            runs = self._registry.list_runs(execution.id)
            execution_items.append(
                {
                    **self._execution(execution),
                    "variants": [
                        {
                            "id": item.id,
                            "ordinal": item.ordinal,
                            "composition_hash": item.composition_hash,
                            "parameters": item.parameters,
                            "rejection_reasons": list(item.rejection_reasons),
                        }
                        for item in variants
                    ],
                    "runs": [
                        {
                            "id": run.id,
                            "variant_id": run.variant_id,
                            "phase": run.phase.value,
                            "status": run.status.value,
                            "stage": run.stage.value,
                            "stage_status": run.stage_status,
                            "manifest_hash": run.manifest_hash,
                            "created_at": run.created_at.isoformat(),
                            "completed_at": run.completed_at.isoformat() if run.completed_at else None,
                            "metrics": [
                                {
                                    "split": metric.split,
                                    "category": metric.category,
                                    "name": metric.name,
                                    "value": metric.value,
                                    "unit": metric.unit,
                                    "p_value": metric.p_value,
                                    "adjusted_p_value": metric.adjusted_p_value,
                                }
                                for metric in self._registry.list_metrics(run.id)
                            ],
                        }
                        for run in runs
                    ],
                }
            )
        return {
            "id": family.id,
            "name": family.name,
            "hypothesis": family.hypothesis,
            "strategy_id": family.strategy_id,
            "research_mode": family.research_mode.value,
            "config": family.config,
            "config_hash": family.config_hash,
            "mark": family.mark.value,
            "note": family.note,
            "tags": list(self._registry.list_tags(family.id)),
            "created_at": family.created_at.isoformat(),
            "executions": execution_items,
        }

    def update_research(
        self,
        family_id: str,
        *,
        mark: str,
        note: str | None,
        tags: tuple[str, ...],
    ) -> dict[str, JsonValue]:
        """更新研究者可变结论字段并返回详情。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        self._registry.update_family_research(
            family_id,
            mark=ResearchMark(mark),
            note=note,
            tags=tags,
        )
        return self.family(family_id)

    def artifact(
        self,
        run_id: str,
        artifact_type: str,
        *,
        page: int,
        page_size: int,
    ) -> dict[str, JsonValue]:
        """从白名单 Manifest 项分页读取运行产物。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        paths = {
            "signals": "signals/signals.parquet",
            "portfolio": "target_portfolios.parquet",
            "execution": "fills.parquet",
            "performance": "analytics/nav.parquet",
        }
        relative = paths.get(artifact_type)
        if relative is None:
            raise ValueError(f"unsupported research artifact type: {artifact_type}")
        run = self._registry.get_run(run_id)
        if run.manifest_path is None or run.manifest_hash is None:
            raise ValueError("research run has no verified manifest")
        manifest_path = Path(run.manifest_path).resolve()
        if not manifest_path.is_relative_to(self._artifact_root):
            raise ValueError("research manifest escaped trusted artifact root")
        manifest_bytes = manifest_path.read_bytes()
        if hashlib.sha256(manifest_bytes).hexdigest() != run.manifest_hash:
            raise ValueError("research manifest identity mismatch")
        manifest = cast(dict[str, JsonValue], json.loads(manifest_bytes))
        entries = manifest.get("entries")
        if not isinstance(entries, list):
            raise TypeError("research manifest entries are invalid")
        entry = next(
            (
                item
                for item in entries
                if isinstance(item, dict) and item.get("relative_path") == relative
            ),
            None,
        )
        if entry is None:
            return {
                "run_id": run_id,
                "artifact_type": artifact_type,
                "manifest_hash": run.manifest_hash,
                "items": [],
                "page": page,
                "page_size": page_size,
                "total": 0,
            }
        target = (manifest_path.parent / relative).resolve()
        if not target.is_relative_to(manifest_path.parent) or not target.is_file():
            raise ValueError("research artifact is missing from verified run directory")
        payload = target.read_bytes()
        if (
            hashlib.sha256(payload).hexdigest() != entry.get("content_hash")
            or len(payload) != entry.get("byte_count")
        ):
            raise ValueError("research artifact identity mismatch")
        frame = pl.read_parquet(target)
        offset = (page - 1) * page_size
        records = cast(list[JsonValue], json.loads(frame.slice(offset, page_size).write_json()))
        return {
            "run_id": run_id,
            "artifact_type": artifact_type,
            "manifest_hash": run.manifest_hash,
            "items": records,
            "page": page,
            "page_size": page_size,
            "total": frame.height,
        }

    @staticmethod
    def _execution(execution: FamilyExecutionRecord) -> dict[str, JsonValue]:
        return {
            "id": execution.id,
            "status": execution.status.value,
            "catalog_hash": execution.catalog_hash,
            "source_hash": execution.source_hash,
            "rulebook_hash": execution.rulebook_hash,
            "selected_variant_id": execution.selected_variant_id,
            "selection_reason": execution.selection_reason,
            "created_at": execution.created_at.isoformat(),
            "started_at": execution.started_at.isoformat() if execution.started_at else None,
            "completed_at": execution.completed_at.isoformat() if execution.completed_at else None,
            "error": execution.error,
        }
