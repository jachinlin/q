"""使用短 SQLite 事务持久化自动研究族及其不可变运行。"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from uuid import NAMESPACE_URL, uuid4, uuid5

from sqlalchemy import Engine, delete, func, select
from sqlalchemy.orm import Session

from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.experiments.research import (
    FamilyExecutionRecord,
    ResearchFamilyRecord,
    ResearchMark,
    ResearchMetricRecord,
    ResearchPhase,
    ResearchRunRecord,
    ResearchStage,
    ResearchStatus,
    ResearchVariantRecord,
)
from quant_research.infrastructure.persistence.orm import (
    ResearchArtifactORM,
    ResearchFamilyExecutionORM,
    ResearchFamilyORM,
    ResearchMetricORM,
    ResearchRunORM,
    ResearchTagORM,
    ResearchVariantORM,
)
from quant_research.research_protocols import (
    CandidateSelection,
    ExpandedVariant,
    ResolvedResearchFamily,
)
from quant_research.research_protocols.models import ResearchMode


class ResearchRegistry:
    """提供研究族创建、展开、状态转换、选型锁定和查询。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def __init__(
        self, engine: Engine, *, clock: Callable[[], datetime] | None = None
    ) -> None:
        if not isinstance(engine, Engine):
            raise TypeError("engine must be a SQLAlchemy Engine")
        self._engine = engine
        self._clock = clock or (lambda: datetime.now(UTC))

    def create_family(
        self,
        resolved: ResolvedResearchFamily,
        *,
        catalog_hash: str,
        source_hash: str,
        lockfile_hash: str,
        rulebook_hash: str,
        environment_hash: str,
    ) -> tuple[ResearchFamilyRecord, FamilyExecutionRecord]:
        """原子创建不可变研究族及首次执行。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        now = self._now()
        family_id = str(uuid4())
        execution_id = str(uuid4())
        identities = (catalog_hash, source_hash, lockfile_hash, rulebook_hash, environment_hash)
        if any(len(value) != 64 for value in identities):
            raise ValueError("research execution identities must be SHA-256 digests")
        with Session(self._engine) as session, session.begin():
            session.add(
                ResearchFamilyORM(
                    id=family_id,
                    name=resolved.config.name,
                    hypothesis=resolved.config.hypothesis,
                    strategy_id=resolved.config.strategy_id,
                    research_mode=resolved.config.research_mode.value,
                    config_json=self._json(resolved.normalized),
                    config_hash=resolved.config_hash,
                    mark=ResearchMark.UNREVIEWED.value,
                    note=None,
                    created_at=self._timestamp(now),
                    archived_at=None,
                )
            )
            session.flush()
            session.add(
                ResearchFamilyExecutionORM(
                    id=execution_id,
                    family_id=family_id,
                    catalog_hash=catalog_hash,
                    source_hash=source_hash,
                    lockfile_hash=lockfile_hash,
                    rulebook_hash=rulebook_hash,
                    environment_hash=environment_hash,
                    status=ResearchStatus.QUEUED.value,
                    selected_variant_id=None,
                    selection_reason=None,
                    created_at=self._timestamp(now),
                    started_at=None,
                    completed_at=None,
                    error_json=None,
                )
            )
        return self.get_family(family_id), self.get_execution(execution_id)

    def create_execution(
        self,
        family_id: str,
        *,
        catalog_hash: str,
        source_hash: str,
        lockfile_hash: str,
        rulebook_hash: str,
        environment_hash: str,
    ) -> FamilyExecutionRecord:
        """为已有不可变研究族创建新的重跑执行。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        self.get_family(family_id)
        now = self._now()
        execution_id = str(uuid4())
        with Session(self._engine) as session, session.begin():
            session.add(
                ResearchFamilyExecutionORM(
                    id=execution_id,
                    family_id=family_id,
                    catalog_hash=catalog_hash,
                    source_hash=source_hash,
                    lockfile_hash=lockfile_hash,
                    rulebook_hash=rulebook_hash,
                    environment_hash=environment_hash,
                    status=ResearchStatus.QUEUED.value,
                    selected_variant_id=None,
                    selection_reason=None,
                    created_at=self._timestamp(now),
                    started_at=None,
                    completed_at=None,
                    error_json=None,
                )
            )
        return self.get_execution(execution_id)

    def expand(
        self, execution_id: str, variants: Sequence[ExpandedVariant]
    ) -> tuple[ResearchRunRecord, ...]:
        """幂等登记候选及其 TRAIN_VALIDATION 运行。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        if not variants:
            raise ValueError("research execution requires at least one variant")
        now = self._now()
        with Session(self._engine) as session, session.begin():
            execution = session.get(ResearchFamilyExecutionORM, execution_id)
            if execution is None:
                raise KeyError(f"research execution not found: {execution_id}")
            existing = session.scalar(
                select(func.count()).select_from(ResearchVariantORM).where(
                    ResearchVariantORM.execution_id == execution_id
                )
            )
            if existing:
                return self._runs_in_session(session, execution_id)
            for ordinal, variant in enumerate(variants):
                variant_id = str(uuid5(NAMESPACE_URL, f"{execution_id}:{variant.composition_hash}"))
                run_id = str(uuid5(NAMESPACE_URL, f"{variant_id}:{ResearchPhase.TRAIN_VALIDATION.value}"))
                session.add(
                    ResearchVariantORM(
                        id=variant_id,
                        execution_id=execution_id,
                        ordinal=ordinal,
                        composition_hash=variant.composition_hash,
                        parameters_json=self._json(variant.parameters),
                        config_json=self._json(variant.config),
                        rejection_reasons_json="[]",
                        created_at=self._timestamp(now),
                    )
                )
                session.flush()
                session.add(
                    ResearchRunORM(
                        id=run_id,
                        execution_id=execution_id,
                        variant_id=variant_id,
                        phase=ResearchPhase.TRAIN_VALIDATION.value,
                        status=ResearchStatus.QUEUED.value,
                        stage=ResearchStage.VALIDATE.value,
                        stage_status_json="{}",
                        manifest_path=None,
                        manifest_hash=None,
                        created_at=self._timestamp(now),
                        started_at=None,
                        completed_at=None,
                        error_json=None,
                    )
                )
            execution.status = ResearchStatus.RUNNING.value
            execution.started_at = self._timestamp(now)
        return self.list_runs(execution_id)

    def create_test_run(
        self, execution_id: str, variant_id: str, reason: str
    ) -> ResearchRunRecord:
        """原子锁定选中候选并创建唯一 TEST 运行。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        now = self._now()
        run_id = str(uuid5(NAMESPACE_URL, f"{variant_id}:{ResearchPhase.TEST.value}"))
        with Session(self._engine) as session, session.begin():
            execution = session.get(ResearchFamilyExecutionORM, execution_id)
            variant = session.get(ResearchVariantORM, variant_id)
            if execution is None or variant is None or variant.execution_id != execution_id:
                raise KeyError("research execution or selected variant not found")
            if execution.selected_variant_id not in (None, variant_id):
                raise ValueError("research selection is already locked")
            execution.selected_variant_id = variant_id
            execution.selection_reason = reason
            existing = session.get(ResearchRunORM, run_id)
            if existing is None:
                session.add(
                    ResearchRunORM(
                        id=run_id,
                        execution_id=execution_id,
                        variant_id=variant_id,
                        phase=ResearchPhase.TEST.value,
                        status=ResearchStatus.QUEUED.value,
                        stage=ResearchStage.VALIDATE.value,
                        stage_status_json="{}",
                        manifest_path=None,
                        manifest_hash=None,
                        created_at=self._timestamp(now),
                        started_at=None,
                        completed_at=None,
                        error_json=None,
                    )
                )
        return self.get_run(run_id)

    def update_run(
        self,
        run_id: str,
        *,
        status: ResearchStatus,
        stage: ResearchStage,
        stage_status: Mapping[str, JsonValue],
        manifest_path: str | None = None,
        manifest_hash: str | None = None,
        error: Mapping[str, JsonValue] | None = None,
    ) -> ResearchRunRecord:
        """更新运行快照；成功状态必须携带已验证 Manifest。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        now = self._now()
        if status is ResearchStatus.SUCCEEDED and (manifest_path is None or manifest_hash is None):
            raise ValueError("successful research run requires a verified manifest")
        with Session(self._engine) as session, session.begin():
            run = session.get(ResearchRunORM, run_id)
            if run is None:
                raise KeyError(f"research run not found: {run_id}")
            run.status = status.value
            run.stage = stage.value
            run.stage_status_json = self._json(stage_status)
            run.started_at = run.started_at or self._timestamp(now)
            run.completed_at = self._timestamp(now) if status in {
                ResearchStatus.SUCCEEDED,
                ResearchStatus.FAILED,
                ResearchStatus.CANCELLED,
            } else None
            run.manifest_path = manifest_path
            run.manifest_hash = manifest_hash
            run.error_json = self._json(error) if error is not None else None
        return self.get_run(run_id)

    def register_metrics(
        self,
        run_id: str,
        metrics: Sequence[ResearchMetricRecord],
    ) -> None:
        """一次登记运行的分区指标，禁止覆盖同名指标。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        with Session(self._engine) as session, session.begin():
            if session.get(ResearchRunORM, run_id) is None:
                raise KeyError(f"research run not found: {run_id}")
            for item in metrics:
                if item.run_id != run_id:
                    raise ValueError("metric run_id does not match target run")
                session.add(
                    ResearchMetricORM(
                        run_id=run_id,
                        split=item.split,
                        category=item.category,
                        name=item.name,
                        value=item.value,
                        unit=item.unit,
                        p_value=item.p_value,
                        adjusted_p_value=item.adjusted_p_value,
                    )
                )

    def register_run_artifacts(
        self,
        run_id: str,
        *,
        manifest_path: str,
        manifest_hash: str,
    ) -> None:
        """从已验证 Manifest 登记运行产物和完整输入身份。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        path = Path(manifest_path).resolve()
        payload = path.read_bytes()
        if hashlib.sha256(payload).hexdigest() != manifest_hash:
            raise ValueError("research manifest identity mismatch during registration")
        manifest = cast(dict[str, JsonValue], json.loads(payload))
        entries = manifest.get("entries")
        identity = manifest.get("identity")
        if not isinstance(entries, list) or not isinstance(identity, dict):
            raise TypeError("research manifest structure is invalid")
        now = self._now()
        with Session(self._engine) as session, session.begin():
            run = session.get(ResearchRunORM, run_id)
            if run is None:
                raise KeyError(f"research run not found: {run_id}")
            execution = session.get(ResearchFamilyExecutionORM, run.execution_id)
            if execution is None:
                raise KeyError(f"research execution not found: {run.execution_id}")
            expected_identity = {
                "execution_id": execution.id,
                "run_id": run.id,
                "variant_id": run.variant_id,
                "catalog_hash": execution.catalog_hash,
            }
            if any(identity.get(key) != value for key, value in expected_identity.items()):
                raise ValueError("research manifest input identity mismatch")
            existing = session.scalar(
                select(func.count())
                .select_from(ResearchArtifactORM)
                .where(ResearchArtifactORM.run_id == run_id)
            )
            if existing:
                raise ValueError("research run artifacts are already registered")
            for raw in entries:
                if not isinstance(raw, dict):
                    raise TypeError("research manifest entry is invalid")
                relative = raw.get("relative_path")
                content_hash = raw.get("content_hash")
                byte_count = raw.get("byte_count")
                artifact_type = raw.get("artifact_type")
                producer = raw.get("producer_component_id")
                row_count = raw.get("row_count")
                if (
                    not isinstance(relative, str)
                    or not isinstance(content_hash, str)
                    or not isinstance(byte_count, int)
                    or not isinstance(artifact_type, str)
                    or not isinstance(producer, str)
                    or (row_count is not None and not isinstance(row_count, int))
                ):
                    raise TypeError("research manifest entry fields are invalid")
                session.add(
                    ResearchArtifactORM(
                        execution_id=execution.id,
                        run_id=run.id,
                        relative_path=f"runs/{run.id}/{relative}",
                        artifact_type=artifact_type,
                        producer_component_id=producer,
                        content_hash=content_hash,
                        byte_count=byte_count,
                        row_count=row_count,
                        metadata_json=self._json(raw),
                        created_at=self._timestamp(now),
                    )
                )

    def complete_execution(
        self, execution_id: str, status: ResearchStatus, error: Mapping[str, JsonValue] | None = None
    ) -> FamilyExecutionRecord:
        """在 TEST 或受控失败后把执行置为终态。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        if status not in {ResearchStatus.SUCCEEDED, ResearchStatus.FAILED, ResearchStatus.CANCELLED}:
            raise ValueError("execution completion requires a terminal status")
        with Session(self._engine) as session, session.begin():
            execution = session.get(ResearchFamilyExecutionORM, execution_id)
            if execution is None:
                raise KeyError(f"research execution not found: {execution_id}")
            execution.status = status.value
            execution.completed_at = self._timestamp(self._now())
            execution.error_json = self._json(error) if error is not None else None
        return self.get_execution(execution_id)

    def record_selection_evidence(
        self,
        execution_id: str,
        selection: CandidateSelection,
        *,
        artifact_hash: str,
        artifact_bytes: int,
    ) -> None:
        """原子登记候选拒绝原因、校正 p-value 和选择产物。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        if len(artifact_hash) != 64 or artifact_bytes < 1:
            raise ValueError("selection artifact identity is invalid")
        now = self._now()
        with Session(self._engine) as session, session.begin():
            execution = session.get(ResearchFamilyExecutionORM, execution_id)
            if execution is None:
                raise KeyError(f"research execution not found: {execution_id}")
            variants = session.scalars(
                select(ResearchVariantORM).where(
                    ResearchVariantORM.execution_id == execution_id
                )
            ).all()
            known = {item.id for item in variants}
            if selection.selected_variant_id not in known:
                raise ValueError("selected variant does not belong to execution")
            for variant in variants:
                variant.rejection_reasons_json = self._json(
                    list(selection.rejected.get(variant.id, ()))
                )
            runs = session.scalars(
                select(ResearchRunORM).where(
                    ResearchRunORM.execution_id == execution_id,
                    ResearchRunORM.phase == ResearchPhase.TRAIN_VALIDATION.value,
                )
            ).all()
            run_by_variant = {item.variant_id: item.id for item in runs}
            for variant_id, adjusted in selection.adjusted_p_values.items():
                run_id = run_by_variant.get(variant_id)
                if run_id is None:
                    raise ValueError("selection p-value references unknown variant")
                metrics = session.scalars(
                    select(ResearchMetricORM).where(
                        ResearchMetricORM.run_id == run_id,
                        ResearchMetricORM.split == "VALIDATION",
                    )
                ).all()
                for metric in metrics:
                    if metric.p_value is not None:
                        metric.adjusted_p_value = adjusted
            session.add(
                ResearchArtifactORM(
                    execution_id=execution_id,
                    run_id=None,
                    relative_path="selection.json",
                    artifact_type="SELECTION",
                    producer_component_id="research_selector",
                    content_hash=artifact_hash,
                    byte_count=artifact_bytes,
                    row_count=None,
                    metadata_json=self._json(
                        {
                            "selected_variant_id": selection.selected_variant_id,
                            "reason": selection.reason,
                        }
                    ),
                    created_at=self._timestamp(now),
                )
            )

    def get_family(self, family_id: str) -> ResearchFamilyRecord:
        """读取一个研究族。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        with Session(self._engine) as session:
            row = session.get(ResearchFamilyORM, family_id)
            if row is None:
                raise KeyError(f"research family not found: {family_id}")
            return self._family(row)

    def update_family_research(
        self,
        family_id: str,
        *,
        mark: ResearchMark,
        note: str | None,
        tags: Sequence[str],
    ) -> ResearchFamilyRecord:
        """更新研究者标记、结论与标签，不改变不可变研究定义。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        normalized_tags = tuple(sorted({item.strip() for item in tags if item.strip()}))
        if len(normalized_tags) > 32 or any(len(item) > 64 for item in normalized_tags):
            raise ValueError("research tags exceed count or length limit")
        normalized_note = None if note is None else note.strip()
        if normalized_note is not None and len(normalized_note) > 20_000:
            raise ValueError("research note exceeds length limit")
        with Session(self._engine) as session, session.begin():
            family = session.get(ResearchFamilyORM, family_id)
            if family is None:
                raise KeyError(f"research family not found: {family_id}")
            family.mark = mark.value
            family.note = normalized_note or None
            session.execute(
                delete(ResearchTagORM).where(ResearchTagORM.family_id == family_id)
            )
            session.add_all(
                ResearchTagORM(family_id=family_id, tag=tag)
                for tag in normalized_tags
            )
        return self.get_family(family_id)

    def list_tags(self, family_id: str) -> tuple[str, ...]:
        """稳定读取研究族标签。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        self.get_family(family_id)
        with Session(self._engine) as session:
            return tuple(
                session.scalars(
                    select(ResearchTagORM.tag)
                    .where(ResearchTagORM.family_id == family_id)
                    .order_by(ResearchTagORM.tag)
                ).all()
            )

    def list_families(self, *, limit: int = 100, offset: int = 0) -> tuple[ResearchFamilyRecord, ...]:
        """按创建时间倒序列出未归档研究族。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        with Session(self._engine) as session:
            rows = session.scalars(
                select(ResearchFamilyORM)
                .where(ResearchFamilyORM.archived_at.is_(None))
                .order_by(ResearchFamilyORM.created_at.desc(), ResearchFamilyORM.id)
                .limit(limit)
                .offset(offset)
            ).all()
            return tuple(self._family(row) for row in rows)

    def get_execution(self, execution_id: str) -> FamilyExecutionRecord:
        """读取一次研究族执行。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        with Session(self._engine) as session:
            row = session.get(ResearchFamilyExecutionORM, execution_id)
            if row is None:
                raise KeyError(f"research execution not found: {execution_id}")
            return self._execution(row)

    def list_executions(self, family_id: str) -> tuple[FamilyExecutionRecord, ...]:
        """按创建顺序列出研究族全部执行。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        with Session(self._engine) as session:
            rows = session.scalars(
                select(ResearchFamilyExecutionORM)
                .where(ResearchFamilyExecutionORM.family_id == family_id)
                .order_by(ResearchFamilyExecutionORM.created_at, ResearchFamilyExecutionORM.id)
            ).all()
            return tuple(self._execution(row) for row in rows)

    def list_variants(self, execution_id: str) -> tuple[ResearchVariantRecord, ...]:
        """按展开序号列出候选。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        with Session(self._engine) as session:
            rows = session.scalars(
                select(ResearchVariantORM)
                .where(ResearchVariantORM.execution_id == execution_id)
                .order_by(ResearchVariantORM.ordinal)
            ).all()
            return tuple(self._variant(row) for row in rows)

    def get_run(self, run_id: str) -> ResearchRunRecord:
        """读取一个研究运行。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        with Session(self._engine) as session:
            row = session.get(ResearchRunORM, run_id)
            if row is None:
                raise KeyError(f"research run not found: {run_id}")
            return self._run(row)

    def list_runs(self, execution_id: str) -> tuple[ResearchRunRecord, ...]:
        """按候选和阶段稳定列出执行内运行。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        with Session(self._engine) as session:
            return self._runs_in_session(session, execution_id)

    def list_metrics(self, run_id: str) -> tuple[ResearchMetricRecord, ...]:
        """按分区、类别和名称读取运行指标。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        with Session(self._engine) as session:
            rows = session.scalars(
                select(ResearchMetricORM)
                .where(ResearchMetricORM.run_id == run_id)
                .order_by(ResearchMetricORM.split, ResearchMetricORM.category, ResearchMetricORM.name)
            ).all()
            return tuple(
                ResearchMetricRecord(
                    run_id=row.run_id,
                    split=row.split,
                    category=row.category,
                    name=row.name,
                    value=row.value,
                    unit=row.unit,
                    p_value=row.p_value,
                    adjusted_p_value=row.adjusted_p_value,
                )
                for row in rows
            )

    def _runs_in_session(self, session: Session, execution_id: str) -> tuple[ResearchRunRecord, ...]:
        rows = session.scalars(
            select(ResearchRunORM)
            .where(ResearchRunORM.execution_id == execution_id)
            .order_by(ResearchRunORM.created_at, ResearchRunORM.variant_id, ResearchRunORM.phase)
        ).all()
        return tuple(self._run(row) for row in rows)

    @staticmethod
    def _family(row: ResearchFamilyORM) -> ResearchFamilyRecord:
        return ResearchFamilyRecord(
            id=row.id,
            name=row.name,
            hypothesis=row.hypothesis,
            strategy_id=row.strategy_id,
            research_mode=ResearchMode(row.research_mode),
            config=cast(dict[str, JsonValue], json.loads(row.config_json)),
            config_hash=row.config_hash,
            mark=ResearchMark(row.mark),
            note=row.note,
            created_at=ResearchRegistry._datetime(row.created_at),
            archived_at=ResearchRegistry._optional_datetime(row.archived_at),
        )

    @staticmethod
    def _execution(row: ResearchFamilyExecutionORM) -> FamilyExecutionRecord:
        return FamilyExecutionRecord(
            id=row.id,
            family_id=row.family_id,
            catalog_hash=row.catalog_hash,
            source_hash=row.source_hash,
            lockfile_hash=row.lockfile_hash,
            rulebook_hash=row.rulebook_hash,
            environment_hash=row.environment_hash,
            status=ResearchStatus(row.status),
            selected_variant_id=row.selected_variant_id,
            selection_reason=row.selection_reason,
            created_at=ResearchRegistry._datetime(row.created_at),
            started_at=ResearchRegistry._optional_datetime(row.started_at),
            completed_at=ResearchRegistry._optional_datetime(row.completed_at),
            error=cast(dict[str, JsonValue] | None, json.loads(row.error_json) if row.error_json else None),
        )

    @staticmethod
    def _variant(row: ResearchVariantORM) -> ResearchVariantRecord:
        return ResearchVariantRecord(
            id=row.id,
            execution_id=row.execution_id,
            ordinal=row.ordinal,
            composition_hash=row.composition_hash,
            parameters=cast(dict[str, JsonValue], json.loads(row.parameters_json)),
            config=cast(dict[str, JsonValue], json.loads(row.config_json)),
            rejection_reasons=tuple(cast(list[str], json.loads(row.rejection_reasons_json))),
            created_at=ResearchRegistry._datetime(row.created_at),
        )

    @staticmethod
    def _run(row: ResearchRunORM) -> ResearchRunRecord:
        return ResearchRunRecord(
            id=row.id,
            execution_id=row.execution_id,
            variant_id=row.variant_id,
            phase=ResearchPhase(row.phase),
            status=ResearchStatus(row.status),
            stage=ResearchStage(row.stage),
            stage_status=cast(dict[str, JsonValue], json.loads(row.stage_status_json)),
            manifest_path=row.manifest_path,
            manifest_hash=row.manifest_hash,
            created_at=ResearchRegistry._datetime(row.created_at),
            started_at=ResearchRegistry._optional_datetime(row.started_at),
            completed_at=ResearchRegistry._optional_datetime(row.completed_at),
            error=cast(dict[str, JsonValue] | None, json.loads(row.error_json) if row.error_json else None),
        )

    @staticmethod
    def _json(value: object) -> str:
        return canonical_json_bytes(cast(JsonValue, value)).decode("utf-8")

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("research registry clock must return timezone-aware datetime")
        return value.astimezone(UTC)

    @staticmethod
    def _timestamp(value: datetime) -> str:
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")

    @staticmethod
    def _datetime(value: str) -> datetime:
        return datetime.fromisoformat(value).astimezone(UTC)

    @staticmethod
    def _optional_datetime(value: str | None) -> datetime | None:
        return None if value is None else ResearchRegistry._datetime(value)
