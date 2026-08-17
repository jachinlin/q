"""通过 SQLite 登记独立因子研究及其不可变运行。"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from uuid import uuid4

from sqlalchemy import Engine, func, select, update
from sqlalchemy.orm import Session

from quant_research.data.contracts import canonical_json_bytes
from quant_research.factor_studies.models import FactorRunStatus, FactorStudyConfig
from quant_research.infrastructure.persistence.orm import FactorRunORM, FactorStudyORM


class FactorStudyRepository:
    """提供因子研究和运行的事务性持久化边界。

    入参：
        engine：引擎。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    def __init__(self, engine: Engine) -> None:
        """使用指定 SQLAlchemy Engine 创建仓库。"""
        self._engine = engine

    def create_study(self, name: str, config: FactorStudyConfig) -> str:
        """创建研究并返回新研究 ID；名称非法时抛出 ``ValueError``。

        入参：
            name：供用户识别研究、任务或数据对象的非空名称。
            config：调用所用的配置对象，类型为 ``FactorStudyConfig``。
        返回值：
            返回创建因子研究后的因子研究（``str``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        title = name.strip()
        if not title or len(title) > 128:
            raise ValueError("study name must contain 1 through 128 characters")
        payload = config.model_dump(mode="json")
        encoded = canonical_json_bytes(payload)
        study_id = str(uuid4())
        with Session(self._engine) as session, session.begin():
            session.add(
                FactorStudyORM(
                    id=study_id,
                    name=title,
                    config_json=encoded.decode("utf-8"),
                    config_hash=hashlib.sha256(encoded).hexdigest(),
                    created_at=self._now(),
                )
            )
        return study_id

    def create_run(self, study_id: str, catalog_hash: str, source_hash: str) -> str:
        """基于研究配置和当前身份创建不可变运行草稿。

        入参：
            study_id：因子研究定义的 UUID 标识。
            catalog_hash：提交时捕获并在运行阶段防漂移校验的 Canonical 数据目录身份。
            source_hash：参与计算的实现源码身份。
        返回值：
            返回创建运行后的运行（``str``）。
        异常：
            无。
        """
        study = self.get_study(study_id)
        run_id = str(uuid4())
        with Session(self._engine) as session, session.begin():
            session.add(
                FactorRunORM(
                    id=run_id,
                    study_id=study_id,
                    task_id=None,
                    config_json=json.dumps(
                        study["config"], sort_keys=True, separators=(",", ":")
                    ),
                    config_hash=cast(str, study["config_hash"]),
                    catalog_hash=catalog_hash,
                    source_hash=source_hash,
                    status=FactorRunStatus.CREATED.value,
                    manifest_path=None,
                    manifest_hash=None,
                    error_json=None,
                    created_at=self._now(),
                    started_at=None,
                    completed_at=None,
                )
            )
        return run_id

    def bind_task(self, run_id: str, task_id: str) -> None:
        """将新运行绑定到任务并原子转换为 ``QUEUED``。

        入参：
            run_id：一次因子研究运行的 UUID 标识。
            task_id：目标任务标识，类型为 ``str``。
        返回值：
            无。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        with Session(self._engine) as session, session.begin():
            result = session.execute(
                update(FactorRunORM)
                .where(
                    FactorRunORM.id == run_id,
                    FactorRunORM.status == FactorRunStatus.CREATED.value,
                )
                .values(task_id=task_id, status=FactorRunStatus.QUEUED.value)
            )
            if getattr(result, "rowcount", None) != 1:
                raise ValueError("factor run cannot be queued from its current state")

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
        """以乐观状态前置条件转换运行状态并记录终态元数据。

        入参：
            run_id：一次因子研究运行的 UUID 标识。
            expected：``expected``。
            target：目标组合。
            manifest_path：记录文件身份、Schema、行数和输入身份的清单路径。
            manifest_hash：参与幂等、漂移或完整性校验的产物清单哈希；使用 SHA-256 十六进制文本。
            error：需要处理或传播的异常，类型为 ``dict[str, object] | None``。
        返回值：
            无。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        values: dict[str, object] = {"status": target.value}
        if target is FactorRunStatus.RUNNING:
            values["started_at"] = self._now()
        if target in {
            FactorRunStatus.SUCCEEDED,
            FactorRunStatus.FAILED,
            FactorRunStatus.CANCELLED,
        }:
            values["completed_at"] = self._now()
        if manifest_path is not None:
            values["manifest_path"] = str(manifest_path)
            values["manifest_hash"] = manifest_hash
        if error is not None:
            values["error_json"] = json.dumps(
                error, sort_keys=True, separators=(",", ":")
            )
        with Session(self._engine) as session, session.begin():
            result = session.execute(
                update(FactorRunORM)
                .where(FactorRunORM.id == run_id, FactorRunORM.status == expected.value)
                .values(**values)
            )
            if getattr(result, "rowcount", None) != 1:
                raise ValueError("factor run state transition conflicted")

    def get_study(self, study_id: str) -> dict[str, object]:
        """返回研究及按创建时间倒序排列的全部运行。

        入参：
            study_id：因子研究定义的 UUID 标识。
        返回值：
            返回读取因子研究后的因子研究（``dict[str, object]``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        with Session(self._engine) as session:
            row = session.get(FactorStudyORM, study_id)
            if row is None:
                raise ValueError("factor study does not exist")
            runs = session.scalars(
                select(FactorRunORM)
                .where(FactorRunORM.study_id == study_id)
                .order_by(FactorRunORM.created_at.desc())
            ).all()
            return {
                "id": row.id,
                "name": row.name,
                "config": json.loads(row.config_json),
                "config_hash": row.config_hash,
                "created_at": row.created_at,
                "runs": [self._run(item) for item in runs],
            }

    def get_run(self, run_id: str) -> dict[str, object]:
        """按 ID 返回运行；运行不存在时抛出 ``ValueError``。

        入参：
            run_id：一次因子研究运行的 UUID 标识。
        返回值：
            返回读取运行后的运行（``dict[str, object]``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        with Session(self._engine) as session:
            row = session.get(FactorRunORM, run_id)
            if row is None:
                raise ValueError("factor run does not exist")
            return self._run(row)

    def get_run_by_task(self, task_id: str) -> dict[str, object]:
        """按绑定任务返回运行；任务未绑定时抛出 ``ValueError``。

        入参：
            task_id：目标任务标识，类型为 ``str``。
        返回值：
            返回读取运行``by``任务后的运行``by``任务（``dict[str, object]``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        with Session(self._engine) as session:
            row = session.scalar(
                select(FactorRunORM).where(FactorRunORM.task_id == task_id)
            )
            if row is None:
                raise ValueError("factor task is not bound to a run")
            return self._run(row)

    def list_studies(self, page: int, page_size: int) -> dict[str, object]:
        """分页列出研究及各研究的最新运行。

        入参：
            page：页码。
            page_size：页码字节数。
        返回值：
            返回按确定性顺序列出``studies``后的``studies``（``dict[str, object]``）。
        异常：
            无。
        """
        offset = (page - 1) * page_size
        with Session(self._engine) as session:
            total = int(
                session.scalar(select(func.count()).select_from(FactorStudyORM)) or 0
            )
            rows = session.scalars(
                select(FactorStudyORM)
                .order_by(FactorStudyORM.created_at.desc())
                .limit(page_size)
                .offset(offset)
            ).all()
            items = []
            for row in rows:
                latest = session.scalar(
                    select(FactorRunORM)
                    .where(FactorRunORM.study_id == row.id)
                    .order_by(FactorRunORM.created_at.desc())
                    .limit(1)
                )
                items.append(
                    {
                        "id": row.id,
                        "name": row.name,
                        "config": json.loads(row.config_json),
                        "config_hash": row.config_hash,
                        "created_at": row.created_at,
                        "latest_run": None if latest is None else self._run(latest),
                    }
                )
        return {"items": items, "page": page, "page_size": page_size, "total": total}

    @staticmethod
    def _run(row: FactorRunORM) -> dict[str, object]:
        return {
            "id": row.id,
            "study_id": row.study_id,
            "task_id": row.task_id,
            "config": json.loads(row.config_json),
            "config_hash": row.config_hash,
            "catalog_hash": row.catalog_hash,
            "source_hash": row.source_hash,
            "status": row.status,
            "manifest_hash": row.manifest_hash,
            "error": None if row.error_json is None else json.loads(row.error_json),
            "created_at": row.created_at,
            "started_at": row.started_at,
            "completed_at": row.completed_at,
        }

    @staticmethod
    def _now() -> str:
        return datetime.now(UTC).isoformat()
