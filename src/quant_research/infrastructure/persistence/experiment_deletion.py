"""实现 SQLite 实验登记与受控文件资源的一致删除。"""

from __future__ import annotations

import json
import os
import shutil
import stat
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from uuid import UUID

from sqlalchemy import Connection, Engine, delete, func, insert, select

from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.experiments.models import ExperimentStatus
from quant_research.experiments.registry import (
    ExperimentDeletionConflict,
    ExperimentDeletionFailure,
    ExperimentNotFound,
)
from quant_research.infrastructure.persistence.orm import (
    AuditEventORM,
    ExperimentArtifactORM,
    ExperimentMetricORM,
    ExperimentORM,
    ExperimentTagORM,
    TaskAttemptORM,
    TaskORM,
)

_DELETABLE_STATUSES = frozenset(
    {
        ExperimentStatus.CREATED,
        ExperimentStatus.SUCCEEDED,
        ExperimentStatus.FAILED,
        ExperimentStatus.CANCELLED,
    }
)


@dataclass(frozen=True, slots=True)
class _DeletionPlan:
    experiment_id: str
    task_ids: tuple[str, ...]
    operation_root: Path


class SqliteExperimentDeletion:
    """原子删除非活动实验，并安全收敛关联产物与任务日志。

    入参：
        engine：保存实验、任务和审计记录的 SQLite 引擎。
        data_root：全部受控状态和产物的共同数据根目录。
        artifact_root：实验发布产物根目录。
        task_log_root：任务诊断日志根目录。
        clock：可选的 UTC 审计时间来源。
    返回值：
        构造可注入应用层删除端口的实例。
    异常：
        根目录越界或时间来源不合法时抛出 ``ValueError``。
    """

    def __init__(
        self,
        engine: Engine,
        *,
        data_root: Path,
        artifact_root: Path,
        task_log_root: Path,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not isinstance(data_root, Path):
            raise TypeError("data_root must be a Path")
        if not isinstance(artifact_root, Path) or not isinstance(task_log_root, Path):
            raise TypeError("artifact_root and task_log_root must be Paths")
        self._engine = engine
        self._data_root = self._absolute(data_root)
        self._artifact_root = self._absolute(artifact_root)
        self._task_log_root = self._absolute(task_log_root)
        if not self._artifact_root.is_relative_to(self._data_root):
            raise ValueError("artifact_root must be inside data_root")
        if not self._task_log_root.is_relative_to(self._data_root):
            raise ValueError("task_log_root must be inside data_root")
        self._deletion_root = self._data_root / "state" / ".experiment-deletions"
        self._clock = clock or (lambda: datetime.now(UTC))

    def delete(
        self,
        experiment_id: str,
        actor: str,
        *,
        request_id: str,
    ) -> None:
        """删除一个非活动实验，并将关联文件移出所有在线目录。

        入参：
            experiment_id：目标实验 UUID。
            actor：执行删除的审计主体。
            request_id：写请求的关联标识。
        返回值：
            无。
        异常：
            实验不存在时抛出 ``ExperimentNotFound``；活动状态时抛出
            ``ExperimentDeletionConflict``；文件无法安全处理时抛出
            ``ExperimentDeletionFailure``。
        """
        identifier = self._uuid(experiment_id, "experiment_id")
        subject = self._identity(actor, "actor")
        request = self._identity(request_id, "request_id")
        self.recover()
        plan: _DeletionPlan | None = None
        with self._engine.connect() as connection:
            connection.exec_driver_sql("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    select(ExperimentORM.status).where(ExperimentORM.id == identifier)
                ).one_or_none()
                if row is None:
                    raise ExperimentNotFound(identifier)
                status = ExperimentStatus(cast(str, row.status))
                if status not in _DELETABLE_STATUSES:
                    raise ExperimentDeletionConflict(identifier, status)
                task_ids = tuple(
                    connection.scalars(
                        select(TaskORM.id)
                        .where(TaskORM.experiment_id == identifier)
                        .order_by(TaskORM.id)
                    )
                )
                plan = _DeletionPlan(
                    experiment_id=identifier,
                    task_ids=task_ids,
                    operation_root=(
                        self._deletion_root / f"experiment_id={identifier}"
                    ),
                )
                counts = self._counts(connection, identifier, task_ids)
                self._stage(plan)
                created_at = self._timestamp(self._clock())
                details: dict[str, JsonValue] = {
                    "action": "delete",
                    "actor": subject,
                    "experiment_id": identifier,
                    "request_id": request,
                    "status": status.value,
                    **counts,
                }
                connection.execute(
                    insert(AuditEventORM).values(
                        experiment_id=identifier,
                        task_id=None,
                        event_type="EXPERIMENT_DELETED",
                        actor=subject,
                        details_json=canonical_json_bytes(details).decode("utf-8"),
                        created_at=created_at,
                    )
                )
                connection.execute(
                    delete(ExperimentORM).where(ExperimentORM.id == identifier)
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                if plan is not None:
                    try:
                        self._restore(plan)
                    except OSError as error:
                        raise ExperimentDeletionFailure(
                            identifier, "rollback_restore"
                        ) from error
                raise
        if plan is not None:
            try:
                self._remove_operation(plan)
            except OSError:
                # The live resources are already gone. A later bootstrap recovery
                # retries reclaiming this private deletion staging directory.
                pass

    def recover(self) -> None:
        """恢复或清扫上次进程中断留下的实验删除暂存目录。

        入参：
            无。
        返回值：
            无。
        异常：
            暂存清单损坏、路径不可信或文件恢复失败时抛出
            ``ExperimentDeletionFailure``。
        """
        try:
            self._ensure_directory(self._deletion_root)
            for operation_root in sorted(self._deletion_root.iterdir()):
                if not operation_root.is_dir() or self._is_reparse(operation_root):
                    raise OSError("unexpected deletion staging entry")
                plan = self._read_plan(operation_root)
                with self._engine.connect() as connection:
                    exists = connection.scalar(
                        select(func.count())
                        .select_from(ExperimentORM)
                        .where(ExperimentORM.id == plan.experiment_id)
                    )
                if int(exists or 0) == 1:
                    self._restore(plan)
                else:
                    self._remove_operation(plan)
        except ExperimentDeletionFailure:
            raise
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise ExperimentDeletionFailure("unknown", "recover") from error

    def _stage(self, plan: _DeletionPlan) -> None:
        try:
            self._ensure_directory(self._deletion_root)
            plan.operation_root.mkdir(exist_ok=False)
            manifest = {
                "experiment_id": plan.experiment_id,
                "task_ids": list(plan.task_ids),
            }
            (plan.operation_root / "manifest.json").write_bytes(
                canonical_json_bytes(cast(JsonValue, manifest))
            )
            for source, staged in self._paths(plan):
                if not source.exists():
                    continue
                self._ensure_plain_tree(source)
                staged.parent.mkdir(parents=True, exist_ok=True)
                os.replace(source, staged)
        except (OSError, TypeError, ValueError) as error:
            try:
                self._restore(plan)
            except OSError as restore_error:
                raise ExperimentDeletionFailure(
                    plan.experiment_id, "stage_restore"
                ) from restore_error
            raise ExperimentDeletionFailure(plan.experiment_id, "stage") from error

    def _restore(self, plan: _DeletionPlan) -> None:
        if not plan.operation_root.exists():
            return
        for original, staged in reversed(tuple(self._paths(plan))):
            if not staged.exists():
                continue
            self._ensure_plain_tree(staged)
            if original.exists():
                raise OSError("refusing to overwrite a restored experiment resource")
            self._ensure_directory(original.parent)
            os.replace(staged, original)
        self._remove_operation(plan)

    def _remove_operation(self, plan: _DeletionPlan) -> None:
        if not plan.operation_root.exists():
            return
        self._ensure_plain_tree(plan.operation_root)
        shutil.rmtree(plan.operation_root)

    def _read_plan(self, operation_root: Path) -> _DeletionPlan:
        name = operation_root.name
        if not name.startswith("experiment_id="):
            raise ValueError("invalid deletion staging directory")
        directory_id = self._uuid(name.removeprefix("experiment_id="), "experiment_id")
        payload = json.loads((operation_root / "manifest.json").read_text("utf-8"))
        if not isinstance(payload, dict):
            raise TypeError("deletion manifest must be an object")
        experiment_id = self._uuid(payload.get("experiment_id"), "experiment_id")
        raw_task_ids = payload.get("task_ids")
        if experiment_id != directory_id or not isinstance(raw_task_ids, list):
            raise ValueError("deletion manifest identity mismatch")
        task_ids = tuple(self._uuid(value, "task_id") for value in raw_task_ids)
        if len(task_ids) != len(set(task_ids)) or list(task_ids) != sorted(task_ids):
            raise ValueError("deletion manifest task identities are not canonical")
        return _DeletionPlan(experiment_id, task_ids, operation_root)

    def _paths(self, plan: _DeletionPlan) -> Iterator[tuple[Path, Path]]:
        yield (
            self._artifact_root / f"experiment_id={plan.experiment_id}",
            plan.operation_root / "published",
        )
        yield (
            self._artifact_root
            / ".experiment-staging"
            / f"experiment_id={plan.experiment_id}",
            plan.operation_root / "staging",
        )
        for task_id in plan.task_ids:
            yield (
                self._task_log_root / f"task_id={task_id}",
                plan.operation_root / "task-logs" / f"task_id={task_id}",
            )

    def _counts(
        self,
        connection: Connection,
        experiment_id: str,
        task_ids: tuple[str, ...],
    ) -> dict[str, JsonValue]:
        models = (
            ("tag_count", ExperimentTagORM),
            ("metric_count", ExperimentMetricORM),
            ("artifact_count", ExperimentArtifactORM),
        )
        counts: dict[str, JsonValue] = {
            name: int(
                connection.scalar(
                    select(func.count())
                    .select_from(model)
                    .where(model.experiment_id == experiment_id)
                )
                or 0
            )
            for name, model in models
        }
        counts["task_count"] = len(task_ids)
        counts["attempt_count"] = (
            int(
                connection.scalar(
                    select(func.count())
                    .select_from(TaskAttemptORM)
                    .where(TaskAttemptORM.task_id.in_(task_ids))
                )
                or 0
            )
            if task_ids
            else 0
        )
        return counts

    def _ensure_directory(self, path: Path) -> None:
        if not path.is_relative_to(self._data_root):
            raise ValueError("controlled directory escaped data_root")
        path.mkdir(parents=True, exist_ok=True)
        self._ensure_plain_ancestry(path)

    def _ensure_plain_ancestry(self, path: Path) -> None:
        current = self._data_root
        self._assert_plain(current)
        for part in path.relative_to(self._data_root).parts:
            current /= part
            self._assert_plain(current)

    def _ensure_plain_tree(self, path: Path) -> None:
        absolute = self._absolute(path)
        if not absolute.is_relative_to(self._data_root):
            raise ValueError("experiment resource escaped data_root")
        self._ensure_plain_ancestry(absolute)
        if not absolute.is_dir():
            raise ValueError("experiment resource must be a directory")
        for entry in os.scandir(absolute):
            child = Path(entry.path)
            self._assert_plain(child)
            if entry.is_dir(follow_symlinks=False):
                self._ensure_plain_tree(child)

    @staticmethod
    def _absolute(path: Path) -> Path:
        return Path(os.path.abspath(path))

    @staticmethod
    def _is_reparse(path: Path) -> bool:
        if path.is_symlink():
            return True
        attributes = getattr(path.lstat(), "st_file_attributes", 0)
        marker = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
        return bool(attributes & marker)

    @classmethod
    def _assert_plain(cls, path: Path) -> None:
        if not path.exists() or cls._is_reparse(path):
            raise ValueError("controlled deletion path is missing or reparse-backed")

    @staticmethod
    def _uuid(value: object, field: str) -> str:
        if not isinstance(value, str):
            raise TypeError(f"{field} must be a UUID string")
        normalized = str(UUID(value))
        if normalized != value:
            raise ValueError(f"{field} must use canonical UUID text")
        return normalized

    @staticmethod
    def _identity(value: str, field: str) -> str:
        if not isinstance(value, str):
            raise TypeError(f"{field} must be a string")
        normalized = value.strip()
        if not normalized or len(normalized) > 128:
            raise ValueError(f"{field} must contain 1 through 128 characters")
        return normalized

    @staticmethod
    def _timestamp(value: datetime) -> str:
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise ValueError("clock must return an aware datetime")
        return value.astimezone(UTC).isoformat()
