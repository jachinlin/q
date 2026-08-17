"""提供python-module-conventions与结构化日志相关的公开模型、协议与处理流程。"""

from __future__ import annotations

import json
import math
import os
import re
import stat
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol, Self, TextIO
from uuid import UUID

from quant_research.data.contracts import JsonValue
from quant_research.domain.enums import Severity
from quant_research.domain.errors import ErrorDetail, QuantError

_CORE_FIELDS = (
    "timestamp",
    "level",
    "event",
)
_OPTIONAL_FIELDS = (
    "request_id",
    "experiment_id",
    "task_id",
    "attempt_id",
    "worker_id",
    "stage",
    "error_code",
)
_SECRET_KEY_MARKERS = (
    "token",
    "password",
    "secret",
    "api_key",
    "apikey",
    "authorization",
    "cookie",
    "credential",
)
_SECRET_CONTAINER_KEYS = frozenset({"env", "environ", "environment"})
_REDACTED = "[REDACTED]"
_MAX_DEPTH = 8
_MAX_NODES = 512
_MIN_GLOBAL_SECRET_LENGTH = 8
MAX_TASK_LOG_BYTES = 16 * 1024 * 1024


class _TaskLogContentUnavailable(ValueError):
    """标记路径与关联字段可信但内容无法作为结构化诊断日志使用。"""


class TextLogStream(Protocol):
    """定义 ``TextLogStream`` 的依赖端口与实现契约。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        由具体实现按接口契约定义。
    Small explicit sink boundary used by ``StructuredLogger``.
    """

    def write(self, value: str) -> object:
        """写入量化研究。

        入参：
            value：待校验或转换的值，类型为 ``str``。
        返回值：
            返回``write``（``object``）。
        异常：
            由具体实现按接口契约定义。
        """
        ...

    def flush(self) -> object:
        """刷新日志流中的缓冲数据量化研究。

        入参：
            无。
        返回值：
            返回``flush``（``object``）。
        异常：
            由具体实现按接口契约定义。
        """
        ...


class StructuredLogWriteError(QuantError):
    """表示 ``StructuredLogWriteError`` 对应的领域异常。

    入参：
        operation：数据操作。
        error：需要处理或传播的异常，类型为 ``Exception``。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    A controlled task log could not be opened or materialized safely.
    """

    def __init__(self, operation: str, error: Exception) -> None:
        super().__init__(
            ErrorDetail(
                code="LOG_WRITE_FAILED",
                severity=Severity.FATAL,
                message="structured log persistence failed",
                context={
                    "error_type": type(error).__name__,
                    "operation": operation,
                },
                remediation="inspect the controlled log sink and retry safely",
                retryable=False,
            )
        )


@dataclass(frozen=True, slots=True)
class LogContext:
    """绑定一次命令、任务或实验运行共享的阶段与关联标识。

    入参：
        request_id：用于关联一次跨边界调用及其日志的请求标识。
        experiment_id：目标实验标识，类型为 ``str | None``。
        task_id：目标任务标识，类型为 ``str | None``。
        attempt_id：一次任务执行尝试的 UUID 标识。
        worker_id：当前 Worker 实例的稳定所有者标识。
        stage：执行阶段。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    Identifiers shared by every record from one execution boundary.
    """

    request_id: str | None = None
    experiment_id: str | None = None
    task_id: str | None = None
    attempt_id: str | None = None
    worker_id: str | None = None
    stage: str | None = None


class StructuredLogger:
    """将脱敏后的结构化事件按稳定 JSON Lines 契约写入日志流。

    入参：
        stream：文本日志流。
        context：本次调用的上下文，类型为 ``LogContext | None``。
        sensitive_values：参与本次处理的敏感值数值表；调用方不得依赖未声明的顺序。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    Best-effort write one stable, redacted JSON object per explicit call.
    """

    def __init__(
        self,
        stream: TextLogStream,
        *,
        context: LogContext | None = None,
        sensitive_values: Sequence[str] = (),
    ) -> None:
        if not callable(getattr(stream, "write", None)) or not callable(
            getattr(stream, "flush", None)
        ):
            raise TypeError("stream must provide write() and flush()")
        if context is not None and not isinstance(context, LogContext):
            raise TypeError("context must be a LogContext or None")
        if isinstance(sensitive_values, (str, bytes)) or not isinstance(
            sensitive_values, Sequence
        ):
            raise TypeError("sensitive_values must be a sequence of strings")
        if any(not isinstance(value, str) for value in sensitive_values):
            raise TypeError("sensitive_values must contain strings")
        self._stream = stream
        self._context = context or LogContext()
        self._sensitive_values = tuple(
            sorted(
                {
                    value
                    for value in sensitive_values
                    if len(value) >= _MIN_GLOBAL_SECRET_LENGTH
                },
                key=len,
                reverse=True,
            )
        )
        self._lock = threading.Lock()

    def emit(
        self,
        level: str,
        event: str,
        *,
        context: Mapping[str, object] | None = None,
        error_code: str | None = None,
        stage: str | None = None,
    ) -> None:
        """脱敏并写入量化研究。

        入参：
            level：日志级别。
            event：事件。
            context：本次调用的上下文，类型为 ``Mapping[str, object] | None``。
            error_code：错误代码。
            stage：执行阶段。
        返回值：
            无。
        异常：
            输入违反日志结构契约时抛出 ``TypeError``、``ValueError``。
        Redact and append one buffered JSON Lines record without flushing.
        """
        normalized_level = _LoggingSupport._nonempty_text(level, "level").upper()
        normalized_event = _LoggingSupport._nonempty_text(event, "event")
        if context is not None and not isinstance(context, Mapping):
            raise TypeError("context must be a mapping or None")
        if error_code is not None:
            error_code = _LoggingSupport._nonempty_text(error_code, "error_code")
        if stage is not None:
            stage = _LoggingSupport._nonempty_text(stage, "stage")
        budget = _RedactionBudget()
        redacted_context = (
            _LoggingSupport._redact(
                context,
                sensitive_values=self._sensitive_values,
                budget=budget,
                depth=0,
                key=None,
            )
            if context is not None
            else None
        )
        record: dict[str, JsonValue] = {
            "timestamp": _LoggingSupport._local_timestamp(),
            "level": normalized_level,
            "event": normalized_event,
        }
        optional_values = {
            "request_id": self._context.request_id,
            "experiment_id": self._context.experiment_id,
            "task_id": self._context.task_id,
            "attempt_id": self._context.attempt_id,
            "worker_id": self._context.worker_id,
            "stage": stage if stage is not None else self._context.stage,
            "error_code": error_code,
        }
        record.update(
            {key: value for key, value in optional_values.items() if value is not None}
        )
        if redacted_context is not None:
            record["context"] = redacted_context
        line = (
            json.dumps(
                record,
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        )
        with self._lock:
            try:
                self._stream.write(line)
            except Exception:  # noqa: BLE001 - logging is explicitly best effort.
                return

    def flush(self) -> None:
        """尽力刷新当前日志 sink 的缓冲区。

        入参：
            无。
        返回值：
            无。
        异常：
            无主动抛出的异常；sink 刷新失败按最佳努力日志策略忽略。
        Callers use this method only at explicit lifecycle boundaries.
        """
        with self._lock:
            try:
                self._stream.flush()
            except Exception:  # noqa: BLE001 - logging is explicitly best effort.
                return


class TeeLogStream:
    """把每条日志独立写入主文件，并尽力复制到终端流。

    入参：
        primary：主日志文件流。
        secondary：终端副本流。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    Buffer each record independently to the primary file and secondary terminal.
    """

    def __init__(
        self,
        primary: TextLogStream,
        secondary: TextLogStream,
    ) -> None:
        for label, stream in (("primary", primary), ("secondary", secondary)):
            if not callable(getattr(stream, "write", None)) or not callable(
                getattr(stream, "flush", None)
            ):
                raise TypeError(f"{label} stream must provide write() and flush()")
        self._primary = primary
        self._secondary = secondary

    def write(self, value: str) -> object:
        """写入量化研究。

        入参：
            value：待校验或转换的值，类型为 ``str``。
        返回值：
            返回``write``（``object``）。
        异常：
            无主动抛出的异常；两个 sink 的写入失败均按最佳努力策略忽略。
        """
        written: object = None
        try:
            written = self._primary.write(value)
        except Exception:  # noqa: BLE001, S110 - explicitly best effort.
            pass
        try:
            self._secondary.write(value)
        except Exception:  # noqa: BLE001, S110 - explicitly best effort.
            pass
        return written

    def flush(self) -> object:
        """刷新日志流中的缓冲数据量化研究。

        入参：
            无。
        返回值：
            返回``flush``（``object``）。
        异常：
            无主动抛出的异常；两个 sink 的刷新失败均按最佳努力策略忽略。
        """
        flushed: object = None
        try:
            flushed = self._primary.flush()
        except Exception:  # noqa: BLE001, S110 - explicitly best effort.
            pass
        try:
            self._secondary.flush()
        except Exception:  # noqa: BLE001, S110 - explicitly best effort.
            pass
        return flushed


@dataclass(slots=True)
class TaskLogSession:
    """独占持有一次任务尝试的诊断日志流并负责关闭与刷新。

    入参：
        path：待处理的文件系统路径，类型为 ``Path``。
        logger：接收结构化事件的日志器，类型为 ``StructuredLogger``。
        _stream：文本日志流。
        _on_close：由组合根注入、用于隔离外部副作用的``on``收盘价端口。
        _closed：控制是否启用``closed``规则的布尔开关。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Manage one exclusive diagnostic stream for one task attempt.
    """

    path: Path
    logger: StructuredLogger
    _stream: _DurableTextStream
    _on_close: Callable[[], None]
    _closed: bool = False

    def __enter__(self) -> Self:
        """进入上下文并返回受管资源。

        入参：
            无。
        返回值：
            返回``enter``（``Self``）。
        异常：
            无。
        """
        return self

    def __exit__(
        self,
        _error_type: type[BaseException] | None,
        _error: BaseException | None,
        _traceback: object,
    ) -> None:
        """退出上下文并释放受管资源。

        入参：
            _error_type：错误类型。
            _error：错误。
            _traceback：异常回溯对象。
        返回值：
            无。
        异常：
            无。
        """
        self.close()

    def flush(self) -> None:
        """刷新日志流中的缓冲数据量化研究。

        入参：
            无。
        返回值：
            无。
        异常：
            session 已关闭时抛出 ``ValueError``；sink 刷新失败按最佳努力策略忽略。
        """
        if self._closed:
            raise ValueError("task log session is closed")
        try:
            self._stream.flush()
        except Exception:  # noqa: BLE001 - logging is explicitly best effort.
            return

    def close(self) -> None:
        """关闭并释放持有的资源。

        入参：
            无。
        返回值：
            无。
        异常：
            无主动抛出的异常；sink 关闭失败按最佳努力策略忽略。
        """
        if self._closed:
            return
        try:
            self._stream.close()
        except Exception:  # noqa: BLE001, S110 - explicitly best effort.
            pass
        finally:
            self._closed = True
            self._on_close()


class TaskLogManager:
    """表示量化研究流程中的任务日志``manager``及其业务不变量。

    入参：
        diagnostic_root：所有派生路径必须位于其中的诊断日志可信根目录。
        artifact_root：不可变实验产物的可信根目录。
        sensitive_values：参与本次处理的敏感值数值表；调用方不得依赖未声明的顺序。
        max_log_bytes：限制资源使用、数量或等待时间的上限日志``bytes``。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    Derive diagnostic and success-log paths from trusted roots and UUIDs.
    """

    def __init__(
        self,
        *,
        diagnostic_root: Path,
        artifact_root: Path,
        sensitive_values: Sequence[str] = (),
        max_log_bytes: int = MAX_TASK_LOG_BYTES,
    ) -> None:
        if not isinstance(diagnostic_root, Path) or not isinstance(artifact_root, Path):
            raise TypeError("diagnostic_root and artifact_root must be Paths")
        if (
            type(max_log_bytes) is not int
            or not 1 <= max_log_bytes <= MAX_TASK_LOG_BYTES
        ):
            raise ValueError(
                f"max_log_bytes must be an integer from 1 through {MAX_TASK_LOG_BYTES}"
            )
        self._diagnostic_root = diagnostic_root.resolve()
        self._artifact_root = artifact_root.resolve()
        self._staging_root = self._artifact_root / ".experiment-staging"
        self._sensitive_values = tuple(sensitive_values)
        self._max_log_bytes = max_log_bytes
        self._sessions: dict[tuple[str, str], TaskLogSession] = {}
        self._lock = threading.Lock()

    def diagnostic_path(self, context: LogContext) -> Path:
        """计算诊断日志路径。

        入参：
            context：本次调用的上下文，类型为 ``LogContext``。
        返回值：
            返回路径（``Path``）。
        异常：
            无。
        Return the only permitted diagnostic path for this task attempt.
        """
        task_id, attempt_id = _LoggingSupport._task_attempt_ids(context)
        return (
            self._diagnostic_root
            / f"task_id={task_id}"
            / f"attempt_id={attempt_id}"
            / "run.log"
        )

    def open(self, context: LogContext) -> TaskLogSession:
        """独占打开量化研究。

        入参：
            context：本次调用的上下文，类型为 ``LogContext``。
        返回值：
            返回``open``（``TaskLogSession``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``StructuredLogWriteError``、``TypeError``、``ValueError``。
        Open a new exclusive, fsyncing JSONL stream for one claimed attempt.
        """
        if not isinstance(context, LogContext):
            raise TypeError("context must be a LogContext")
        task_id, attempt_id = _LoggingSupport._task_attempt_ids(context)
        key = (task_id, attempt_id)
        path = self.diagnostic_path(context)
        with self._lock:
            if key in self._sessions:
                raise ValueError("task log session is already open")
            try:
                _LoggingSupport._mkdir_controlled(self._diagnostic_root)
                _LoggingSupport._mkdir_controlled(
                    path.parent.parent, anchor=self._diagnostic_root
                )
                _LoggingSupport._mkdir_controlled(
                    path.parent, anchor=self._diagnostic_root
                )
                raw = _LoggingSupport._open_task_log(
                    path,
                    self._diagnostic_root,
                )
            except ValueError:
                raise
            except Exception as error:
                raise StructuredLogWriteError("open", error) from error
            stream = _DurableTextStream(
                raw,
                max_bytes=self._max_log_bytes,
            )
            session: TaskLogSession

            def unregister() -> None:
                with self._lock:
                    if self._sessions.get(key) is session:
                        self._sessions.pop(key, None)

            session = TaskLogSession(
                path=path,
                logger=StructuredLogger(
                    stream,
                    context=context,
                    sensitive_values=self._sensitive_values,
                ),
                _stream=stream,
                _on_close=unregister,
            )
            self._sessions[key] = session
            return session

    def materialize(self, context: LogContext, staging_dir: Path) -> Path:
        """校验并固化量化研究。

        入参：
            context：本次调用的上下文，类型为 ``LogContext``。
            staging_dir：发布前写入文件的同文件系统暂存目录。
        返回值：
            返回校验并固化量化研究后的``materialize``（``Path``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``StructuredLogWriteError``、``TypeError``、``ValueError``。
        Freeze the active diagnostic log into one trusted success staging dir.
        """
        task_id, attempt_id = _LoggingSupport._task_attempt_ids(context)
        key = (task_id, attempt_id)
        resolved_staging = self._resolved_staging_dir(staging_dir)
        with self._lock:
            session = self._sessions.get(key)
        if session is None:
            raise ValueError("task log materialization requires an active session")
        source = self.diagnostic_path(context)
        if session.path != source:
            raise ValueError("active task log path does not match its context")
        try:
            payload = session._stream.read_bytes()
        except OSError:
            return self.materialize_unavailable(
                context,
                resolved_staging,
                stage="ARTIFACT_VERIFY",
            )
        try:
            _LoggingSupport._validate_json_lines(payload, context=context)
        except _TaskLogContentUnavailable:
            return self.materialize_unavailable(
                context,
                resolved_staging,
                stage="ARTIFACT_VERIFY",
            )
        return self._materialize_payload(resolved_staging, payload)

    def materialize_unavailable(
        self,
        context: LogContext,
        staging_dir: Path,
        *,
        stage: str,
    ) -> Path:
        """在真实任务日志不可用时生成受控的结构化占位日志。

        入参：
            context：任务、尝试、实验和 Worker 的可信关联标识。
            staging_dir：受控实验 staging 目录。
            stage：生成占位日志时所处的实验阶段。
        返回值：
            返回 staging 目录中的必需 ``run.log`` 路径。
        异常：
            路径、身份或产物写入违反契约时抛出 ``TypeError``、``ValueError`` 或 ``StructuredLogWriteError``。
        """
        _LoggingSupport._task_attempt_ids(context)
        normalized_stage = _LoggingSupport._nonempty_text(stage, "stage")
        resolved_staging = self._resolved_staging_dir(staging_dir)
        payload = _LoggingSupport._unavailable_log_payload(
            context,
            stage=normalized_stage,
        )
        return self._materialize_payload(resolved_staging, payload)

    def _resolved_staging_dir(self, staging_dir: Path) -> Path:
        if not isinstance(staging_dir, Path):
            raise TypeError("staging_dir must be a Path")
        resolved_staging = Path(os.path.abspath(staging_dir))
        expected_root = self._staging_root
        if not resolved_staging.is_relative_to(expected_root):
            raise ValueError("staging_dir must be inside controlled experiment staging")
        try:
            _LoggingSupport._require_no_reparse_between(expected_root, resolved_staging)
        except ValueError as error:
            raise ValueError(
                "staging_dir must be an existing trusted directory"
            ) from error
        return resolved_staging

    def _materialize_payload(self, staging_dir: Path, payload: bytes) -> Path:
        target = staging_dir / "run.log"
        try:
            _LoggingSupport._write_new_controlled_file(
                target,
                root=self._staging_root,
                payload=payload,
                max_bytes=self._max_log_bytes,
            )
        except ValueError:
            raise
        except Exception as error:
            raise StructuredLogWriteError("materialize", error) from error
        return target

    def seal_and_materialize(
        self,
        context: LogContext,
        staging_dir: Path,
        *,
        stage: str,
        sealed_through: str,
    ) -> Path:
        """封存并固化并``materialize``。

        入参：
            context：本次调用的上下文，类型为 ``LogContext``。
            staging_dir：发布前写入文件的同文件系统暂存目录。
            stage：执行阶段。
            sealed_through：已封存位置截止字节位置。
        返回值：
            返回并``materialize``（``Path``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        Record the immutable prefix boundary, then copy exactly that prefix.
        """
        task_id, attempt_id = _LoggingSupport._task_attempt_ids(context)
        normalized_stage = _LoggingSupport._nonempty_text(stage, "stage")
        normalized_boundary = _LoggingSupport._nonempty_text(
            sealed_through, "sealed_through"
        )
        with self._lock:
            session = self._sessions.get((task_id, attempt_id))
        if session is None:
            raise ValueError("task log seal requires an active session")
        session.logger.emit(
            "INFO",
            "task.log_sealed",
            stage=normalized_stage,
            context={
                "authoritative_after_seal": ["audit_event", "task_attempt"],
                "sealed_through": normalized_boundary,
            },
        )
        return self.materialize(context, staging_dir)


class _DurableTextStream:
    def __init__(
        self,
        stream: TextIO,
        *,
        max_bytes: int,
    ) -> None:
        self._stream = stream
        self._max_bytes = max_bytes
        self._bytes_written = 0
        self._lock = threading.RLock()

    def write(self, value: str) -> object:
        encoded_size = len(value.encode("utf-8"))
        with self._lock:
            if self._bytes_written + encoded_size > self._max_bytes:
                raise ValueError("task log exceeds the explicit byte limit")
            written = self._stream.write(value)
            if written != len(value):
                raise OSError("task log stream accepted a partial write")
            self._bytes_written += encoded_size
            return written

    def flush(self) -> None:
        with self._lock:
            self._flush_unlocked()

    def read_bytes(self) -> bytes:
        """Flush and read one bounded prefix from the active log descriptor."""
        with self._lock:
            self._flush_unlocked()
            descriptor = self._stream.fileno()
            status = os.fstat(descriptor)
            _LoggingSupport._require_regular_status(
                status, "task log descriptor is not regular"
            )
            if status.st_size > self._max_bytes:
                raise ValueError("task log exceeds the size limit")
            position = os.lseek(descriptor, 0, os.SEEK_CUR)
            try:
                os.lseek(descriptor, 0, os.SEEK_SET)
                payload = _LoggingSupport._read_descriptor_bounded(
                    descriptor, self._max_bytes
                )
            finally:
                os.lseek(descriptor, position, os.SEEK_SET)
            if len(payload) != status.st_size:
                raise ValueError("task log size changed while materializing")
            return payload

    def _flush_unlocked(self) -> None:
        self._stream.flush()
        os.fsync(self._stream.fileno())

    def close(self) -> None:
        with self._lock:
            try:
                self._flush_unlocked()
            finally:
                self._stream.close()


class _LoggingSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _task_attempt_ids(context: LogContext) -> tuple[str, str]:
        if not isinstance(context, LogContext):
            raise TypeError("context must be a LogContext")
        return (
            _LoggingSupport._canonical_uuid(context.task_id, "task_id"),
            _LoggingSupport._canonical_uuid(context.attempt_id, "attempt_id"),
        )

    @staticmethod
    def _canonical_uuid(value: str | None, label: str) -> str:
        if not isinstance(value, str):
            raise TypeError(f"{label} must be a canonical UUID")
        try:
            parsed = UUID(value)
        except ValueError as error:
            raise ValueError(f"{label} must be a canonical UUID") from error
        if str(parsed) != value:
            raise ValueError(f"{label} must be a canonical UUID")
        return value

    @staticmethod
    def _mkdir_controlled(path: Path, *, anchor: Path | None = None) -> None:
        if anchor is not None and not path.is_relative_to(anchor):
            raise ValueError("controlled directory escaped its trusted root")
        path.mkdir(parents=True, exist_ok=True)
        if anchor is None:
            _LoggingSupport._require_directory(path)
            return
        _LoggingSupport._require_no_reparse_between(anchor, path)

    @staticmethod
    def _require_no_reparse_between(anchor: Path, target: Path) -> None:
        if not target.is_relative_to(anchor):
            raise ValueError("controlled path escaped its trusted root")
        _LoggingSupport._require_directory(anchor)
        current = anchor
        for part in target.relative_to(anchor).parts:
            current = current / part
            _LoggingSupport._require_directory(current)

    @staticmethod
    def _require_directory(path: Path) -> None:
        try:
            status = os.lstat(path)
        except OSError as error:
            raise ValueError("controlled path is unavailable") from error
        attributes = int(getattr(status, "st_file_attributes", 0))
        reparse_flag = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
        if (
            not stat.S_ISDIR(status.st_mode)
            or stat.S_ISLNK(status.st_mode)
            or bool(attributes & reparse_flag)
        ):
            raise ValueError("controlled path contains a reparse point")

    @staticmethod
    def _open_task_log(path: Path, root: Path) -> TextIO:
        path = Path(os.path.abspath(path))
        root = Path(os.path.abspath(root))
        if not path.is_relative_to(root) or path.name != "run.log":
            raise ValueError("controlled task log escaped its trusted root")
        _LoggingSupport._require_no_reparse_between(root, path.parent)
        try:
            existing = os.lstat(path)
        except FileNotFoundError:
            pass
        except OSError as error:
            raise ValueError("controlled task log path is unavailable") from error
        else:
            _LoggingSupport._require_regular_status(
                existing,
                "controlled task log final component is a reparse point",
            )
            raise ValueError("controlled task log path already exists")
        descriptor = -1
        try:
            descriptor = os.open(path, _LoggingSupport._new_file_flags(), 0o600)
            descriptor_status = os.fstat(descriptor)
            _LoggingSupport._require_regular_status(
                descriptor_status,
                "created task log descriptor is not regular",
            )
            _LoggingSupport._require_trusted_regular_path(path, root)
            stream = os.fdopen(
                descriptor,
                "w",
                encoding="utf-8",
                newline="\n",
                closefd=True,
            )
            descriptor = -1
            return stream
        finally:
            _LoggingSupport._close_descriptor(descriptor)

    @staticmethod
    def _write_new_controlled_file(
        path: Path,
        *,
        root: Path,
        payload: bytes,
        max_bytes: int,
    ) -> None:
        if len(payload) > max_bytes:
            raise ValueError("materialized task log exceeds the size limit")
        path = Path(os.path.abspath(path))
        root = Path(os.path.abspath(root))
        if not path.is_relative_to(root) or path.name != "run.log":
            raise ValueError("materialized task log escaped its trusted root")
        _LoggingSupport._require_no_reparse_between(root, path.parent)
        try:
            existing = os.lstat(path)
        except FileNotFoundError:
            pass
        except OSError as error:
            raise ValueError("materialized task log path is unavailable") from error
        else:
            _LoggingSupport._require_regular_status(
                existing,
                "materialized task log final component is a reparse point",
            )
            raise ValueError("materialized task log already exists")
        descriptor = -1
        try:
            descriptor = os.open(path, _LoggingSupport._new_write_file_flags(), 0o600)
            before = os.fstat(descriptor)
            _LoggingSupport._require_regular_status(
                before, "materialized task log descriptor is not regular"
            )
            _LoggingSupport._require_trusted_regular_path(path, root)
            view = memoryview(payload)
            written = 0
            while written < len(view):
                count = os.write(descriptor, view[written:])
                if count <= 0:
                    raise OSError("materialized task log write made no progress")
                written += count
            os.fsync(descriptor)
            after = os.fstat(descriptor)
            _LoggingSupport._require_regular_status(
                after, "materialized task log descriptor is not regular"
            )
            _LoggingSupport._require_trusted_regular_path(path, root)
            if after.st_size != len(payload):
                raise ValueError("materialized task log size changed while writing")
        finally:
            _LoggingSupport._close_descriptor(descriptor)

    @staticmethod
    def _new_file_flags() -> int:
        return (
            os.O_RDWR
            | os.O_CREAT
            | os.O_EXCL
            | int(getattr(os, "O_BINARY", 0))
            | int(getattr(os, "O_NOFOLLOW", 0))
        )

    @staticmethod
    def _new_write_file_flags() -> int:
        return (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | int(getattr(os, "O_BINARY", 0))
            | int(getattr(os, "O_NOFOLLOW", 0))
        )

    @staticmethod
    def _require_trusted_regular_path(path: Path, root: Path) -> None:
        if not path.is_relative_to(root) or path.name != "run.log":
            raise ValueError("task log escaped its trusted root")
        _LoggingSupport._require_no_reparse_between(root, path.parent)
        try:
            status = os.lstat(path)
        except OSError as error:
            raise ValueError("task log is unavailable") from error
        _LoggingSupport._require_regular_status(
            status, "task log is not a regular file"
        )

    @staticmethod
    def _require_regular_status(status: os.stat_result, message: str) -> None:
        attributes = int(getattr(status, "st_file_attributes", 0))
        reparse_flag = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
        if (
            not stat.S_ISREG(status.st_mode)
            or stat.S_ISLNK(status.st_mode)
            or bool(attributes & reparse_flag)
        ):
            raise ValueError(message)

    @staticmethod
    def _read_descriptor_bounded(descriptor: int, max_bytes: int) -> bytes:
        chunks: list[bytes] = []
        total = 0
        while total <= max_bytes:
            chunk = os.read(descriptor, min(65_536, max_bytes + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
        payload = b"".join(chunks)
        if len(payload) > max_bytes:
            raise ValueError("task log exceeds the size limit")
        return payload

    @staticmethod
    def _close_descriptor(descriptor: int) -> None:
        if descriptor < 0:
            return
        try:
            os.close(descriptor)
        except OSError:
            pass

    @staticmethod
    def _validate_json_lines(
        payload: bytes, *, context: LogContext | None = None
    ) -> None:
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError as error:
            raise _TaskLogContentUnavailable(
                "run.log must be UTF-8 JSON Lines"
            ) from error
        lines = text.splitlines()
        if not lines or any(not line for line in lines):
            raise _TaskLogContentUnavailable(
                "run.log must contain nonempty JSON Lines"
            )
        for line in lines:
            try:
                record = json.loads(line)
            except json.JSONDecodeError as error:
                raise _TaskLogContentUnavailable(
                    "run.log must contain valid JSON Lines"
                ) from error
            if (
                not isinstance(record, dict)
                or tuple(record)[: len(_CORE_FIELDS)] != _CORE_FIELDS
                or set(record).difference((*_CORE_FIELDS, *_OPTIONAL_FIELDS, "context"))
            ):
                raise _TaskLogContentUnavailable(
                    "run.log record core fields are invalid"
                )
            if context is not None:
                expected = {
                    "request_id": context.request_id,
                    "experiment_id": context.experiment_id,
                    "task_id": context.task_id,
                    "attempt_id": context.attempt_id,
                    "worker_id": context.worker_id,
                }
                if any(
                    (key in record if value is None else record.get(key) != value)
                    for key, value in expected.items()
                ):
                    raise ValueError("run.log record correlation fields are invalid")

    @staticmethod
    def _unavailable_log_payload(context: LogContext, *, stage: str) -> bytes:
        record: dict[str, JsonValue] = {
            "timestamp": _LoggingSupport._local_timestamp(),
            "level": "WARNING",
            "event": "task.log_unavailable",
        }
        optional_values = {
            "request_id": context.request_id,
            "experiment_id": context.experiment_id,
            "task_id": context.task_id,
            "attempt_id": context.attempt_id,
            "worker_id": context.worker_id,
            "stage": stage,
        }
        record.update(
            {key: value for key, value in optional_values.items() if value is not None}
        )
        record["context"] = {"reason": "diagnostic_log_unavailable"}
        return (
            json.dumps(
                record,
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")

    @staticmethod
    def _redact(
        value: object,
        *,
        sensitive_values: tuple[str, ...],
        budget: _RedactionBudget,
        depth: int,
        key: str | None,
    ) -> JsonValue:
        budget.visit(depth)
        normalized_key = (
            _LoggingSupport._normalized_key(key) if key is not None else None
        )
        if normalized_key is not None and (
            normalized_key in _SECRET_CONTAINER_KEYS
            or any(marker in normalized_key for marker in _SECRET_KEY_MARKERS)
        ):
            return _REDACTED
        if value is None or isinstance(value, (bool, int)):
            return value
        if isinstance(value, float):
            return value if math.isfinite(value) else "[NON_FINITE]"
        if isinstance(value, str):
            return _LoggingSupport._redact_text(value, sensitive_values)
        if isinstance(value, BaseException):
            return {
                "exception_type": type(value).__name__,
                "message": _LoggingSupport._redact_text(str(value), sensitive_values),
            }
        if isinstance(value, Mapping):
            result: dict[str, JsonValue] = {}
            for raw_key, item in value.items():
                item_key = str(raw_key)
                result[item_key] = _LoggingSupport._redact(
                    item,
                    sensitive_values=sensitive_values,
                    budget=budget,
                    depth=depth + 1,
                    key=item_key,
                )
            return result
        if isinstance(value, Sequence) and not isinstance(
            value, (str, bytes, bytearray)
        ):
            return [
                _LoggingSupport._redact(
                    item,
                    sensitive_values=sensitive_values,
                    budget=budget,
                    depth=depth + 1,
                    key=None,
                )
                for item in value
            ]
        return {
            "object_type": type(value).__name__,
            "value": "[UNSERIALIZABLE]",
        }

    @staticmethod
    def _normalized_key(value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "_", value.casefold()).strip("_")

    @staticmethod
    def _is_secret_key(value: str) -> bool:
        normalized = _LoggingSupport._normalized_key(value)
        return normalized in _SECRET_CONTAINER_KEYS or any(
            marker in normalized for marker in _SECRET_KEY_MARKERS
        )

    @staticmethod
    def _redact_text(value: str, sensitive_values: tuple[str, ...]) -> str:
        redacted = value
        for sensitive in sensitive_values:
            redacted = redacted.replace(sensitive, _REDACTED)
        return redacted

    @staticmethod
    def _local_timestamp() -> str:
        return datetime.now().astimezone().isoformat()

    @staticmethod
    def _nonempty_text(value: str, label: str) -> str:
        if not isinstance(value, str):
            raise TypeError(f"{label} must be a string")
        if not value.strip():
            raise ValueError(f"{label} must not be empty")
        return value


class _RedactionBudget:
    def __init__(self) -> None:
        self.nodes = 0

    def visit(self, depth: int) -> None:
        self.nodes += 1
        if depth > _MAX_DEPTH or self.nodes > _MAX_NODES:
            raise ValueError("log context exceeds redaction limits")


def redact_context(
    value: Mapping[str, object], *, sensitive_values: Sequence[str] = ()
) -> dict[str, JsonValue]:
    """递归脱敏运行上下文；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        value：待校验或转换的值，类型为 ``Mapping[str, object]``。
        sensitive_values：参与本次处理的敏感值数值表；调用方不得依赖未声明的顺序。
    返回值：
        返回运行上下文（``dict[str, JsonValue]``）。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    Return one bounded secret-safe mapping for non-log JSON boundaries.
    """
    if not isinstance(value, Mapping):
        raise TypeError("value must be a mapping")
    if isinstance(sensitive_values, (str, bytes)) or not isinstance(
        sensitive_values, Sequence
    ):
        raise TypeError("sensitive_values must be a sequence of strings")
    normalized = tuple(
        sorted(
            {
                item
                for item in sensitive_values
                if isinstance(item, str) and len(item) >= _MIN_GLOBAL_SECRET_LENGTH
            },
            key=len,
            reverse=True,
        )
    )
    try:
        redacted = _LoggingSupport._redact(
            value,
            sensitive_values=normalized,
            budget=_RedactionBudget(),
            depth=0,
            key=None,
        )
    except Exception:  # noqa: BLE001 - redaction must fail to a fixed safe value
        return {"redaction": "[REDACTION_FAILED]"}
    if not isinstance(redacted, dict):
        return {"redaction": "[REDACTION_FAILED]"}
    return redacted


def sensitive_environment_values(environment: Mapping[str, str]) -> tuple[str, ...]:
    """提取显式敏感环境变量中的长值运行环境数值表；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        environment：参与本次处理的运行环境；调用方不得依赖未声明的顺序。
    返回值：
        返回运行环境数值表（``tuple[str, ...]``）。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    Extract only long values whose environment keys explicitly denote secrets.
    """
    if not isinstance(environment, Mapping):
        raise TypeError("environment must be a mapping")
    return tuple(
        value
        for key, value in environment.items()
        if isinstance(key, str)
        and isinstance(value, str)
        and len(value) >= _MIN_GLOBAL_SECRET_LENGTH
        and _LoggingSupport._is_secret_key(key)
    )


__all__ = [
    "MAX_TASK_LOG_BYTES",
    "LogContext",
    "StructuredLogWriteError",
    "StructuredLogger",
    "TaskLogManager",
    "TaskLogSession",
    "TeeLogStream",
    "TextLogStream",
    "redact_context",
    "sensitive_environment_values",
]
