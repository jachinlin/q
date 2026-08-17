"""提供不可变数据存储使用的路径校验与数据根独占执行锁。"""

from __future__ import annotations

import errno
import os
import stat
import threading
from pathlib import Path
from typing import BinaryIO, ClassVar, Never, Self

from quant_research.domain.enums import Severity
from quant_research.domain.errors import ErrorDetail, QuantError


class _DataRootLockState:
    """保存同一进程内共享的数据根锁状态。"""

    def __init__(self) -> None:
        self.guard = threading.RLock()
        self.depth = 0
        self.stream: BinaryIO | None = None


class DataRootExecutionLock:
    """串行化同一数据根上的本机数据流水线写操作。

    入参：
        data_root：包含 Raw、Canonical 与 state 子目录的可信数据根。
    返回值：
        构造可重复进入的上下文管理器；同线程嵌套调用只持有一份 OS 锁。
    异常：
        QuantError：同进程其他线程或其他 Windows 进程正在运行数据流水线时抛出。
        RuntimeError：在非 Windows 平台使用该 Windows 本地平台锁时抛出。

    锁语义：
        固定锁文件可以永久存在，所有权由 Windows 字节范围锁决定。进程退出或崩溃
        时操作系统自动释放所有权，因此不需要 PID 探测、owner token 或陈旧锁回收。
    """

    _states: ClassVar[dict[str, _DataRootLockState]] = {}
    _states_guard: ClassVar[threading.Lock] = threading.Lock()

    def __init__(self, data_root: Path) -> None:
        self._root = resolved_storage_root(data_root)
        self._path = self._root / "state" / "data-pipeline.lock"
        key = os.path.normcase(str(self._path))
        with self._states_guard:
            self._state = self._states.setdefault(key, _DataRootLockState())

    def __enter__(self) -> Self:
        """立即取得进程内和 Windows 跨进程锁。

        入参：
            无。
        返回值：
            已持有数据根独占锁的当前上下文管理器。
        异常：
            QuantError：同一数据根已有其他执行者时抛出
            ``DATA_PIPELINE_ALREADY_RUNNING``。
            RuntimeError：当前平台不是 Windows 时抛出。
        """
        if os.name != "nt":
            raise RuntimeError("data pipeline execution lock requires Windows")
        if not self._state.guard.acquire(blocking=False):
            self._raise_busy()
        try:
            if self._state.depth == 0:
                self._state.stream = self._acquire_windows_lock()
            self._state.depth += 1
        except BaseException:
            self._state.guard.release()
            raise
        return self

    def __exit__(self, *_: object) -> None:
        """释放当前嵌套层，并在最外层退出时释放 Windows 锁。

        入参：
            退出上下文传入的异常信息；本实现不抑制异常。
        返回值：
            无。
        异常：
            OS 锁状态损坏或释放失败时传播对应异常。
        """
        try:
            if self._state.depth <= 0:
                raise RuntimeError("data pipeline execution lock is not held")
            self._state.depth -= 1
            if self._state.depth == 0:
                self._release_windows_lock()
        finally:
            self._state.guard.release()

    def _acquire_windows_lock(self) -> BinaryIO:
        import msvcrt

        self._path.parent.mkdir(parents=True, exist_ok=True)
        validate_storage_path(self._root, self._path.parent)
        stream = self._path.open("a+b", buffering=0)
        try:
            if self._path.stat().st_size == 0:
                stream.write(b"\0")
            stream.seek(0)
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as error:
            stream.close()
            if error.errno in {errno.EACCES, errno.EDEADLK}:
                self._raise_busy()
            raise
        return stream

    def _release_windows_lock(self) -> None:
        import msvcrt

        stream = self._state.stream
        if stream is None:
            raise RuntimeError("data pipeline OS lock handle is missing")
        self._state.stream = None
        try:
            stream.seek(0)
            msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
        finally:
            stream.close()

    @staticmethod
    def _raise_busy() -> Never:
        raise QuantError(
            ErrorDetail(
                code="DATA_PIPELINE_ALREADY_RUNNING",
                severity=Severity.SEVERE,
                message="another data pipeline is already running for this data root",
                context={},
                remediation="wait for the active data operation to finish and retry",
                retryable=True,
            )
        )


def resolved_storage_root(path: Path) -> Path:
    """解析并校验不可变存储根目录；该路径边界是稳定公开 API，因此保留为模块级入口。

    入参：
        path：待校验或读取的文件系统路径。
    返回值：
        返回``storage``可信根目录（``Path``）。
    异常：
        实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
    """
    absolute = path.absolute()
    _StoragePathValidator.reject_existing_reparse_components(absolute)
    return _StoragePathValidator.normalize_windows_extended_path(
        absolute.resolve(strict=False)
    )


def validate_storage_path(
    root: Path,
    path: Path,
    *,
    require_file: bool = False,
) -> Path:
    """校验路径位于可信存储根且不含重解析跳转；该路径边界是稳定公开 API，因此保留为模块级入口。

    入参：
        root：配置的可信存储根目录。
        path：待校验或读取的文件系统路径。
        require_file：调用接口所需的同名参数，具体约束见类型标注。
    返回值：
        返回校验``storage``路径后的``storage``路径（``Path``）。
    异常：
        ValueError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
    """
    absolute = path.absolute()
    try:
        absolute.relative_to(root)
    except ValueError as error:
        raise ValueError("storage path is outside its configured root") from error
    _StoragePathValidator.reject_existing_reparse_components(root)
    _StoragePathValidator.reject_existing_reparse_components(absolute, minimum=root)
    resolved = _StoragePathValidator.normalize_windows_extended_path(
        absolute.resolve(strict=False)
    )
    try:
        resolved.relative_to(root)
    except ValueError as error:
        raise ValueError("storage path resolves outside its configured root") from error
    if resolved != absolute:
        raise ValueError("storage path contains a link or reparse point")
    if require_file and not absolute.is_file():
        raise ValueError("storage path is not a regular file")
    return absolute


class _StoragePathValidator:
    """集中执行存储根目录的链接检测与 Windows 路径规范化。"""

    @classmethod
    def reject_existing_reparse_components(
        cls, path: Path, *, minimum: Path | None = None
    ) -> None:
        components = [path, *path.parents]
        for component in reversed(components):
            if minimum is not None:
                try:
                    component.relative_to(minimum)
                except ValueError:
                    continue
            if not component.exists() and not component.is_symlink():
                continue
            if cls.is_reparse_point(component):
                raise ValueError("storage path contains a link or reparse point")

    @staticmethod
    def is_reparse_point(path: Path) -> bool:
        if path.is_symlink():
            return True
        is_junction = getattr(path, "is_junction", None)
        if is_junction is not None and is_junction():
            return True
        try:
            attributes = path.lstat().st_file_attributes
        except (AttributeError, OSError):
            return False
        return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))

    @staticmethod
    def normalize_windows_extended_path(path: Path) -> Path:
        if os.name != "nt":
            return path
        value = str(path)
        if value.startswith("\\\\?\\UNC\\"):
            value = "\\\\" + value[8:]
        elif value.startswith("\\\\?\\"):
            value = value[4:]
        return Path(value)
