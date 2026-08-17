"""负责不可变 Raw Parquet 分区的原子发布、定位与完整性校验。"""

import hashlib
import importlib
import json
import os
import re
import threading
import time
import uuid
from _thread import LockType
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Never, Protocol, Self, cast

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from quant_research.data.contracts import (
    JsonValue,
    PublishedPartition,
    RawBatch,
    canonical_json_bytes,
)
from quant_research.data.storage import resolved_storage_root, validate_storage_path
from quant_research.domain.enums import Severity
from quant_research.domain.errors import ErrorDetail, QuantError

_SOURCE_OR_ENDPOINT = re.compile(r"[A-Za-z0-9_-]+\Z")

_THREAD_GUARDS: dict[str, LockType] = {}
_THREAD_GUARDS_LOCK = threading.Lock()


class _FcntlModule(Protocol):
    LOCK_EX: int
    LOCK_NB: int
    LOCK_UN: int

    def flock(self, descriptor: int, operation: int) -> None: ...


class _PartitionLockSupport:
    """集中提供跨平台分区锁依赖与进程内线程锁。"""

    @staticmethod
    def fcntl_module() -> _FcntlModule:
        return cast(_FcntlModule, importlib.import_module("fcntl"))

    @staticmethod
    def thread_guard_for(path: Path) -> LockType:
        key = os.path.normcase(str(path.resolve(strict=False)))
        with _THREAD_GUARDS_LOCK:
            return _THREAD_GUARDS.setdefault(key, threading.Lock())


class _AcquisitionGuard:
    """Serialize fixed lock-path transitions across threads and processes."""

    def __init__(
        self, path: Path, *, deadline: float | None, poll_seconds: float
    ) -> None:
        self._path = path.parent / f"{path.name}.guard"
        self._lock_path = path
        self._deadline = deadline
        self._poll_seconds = poll_seconds
        self._thread_lock = _PartitionLockSupport.thread_guard_for(self._path)
        self._descriptor: int | None = None
        self._handle: int | None = None

    def __enter__(self) -> Self:
        if self._deadline is None:
            self._thread_lock.acquire()
        else:
            remaining = self._deadline - time.monotonic()
            if remaining <= 0 or not self._thread_lock.acquire(timeout=remaining):
                self._raise_timeout()
        try:
            while not self._try_acquire_os_guard():
                if self._deadline is None:
                    time.sleep(self._poll_seconds)
                else:
                    remaining = self._deadline - time.monotonic()
                    if remaining <= 0:
                        self._raise_timeout()
                    time.sleep(min(self._poll_seconds, remaining))
        except BaseException:
            self._thread_lock.release()
            raise
        return self

    def __exit__(self, *_: object) -> None:
        try:
            if os.name == "nt":
                self._close_windows_guard()
            else:
                self._close_posix_guard()
        finally:
            self._thread_lock.release()

    def _try_acquire_os_guard(self) -> bool:
        if os.name == "nt":
            return self._try_open_windows_guard()
        return self._try_open_posix_guard()

    def _try_open_windows_guard(self) -> bool:
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        create_file = kernel32.CreateFileW
        create_file.argtypes = [
            wintypes.LPCWSTR,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.LPVOID,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.HANDLE,
        ]
        create_file.restype = wintypes.HANDLE
        handle = create_file(
            str(self._path),
            0x80000000 | 0x40000000,
            0,
            None,
            4,
            0x00000080,
            None,
        )
        if handle != wintypes.HANDLE(-1).value:
            self._handle = handle
            return True
        error = ctypes.get_last_error()
        if error in {32, 33}:
            return False
        raise ctypes.WinError(error)

    def _close_windows_guard(self) -> None:
        if self._handle is None:
            return
        import ctypes
        from ctypes import wintypes

        close_handle = ctypes.WinDLL("kernel32", use_last_error=True).CloseHandle
        close_handle.argtypes = [wintypes.HANDLE]
        close_handle.restype = wintypes.BOOL
        handle = self._handle
        self._handle = None
        if not close_handle(handle):
            raise ctypes.WinError(ctypes.get_last_error())

    def _try_open_posix_guard(self) -> bool:
        fcntl = _PartitionLockSupport.fcntl_module()

        descriptor = os.open(
            self._path,
            os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(descriptor)
            return False
        except BaseException:
            os.close(descriptor)
            raise
        self._descriptor = descriptor
        return True

    def _close_posix_guard(self) -> None:
        if self._descriptor is None:
            return
        fcntl = _PartitionLockSupport.fcntl_module()

        descriptor = self._descriptor
        self._descriptor = None
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    def _raise_timeout(self) -> None:
        raise TimeoutError(f"timed out waiting for partition lock: {self._lock_path}")


class _PartitionLock:
    """A Windows-compatible inter-process lock backed by atomic directory creation."""

    _OWNER_FILE = "owner.json"
    _TOKEN = re.compile(r"[0-9a-f]{32}\Z")

    def __init__(
        self,
        path: Path,
        *,
        timeout_seconds: float = 30.0,
        poll_seconds: float = 0.05,
        stale_after_seconds: float = 300.0,
    ) -> None:
        self._path = path
        self._timeout_seconds = timeout_seconds
        self._poll_seconds = poll_seconds
        self._stale_after_seconds = stale_after_seconds
        self._owned = False
        self._token: str | None = None

    def __enter__(self) -> Self:
        """Acquire the lock, recovering only a demonstrably dead stale owner."""
        if self._owned:
            raise RuntimeError("partition lock is already owned")
        token = uuid.uuid4().hex
        deadline = time.monotonic() + self._timeout_seconds
        while True:
            temporary_path = self._path.parent / f".locktmp-{uuid.uuid4().hex[:16]}"
            try:
                temporary_path.mkdir()
            except FileExistsError:
                continue
            try:
                self._owner_path_for(temporary_path).write_text(
                    json.dumps(
                        {"pid": os.getpid(), "token": token},
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    encoding="utf-8",
                )
            except Exception:
                self._remove_private_directory(temporary_path)
                raise
            try:
                installed = self._install_under_guard(
                    temporary_path, token, deadline=deadline
                )
            except Exception:
                self._remove_private_directory(temporary_path)
                raise
            if installed:
                self._owned = True
                self._token = token
                return self
            self._remove_private_directory(temporary_path)
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"timed out waiting for partition lock: {self._path}"
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"timed out waiting for partition lock: {self._path}"
                )
            time.sleep(min(self._poll_seconds, remaining))

    def __exit__(self, *_: object) -> None:
        """Release this owner, waiting until the crash-safe guard is available."""
        self.release()

    def release(self) -> None:
        """Detach this owner's fixed path; retain ownership on pre-detach errors."""
        if not self._owned:
            return
        token = self._token
        if token is None:
            raise RuntimeError("partition lock ownership token is missing")
        detached = False
        tombstone: Path | None = None
        try:
            with _AcquisitionGuard(
                self._path, deadline=None, poll_seconds=self._poll_seconds
            ):
                identity = self._path_identity(self._path)
                owner_identity = self._owner_identity(self._path)
                owner = self._read_owner(self._path)
                if owner != (os.getpid(), token):
                    raise RuntimeError(
                        "partition lock ownership changed before release"
                    )
                if (
                    self._path_identity(self._path) != identity
                    or self._owner_identity(self._path) != owner_identity
                    or self._read_owner(self._path) != owner
                ):
                    raise RuntimeError("partition lock identity changed before release")
                tombstone = self._detach_to_private_path(".release-")
                detached = True
            if tombstone is None:
                raise RuntimeError("partition lock release tombstone is missing")
            self._remove_token_directory(tombstone, token)
        finally:
            if detached:
                self._owned = False
                self._token = None

    def _owner_path_for(self, directory: Path) -> Path:
        return directory / self._OWNER_FILE

    def _install_under_guard(
        self, temporary_path: Path, claimant_token: str, *, deadline: float
    ) -> bool:
        """Install one prepared owner while every fixed-path transition is guarded."""
        with _AcquisitionGuard(
            self._path, deadline=deadline, poll_seconds=self._poll_seconds
        ):
            try:
                temporary_path.rename(self._path)
            except FileExistsError:
                detached = self._detach_stale_lock(claimant_token)
                if detached is None:
                    return False
                tombstone, owner = detached
                self._remove_detached_stale_directory(tombstone, owner)
                temporary_path.rename(self._path)
            return True

    def _reclaim_stale_lock(self, claimant_token: str) -> None:
        """Guard stale observation and detachment as one fixed-path transition."""
        deadline = time.monotonic() + self._timeout_seconds
        with _AcquisitionGuard(
            self._path, deadline=deadline, poll_seconds=self._poll_seconds
        ):
            detached = self._detach_stale_lock(claimant_token)
        if detached is not None:
            self._remove_detached_stale_directory(*detached)

    def _detach_stale_lock(
        self, claimant_token: str
    ) -> tuple[Path, tuple[int, str] | None] | None:
        """Observe and detach a stale fixed-path object while the guard is held."""
        try:
            observed_stat = self._path.stat(follow_symlinks=False)
        except FileNotFoundError:
            return None
        age_seconds = time.time() - observed_stat.st_mtime
        if age_seconds < self._stale_after_seconds:
            return None
        owner_identity = self._owner_identity(self._path)
        owner = self._read_owner(self._path)
        if owner is not None and self._process_is_alive(owner[0]):
            return None
        if (
            self._path_identity(self._path) != self._stat_identity(observed_stat)
            or self._owner_identity(self._path) != owner_identity
            or self._read_owner(self._path) != owner
        ):
            return None
        del claimant_token
        try:
            tombstone = self._detach_to_private_path(".stale-")
        except FileNotFoundError:
            return None
        return tombstone, owner

    def _detach_to_private_path(self, prefix: str) -> Path:
        """Detach to a short random name; owner.json retains the full token."""
        while True:
            target = self._path.parent / f"{prefix}{uuid.uuid4().hex[:16]}"
            try:
                self._path.rename(target)
            except FileExistsError:
                continue
            return target

    @classmethod
    def _remove_detached_stale_directory(
        cls, tombstone: Path, owner: tuple[int, str] | None
    ) -> None:
        if owner is None:
            cls._remove_confirmed_stale_directory(tombstone)
        else:
            cls._remove_token_directory(tombstone, owner[1])

    @classmethod
    def _read_owner(cls, directory: Path) -> tuple[int, str] | None:
        try:
            owner = json.loads(
                (directory / cls._OWNER_FILE).read_text(encoding="utf-8")
            )
            if not isinstance(owner, Mapping) or set(owner) != {"pid", "token"}:
                return None
            process_id = owner["pid"]
            token = owner["token"]
            if (
                type(process_id) is not int
                or not isinstance(token, str)
                or cls._TOKEN.fullmatch(token) is None
            ):
                return None
            return process_id, token
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return None

    @classmethod
    def _path_identity(cls, path: Path) -> tuple[int, int, int]:
        return cls._stat_identity(path.stat(follow_symlinks=False))

    @classmethod
    def _owner_identity(cls, directory: Path) -> tuple[int, int, int] | None:
        try:
            return cls._path_identity(directory / cls._OWNER_FILE)
        except FileNotFoundError:
            return None

    @staticmethod
    def _stat_identity(value: os.stat_result) -> tuple[int, int, int]:
        return value.st_dev, value.st_ino, value.st_mtime_ns

    @classmethod
    def _remove_token_directory(cls, path: Path, expected_token: str) -> None:
        """Delete a claimed directory only while its owner token still matches."""
        owner = cls._read_owner(path)
        if owner is None or owner[1] != expected_token:
            raise RuntimeError("partition lock tombstone ownership changed")
        children = tuple(path.iterdir())
        if {child.name for child in children} != {cls._OWNER_FILE}:
            raise RuntimeError("partition lock directory contains unexpected paths")
        (path / cls._OWNER_FILE).unlink()
        path.rmdir()

    @classmethod
    def _remove_confirmed_stale_directory(cls, path: Path) -> None:
        """Delete only the malformed lock directory atomically claimed as stale."""
        children = tuple(path.iterdir())
        if {child.name for child in children} - {cls._OWNER_FILE}:
            raise RuntimeError("stale partition lock contains unexpected paths")
        (path / cls._OWNER_FILE).unlink(missing_ok=True)
        path.rmdir()

    @classmethod
    def _remove_private_directory(cls, path: Path) -> None:
        """Clean an unpublished temporary directory created by this acquisition."""
        try:
            (path / cls._OWNER_FILE).unlink(missing_ok=True)
            path.rmdir()
        except FileNotFoundError:
            return

    @staticmethod
    def _process_is_alive(process_id: int) -> bool:
        if process_id <= 0:
            return False
        if os.name == "nt":
            return _WindowsProcessInspector.is_alive(process_id)
        try:
            os.kill(process_id, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError:
            return False
        return True


class _WindowsProcessInspector:
    """通过 Windows API 判断持锁进程是否仍存活。"""

    @staticmethod
    def is_alive(process_id: int) -> bool:
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        open_process = kernel32.OpenProcess
        open_process.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        open_process.restype = wintypes.HANDLE
        get_exit_code = kernel32.GetExitCodeProcess
        get_exit_code.argtypes = [wintypes.HANDLE, wintypes.LPDWORD]
        get_exit_code.restype = wintypes.BOOL
        close_handle = kernel32.CloseHandle
        close_handle.argtypes = [wintypes.HANDLE]
        close_handle.restype = wintypes.BOOL

        handle = open_process(0x1000, False, process_id)
        if not handle:
            return ctypes.get_last_error() == 5
        try:
            exit_code = wintypes.DWORD()
            if not get_exit_code(handle, ctypes.byref(exit_code)):
                error = ctypes.get_last_error()
                if error == 5:
                    return True
                raise ctypes.WinError(error)
            return exit_code.value == 259
        finally:
            close_handle(handle)


class RawPartitionStore:
    """以内容寻址 Parquet 和请求 manifest 原子发布 Raw 批次。

    入参：
        raw_root：构造对象所需的同名字段，约束见类型标注。
    返回值：
        构造并返回 ``RawPartitionStore`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    def __init__(self, raw_root: Path) -> None:
        self._raw_root = resolved_storage_root(raw_root)

    @property
    def root(self) -> Path:
        """返回该存储实例受信任的绝对根目录。

        入参：
            无。
        返回值：
            返回可信根目录（``Path``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        return self._raw_root

    def publish(self, batch: RawBatch) -> PublishedPartition:
        """发布不可变内容，并在内容相同时复用已有对象。

        入参：
            batch：待发布的不可变数据批次。
        返回值：
            返回校验并原子发布Canonical 数据后的``publish``（``PublishedPartition``）。
        异常：
            ValueError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        self._validate_path_segment(batch.source, "source", _SOURCE_OR_ENDPOINT)
        self._validate_path_segment(batch.endpoint, "endpoint", _SOURCE_OR_ENDPOINT)
        request_hash = hashlib.sha256(canonical_json_bytes(batch.request)).hexdigest()
        table = self._table_from_batch(batch)
        content_hash = self._content_hash(table)
        schema_fingerprint = self._schema_fingerprint(table.schema)
        row_count = table.num_rows
        dataset_dir = (
            self._raw_root / f"source={batch.source}" / f"endpoint={batch.endpoint}"
        )
        request_dir = dataset_dir / request_hash
        validate_storage_path(self._raw_root, dataset_dir)
        dataset_dir.mkdir(parents=True, exist_ok=True)
        validate_storage_path(self._raw_root, dataset_dir)
        lock_path = self._identity_lock_path(dataset_dir, request_hash)
        validate_storage_path(self._raw_root, lock_path)
        with _PartitionLock(lock_path):
            validate_storage_path(self._raw_root, request_dir)
            request_dir.mkdir(parents=True, exist_ok=True)
            validate_storage_path(self._raw_root, request_dir)
            data_path = request_dir / f"{content_hash}.parquet"
            manifest_path = request_dir / _MANIFEST_FILE
            validate_storage_path(self._raw_root, data_path)
            validate_storage_path(self._raw_root, manifest_path)
            try:
                self._ensure_data_file(
                    data_path,
                    request_dir,
                    table,
                    content_hash,
                    schema_fingerprint,
                    row_count,
                    batch=batch,
                    request_hash=request_hash,
                )
            except ValueError as error:
                self._raise_conflict(
                    manifest_path,
                    request_hash,
                    "published data fails integrity checks",
                    error,
                )
            entry = self._manifest_entry(
                batch, content_hash, schema_fingerprint, row_count
            )
            try:
                manifest = (
                    self._read_manifest(manifest_path)
                    if manifest_path.exists()
                    else None
                )
                if manifest is None:
                    self._write_manifest(
                        manifest_path,
                        {
                            "source": batch.source,
                            "endpoint": batch.endpoint,
                            "request": dict(batch.request),
                            "request_hash": request_hash,
                            "current_content_hash": content_hash,
                            "files": [entry],
                        },
                    )
                else:
                    self._raise_if_manifest_mismatch(manifest, batch, request_hash)
                    if manifest["current_content_hash"] != content_hash:
                        retained = [
                            item
                            for item in _RawManifestSupport.entry_list(manifest)
                            if item["content_hash"] != content_hash
                        ]
                        retained.append(entry)
                        if (
                            batch.endpoint == "query_stock_basic"
                            and len(retained) > _INSTRUMENT_MANIFEST_FILE_LIMIT
                        ):
                            retained = retained[-_INSTRUMENT_MANIFEST_FILE_LIMIT:]
                        manifest["current_content_hash"] = content_hash
                        manifest["files"] = retained
                        self._write_manifest(manifest_path, manifest)
                    else:
                        entry = self._entry_for(manifest, content_hash)
                        if (
                            str(entry["schema_fingerprint"]) != schema_fingerprint
                            or _RawManifestSupport.entry_int(entry, "row_count")
                            != row_count
                        ):
                            raise ValueError(
                                "raw manifest entry does not match its content"
                            )
            except ValueError as error:
                self._raise_conflict(
                    manifest_path, request_hash, "manifest is invalid", error
                )
            return PublishedPartition(
                source=batch.source,
                endpoint=batch.endpoint,
                request=batch.request,
                retrieved_at=_RawManifestSupport.parse_retrieved_at(
                    str(entry["retrieved_at"])
                ),
                data_path=data_path,
                manifest_path=manifest_path,
                request_hash=request_hash,
                content_hash=content_hash,
                schema_fingerprint=str(entry["schema_fingerprint"]),
                row_count=_RawManifestSupport.entry_int(entry, "row_count"),
            )

    def find_by_request(
        self,
        source: str,
        endpoint: str,
        request: Mapping[str, JsonValue],
    ) -> PublishedPartition | None:
        """按规范化请求定位当前已发布 Raw 分区。

        入参：
            source：供应商标识。
            endpoint：供应商原生端点名称。
            request：包含完整业务字段的规范化供应商请求。
        返回值：
            返回查询``by``请求后的``by``请求（``PublishedPartition | None``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        partition = self.find_metadata_by_request(source, endpoint, request)
        if partition is None:
            return None
        validate_storage_path(self._raw_root, partition.data_path, require_file=True)
        try:
            self._verify_data_file(
                partition.data_path,
                content_hash=partition.content_hash,
                schema_fingerprint=partition.schema_fingerprint,
                row_count=partition.row_count,
            )
        except (OSError, pa.ArrowException, ValueError) as error:
            self._raise_conflict(
                partition.manifest_path,
                partition.request_hash,
                "published data fails integrity checks",
                error,
            )
        return partition

    def find_metadata_by_request(
        self,
        source: str,
        endpoint: str,
        request: Mapping[str, JsonValue],
    ) -> PublishedPartition | None:
        """仅从目录读取请求当前头的元数据。

        入参：
            source：供应商标识。
            endpoint：供应商原生端点名称。
            request：包含完整业务字段的规范化供应商请求。
        返回值：
            返回查询元数据``by``请求后的元数据``by``请求（``PublishedPartition | None``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        self._validate_path_segment(source, "source", _SOURCE_OR_ENDPOINT)
        self._validate_path_segment(endpoint, "endpoint", _SOURCE_OR_ENDPOINT)
        request_hash = hashlib.sha256(canonical_json_bytes(request)).hexdigest()
        dataset_dir = self._raw_root / f"source={source}" / f"endpoint={endpoint}"
        manifest_path = dataset_dir / request_hash / _MANIFEST_FILE
        if not manifest_path.is_file():
            return None
        validate_storage_path(self._raw_root, manifest_path, require_file=True)
        try:
            manifest = self._read_manifest(manifest_path)
        except ValueError as error:
            self._raise_conflict(
                manifest_path, request_hash, "manifest is unreadable", error
            )
        if (
            manifest["source"] != source
            or manifest["endpoint"] != endpoint
            or manifest["request_hash"] != request_hash
            or manifest["request"] != dict(request)
        ):
            self._raise_conflict(
                manifest_path, request_hash, "manifest does not match its request"
            )
        content_hash = str(manifest["current_content_hash"])
        entry = self._entry_for(manifest, content_hash)
        return PublishedPartition(
            source=source,
            endpoint=endpoint,
            request=request,
            retrieved_at=_RawManifestSupport.parse_retrieved_at(
                str(entry["retrieved_at"])
            ),
            data_path=manifest_path.parent / f"{content_hash}.parquet",
            manifest_path=manifest_path,
            request_hash=request_hash,
            content_hash=content_hash,
            schema_fingerprint=str(entry["schema_fingerprint"]),
            row_count=_RawManifestSupport.entry_int(entry, "row_count"),
        )

    def list_current(
        self, source: str, endpoint: str
    ) -> tuple[PublishedPartition, ...]:
        """列出指定供应商端点下已校验的当前请求头。

        入参：
            source：供应商标识。
            endpoint：供应商原生端点名称。
        返回值：
            返回按确定性顺序列出当前值后的当前值（``tuple[PublishedPartition, ...]``）。
        异常：
            TypeError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        self._validate_path_segment(source, "source", _SOURCE_OR_ENDPOINT)
        self._validate_path_segment(endpoint, "endpoint", _SOURCE_OR_ENDPOINT)
        directory = self._raw_root / f"source={source}" / f"endpoint={endpoint}"
        if not directory.is_dir():
            return ()
        validate_storage_path(self._raw_root, directory)
        partitions: list[PublishedPartition] = []
        for request_dir in sorted(directory.iterdir(), key=lambda item: item.name):
            if not request_dir.is_dir() or not re.fullmatch(
                r"[0-9a-f]{64}", request_dir.name
            ):
                continue
            manifest_path = request_dir / _MANIFEST_FILE
            if not manifest_path.is_file():
                continue
            manifest = self._read_manifest(manifest_path)
            request = manifest["request"]
            if not isinstance(request, Mapping):
                raise TypeError("raw manifest request is invalid")
            partition = self.find_by_request(source, endpoint, request)
            if partition is not None:
                partitions.append(partition)
        return tuple(partitions)

    def read(self, partition: PublishedPartition, *, verify: bool = True) -> pa.Table:
        """读取已发布 Raw 分区，并按需重新校验内容。

        入参：
            partition：待读取、校验或映射的分区。
            verify：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回读取并校验Canonical 数据后的``read``（``pa.Table``）。
        异常：
            ValueError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        validate_storage_path(self._raw_root, partition.data_path, require_file=True)
        table = pq.read_table(partition.data_path)
        if verify and (
            self._content_hash(table) != partition.content_hash
            or self._schema_fingerprint(table.schema) != partition.schema_fingerprint
            or table.num_rows != partition.row_count
        ):
            raise ValueError("raw partition data fails integrity checks")
        return table

    def verify_managed_partition(self, partition: PublishedPartition) -> None:
        """确认目录分区属于当前存储且内容完整。

        入参：
            partition：待读取、校验或映射的分区。
        返回值：
            无。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        validate_storage_path(self._raw_root, partition.data_path, require_file=True)
        validate_storage_path(
            self._raw_root, partition.manifest_path, require_file=True
        )
        self.verify_partition(partition)

    @staticmethod
    def verify_partition(partition: PublishedPartition) -> None:
        """校验 Raw 分区布局、manifest 条目与文件内容。

        入参：
            partition：待读取、校验或映射的分区。
        返回值：
            无。
        异常：
            ValueError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        data_path = partition.data_path
        manifest_path = partition.manifest_path
        request_dir = data_path.parent
        if (
            request_dir != manifest_path.parent
            or request_dir.name != partition.request_hash
            or request_dir.parent.name != f"endpoint={partition.endpoint}"
            or request_dir.parent.parent.name != f"source={partition.source}"
            or data_path.name != f"{partition.content_hash}.parquet"
            or manifest_path.name != _MANIFEST_FILE
        ):
            raise ValueError("raw partition path does not use the canonical layout")
        manifest = RawPartitionStore._read_manifest(manifest_path)
        if (
            manifest["source"] != partition.source
            or manifest["endpoint"] != partition.endpoint
            or manifest["request_hash"] != partition.request_hash
        ):
            raise ValueError("raw partition manifest does not match its request")
        entry = RawPartitionStore._entry_for(manifest, partition.content_hash)
        if (
            str(entry["schema_fingerprint"]) != partition.schema_fingerprint
            or _RawManifestSupport.entry_int(entry, "row_count") != partition.row_count
            or str(entry["retrieved_at"])
            != partition.retrieved_at.astimezone(UTC).isoformat()
        ):
            raise ValueError(
                "raw partition manifest entry does not match its partition"
            )
        table = pq.read_table(data_path)
        if (
            RawPartitionStore._content_hash(table) != partition.content_hash
            or RawPartitionStore._schema_fingerprint(table.schema)
            != partition.schema_fingerprint
            or table.num_rows != partition.row_count
        ):
            raise ValueError("raw partition data fails integrity checks")

    def _ensure_data_file(
        self,
        data_path: Path,
        request_dir: Path,
        table: pa.Table,
        content_hash: str,
        schema_fingerprint: str,
        row_count: int,
        *,
        batch: RawBatch,
        request_hash: str,
    ) -> None:
        if data_path.exists():
            return
        temporary = request_dir / f".{uuid.uuid4().hex}.parquet.tmp"
        try:
            validate_storage_path(self._raw_root, temporary)
            pq.write_table(table, temporary, compression="zstd")
            validate_storage_path(self._raw_root, temporary, require_file=True)
            validate_storage_path(self._raw_root, data_path)
            temporary.replace(data_path)
            validate_storage_path(self._raw_root, data_path, require_file=True)
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _manifest_entry(
        batch: RawBatch,
        content_hash: str,
        schema_fingerprint: str,
        row_count: int,
    ) -> dict[str, object]:
        retrieved_at = batch.retrieved_at.astimezone(UTC)
        return {
            "content_hash": content_hash,
            "ingest_date": retrieved_at.date().isoformat(),
            "retrieved_at": retrieved_at.isoformat(),
            "schema_fingerprint": schema_fingerprint,
            "row_count": row_count,
        }

    def _write_manifest(self, manifest_path: Path, manifest: dict[str, object]) -> None:
        temporary = manifest_path.parent / f".{uuid.uuid4().hex}.manifest.tmp"
        try:
            validate_storage_path(self._raw_root, temporary)
            temporary.write_text(
                json.dumps(
                    manifest,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                encoding="utf-8",
            )
            validate_storage_path(self._raw_root, temporary, require_file=True)
            validate_storage_path(self._raw_root, manifest_path)
            temporary.replace(manifest_path)
            validate_storage_path(self._raw_root, manifest_path, require_file=True)
        finally:
            temporary.unlink(missing_ok=True)

    @classmethod
    def _read_manifest(cls, manifest_path: Path) -> dict[str, object]:
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("raw manifest is unreadable") from error
        if not isinstance(manifest, Mapping) or set(manifest) != _MANIFEST_FIELDS:
            raise ValueError("raw manifest structure is invalid")
        files = manifest["files"]
        if not isinstance(files, list):
            raise TypeError("raw manifest files is invalid")
        for item in files:
            if not isinstance(item, Mapping) or set(item) != _ENTRY_FIELDS:
                raise TypeError("raw manifest file entry is invalid")
        if not isinstance(manifest["request"], Mapping):
            raise TypeError("raw manifest request is invalid")
        return dict(manifest)

    @staticmethod
    def _raise_if_manifest_mismatch(
        manifest: dict[str, object],
        batch: RawBatch,
        request_hash: str,
    ) -> None:
        if (
            manifest["source"] != batch.source
            or manifest["endpoint"] != batch.endpoint
            or manifest["request_hash"] != request_hash
            or manifest["request"] != dict(batch.request)
        ):
            raise ValueError("raw manifest does not match its request")

    @staticmethod
    def _entry_for(manifest: dict[str, object], content_hash: str) -> dict[str, object]:
        for item in _RawManifestSupport.entry_list(manifest):
            if item["content_hash"] == content_hash:
                return dict(item)
        raise ValueError("raw manifest has no entry for the requested content")

    @staticmethod
    def _identity_lock_path(dataset_dir: Path, request_hash: str) -> Path:
        identity = hashlib.sha256(request_hash.encode()).hexdigest()
        return dataset_dir / f".{identity}.lock"

    @staticmethod
    def _validate_path_segment(
        value: str, label: str, pattern: re.Pattern[str]
    ) -> None:
        if not pattern.fullmatch(value):
            raise ValueError(f"{label} contains unsupported characters")

    @staticmethod
    def _table_from_batch(batch: RawBatch) -> pa.Table:
        if len(set(batch.schema)) != len(batch.schema):
            raise ValueError("schema must not contain duplicate column names")
        expected_keys = set(batch.schema)
        for row in batch.rows:
            if set(row) != expected_keys:
                raise ValueError("row keys must match schema exactly")
        try:
            arrays = [
                pa.array(
                    [row[column] for row in batch.rows],
                    type=pa.string(),
                )
                for column in batch.schema
            ]
        except (pa.ArrowException, TypeError, ValueError) as error:
            raise ValueError("raw values must be strings or null") from error
        return pa.Table.from_arrays(arrays, names=batch.schema)

    @staticmethod
    def _content_hash(table: pa.Table) -> str:
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()

    @staticmethod
    def _schema_fingerprint(schema: pa.Schema) -> str:
        return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()

    @classmethod
    def schema_fingerprint_for_fields(cls, fields: Sequence[str]) -> str:
        """计算供应商原生字符串字段集合的 Raw Schema 指纹。

        入参：
            fields：需要读取、映射或计算的字段集合。
        返回值：
            返回研究指纹``for``字段集合（``str``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        schema = pa.schema([pa.field(field, pa.string()) for field in fields])
        return cls._schema_fingerprint(schema)

    @classmethod
    def _verify_data_file(
        cls,
        path: Path,
        *,
        content_hash: str,
        schema_fingerprint: str,
        row_count: int,
    ) -> None:
        table = pq.read_table(path)
        if (
            cls._content_hash(table) != content_hash
            or cls._schema_fingerprint(table.schema) != schema_fingerprint
            or table.num_rows != row_count
        ):
            raise ValueError("written Parquet file did not pass integrity verification")

    @staticmethod
    def _raise_conflict(
        manifest_path: Path,
        request_hash: str,
        reason: str,
        cause: Exception | None = None,
    ) -> Never:
        detail = ErrorDetail(
            code="raw_partition_conflict",
            severity=Severity.SEVERE,
            message=f"raw partition already exists: {reason}",
            context={
                "manifest_path": str(manifest_path),
                "request_hash": request_hash,
            },
            remediation="publish a distinct partition or investigate the existing data",
            retryable=False,
        )
        if cause is None:
            raise QuantError(detail)
        raise QuantError(detail) from cause


_MANIFEST_FIELDS = {
    "current_content_hash",
    "endpoint",
    "files",
    "source",
    "request",
    "request_hash",
}
_ENTRY_FIELDS = {
    "content_hash",
    "ingest_date",
    "retrieved_at",
    "row_count",
    "schema_fingerprint",
}
_MANIFEST_FILE = "manifest.json"
_INSTRUMENT_MANIFEST_FILE_LIMIT = 20


class _RawManifestSupport:
    """集中解析 Raw manifest 中的强类型字段。"""

    @staticmethod
    def parse_retrieved_at(value: str) -> datetime:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("retrieval timestamp is not timezone-aware")
        return parsed.astimezone(UTC)

    @staticmethod
    def entry_list(manifest: dict[str, object]) -> list[dict[str, object]]:
        files = manifest["files"]
        if not isinstance(files, list):
            raise TypeError("raw manifest files is invalid")
        entries: list[dict[str, object]] = []
        for item in files:
            if not isinstance(item, Mapping):
                raise TypeError("raw manifest file entry is invalid")
            entries.append(dict(item))
        return entries

    @staticmethod
    def entry_int(entry: dict[str, object], field: str) -> int:
        value = entry[field]
        if type(value) is not int:
            raise ValueError(f"raw manifest {field} must be an integer")
        return value
