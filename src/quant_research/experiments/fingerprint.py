"""提供实验与实验指纹相关的公开模型、协议与处理流程。"""

from __future__ import annotations

import hashlib
import os
import platform
import re
import stat
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Protocol

from quant_research.data.contracts import JsonValue, canonical_json_bytes

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_GIT_OID = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_TREE_HASH_DOMAIN = b"quant.source-tree\0"


@dataclass(frozen=True, slots=True)
class ExperimentFingerprintInput:
    """表示实验流程中的实验研究指纹输入及其业务不变量。

    入参：
        strategy_id：用于持久化关联和日志追踪的策略标识。
        resolved_config：参与本次处理的解析后配置配置；调用方不得依赖未声明的顺序。
        data_hash：Canonical 数据内容或本次研究输入的数据身份。
        source_hash：参与计算的实现源码身份。
        lockfile_hash：参与幂等、漂移或完整性校验的依赖锁文件哈希；使用 SHA-256 十六进制文本。
        rulebook_hash：唯一 A 股交易规则文件的内容身份。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    Every independent domain that defines one research identity.
    """

    strategy_id: str
    resolved_config: Mapping[str, JsonValue]
    data_hash: str
    source_hash: str
    lockfile_hash: str
    rulebook_hash: str

    def __post_init__(self) -> None:
        if not isinstance(self.strategy_id, str) or not self.strategy_id.strip():
            raise ValueError("strategy_id must be nonempty")
        if not isinstance(self.resolved_config, Mapping):
            raise TypeError("resolved_config must be a mapping")
        canonical_json_bytes(self.resolved_config)
        for name in (
            "data_hash",
            "source_hash",
            "lockfile_hash",
            "rulebook_hash",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not _SHA256.fullmatch(value):
                raise ValueError(f"{name} must be a lowercase SHA-256 hex digest")


@dataclass(frozen=True, slots=True)
class SourceIdentity:
    """记录干净 Git 提交或当前源码树形成的可复现代码身份。

    入参：
        mode：``mode``。
        source_hash：参与计算的实现源码身份。
        git_commit：Git提交。
        source_tree_hash：参与幂等、漂移或完整性校验的数据来源``tree``哈希；使用 SHA-256 十六进制文本。
        返回完成字段规范化和不变量校验的对象。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    A commit identity for clean Git or a deterministic current source tree.
    """

    mode: str
    source_hash: str
    git_commit: str | None
    source_tree_hash: str | None
    working_tree_dirty: bool

    def __post_init__(self) -> None:
        if self.mode not in {"git_commit", "source_tree"}:
            raise ValueError("source identity mode is invalid")
        if not _SHA256.fullmatch(self.source_hash):
            raise ValueError("source_hash must be a lowercase SHA-256 hex digest")
        if self.git_commit is not None and not _GIT_OID.fullmatch(self.git_commit):
            raise ValueError("git_commit must be a full lowercase Git OID")
        if self.source_tree_hash is not None and not _SHA256.fullmatch(
            self.source_tree_hash
        ):
            raise ValueError("source_tree_hash must be a lowercase SHA-256 hex digest")
        if type(self.working_tree_dirty) is not bool:
            raise TypeError("working_tree_dirty must be a boolean")
        if self.mode == "git_commit" and (
            self.git_commit is None
            or self.source_tree_hash is not None
            or self.working_tree_dirty
        ):
            raise ValueError("clean Git identity fields are inconsistent")
        if self.mode == "source_tree" and self.source_tree_hash is None:
            raise ValueError("source tree identity requires source_tree_hash")
        if self.mode == "source_tree" and (
            (self.git_commit is None and self.working_tree_dirty)
            or (self.git_commit is not None and not self.working_tree_dirty)
        ):
            raise ValueError("source tree identity fields are inconsistent")
        expected_hash = _FingerprintSupport._source_identity_hash(
            self.mode, self.git_commit, self.source_tree_hash
        )
        if self.source_hash != expected_hash:
            raise ValueError("source_hash does not match the disclosed source identity")


@dataclass(frozen=True, slots=True)
class SourceTreeSpec:
    """定义可持久化并参与身份计算的数据来源``tree``不可变规格。

    入参：
        include：参与本次处理的包含范围；调用方不得依赖未声明的顺序。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    Explicit no-Git source-file inclusion contract.
    """

    include: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.include, tuple) or not self.include:
            raise ValueError("source tree include must be a nonempty tuple")
        normalized: list[str] = []
        for value in self.include:
            if not isinstance(value, str):
                raise TypeError("source tree include entries must be strings")
            _FingerprintSupport._portable_source_path(value, "source tree include")
            normalized.append(value)
        if tuple(sorted(set(normalized))) != self.include:
            raise ValueError("source tree include must be sorted and unique")


def compute_fingerprint(value: ExperimentFingerprintInput) -> str:
    """计算研究指纹；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        value：待校验或转换的值，类型为 ``ExperimentFingerprintInput``。
    返回值：
        返回计算研究指纹后的研究指纹（``str``）。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    Return the canonical, domain-separated SHA-256 experiment fingerprint.
    """
    if not isinstance(value, ExperimentFingerprintInput):
        raise TypeError("value must be an ExperimentFingerprintInput")
    payload: dict[str, JsonValue] = {
        "strategy_id": value.strategy_id,
        "resolved_config": value.resolved_config,
        "data_hash": value.data_hash,
        "source_hash": value.source_hash,
        "lockfile_hash": value.lockfile_hash,
        "rulebook_hash": value.rulebook_hash,
    }
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def resolve_source_identity(
    source_root: Path, *, source_tree_spec: SourceTreeSpec | None = None
) -> SourceIdentity:
    """解析并返回确定结果；该函数作为稳定公开 API保留在模块级。

    入参：
        source_root：所有派生路径必须位于其中的数据来源可信根目录。
        source_tree_spec：数据来源源码树不可变规格。
    返回值：
        返回解析数据来源身份后的数据来源身份（``SourceIdentity``）。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Resolve a source identity from an explicit repository or tree root.
    """
    root = _FingerprintSupport._source_root(source_root)
    repository = _FingerprintSupport._git_repository_root(root)
    if repository is not None:
        head = _FingerprintSupport._run_git(repository, "rev-parse", "--verify", "HEAD")
        if head.returncode != 0:
            if _FingerprintSupport._is_unborn_repository(repository):
                return _FingerprintSupport._source_tree_identity(root, source_tree_spec)
            _FingerprintSupport._raise_git_command_failure(
                head, "rev-parse --verify HEAD"
            )
        commit = _FingerprintSupport._decode_git_text(
            head.stdout, "Git commit identity"
        ).strip()
        if not _GIT_OID.fullmatch(commit):
            raise ValueError("Git returned an invalid full commit OID")
        dirty = bool(
            _FingerprintSupport._git_output(
                repository,
                "status",
                "--porcelain=v1",
                "-z",
                "--untracked-files=no",
            )
        )
        if not dirty:
            source_hash = _FingerprintSupport._source_identity_hash(
                "git_commit", commit, None
            )
            return SourceIdentity("git_commit", source_hash, commit, None, False)
        tracked = _FingerprintSupport._git_output(
            repository, "ls-files", "-z", "--cached"
        ).split(b"\0")
        tracked_paths = tuple(path for path in tracked if path)
        tree_hash = _FingerprintSupport._hash_tree(repository, tracked_paths)
        source_hash = _FingerprintSupport._source_identity_hash(
            "source_tree", commit, tree_hash
        )
        return SourceIdentity("source_tree", source_hash, commit, tree_hash, True)

    return _FingerprintSupport._source_tree_identity(root, source_tree_spec)


def hash_lockfile(lockfile_path: Path) -> str:
    """计算依赖锁文件；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        lockfile_path：经可信根边界校验后使用的依赖锁文件路径。
    返回值：
        返回依赖锁文件（``str``）。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    Hash the actual lockfile bytes, failing closed when evidence is absent.
    """
    if not isinstance(lockfile_path, Path):
        raise TypeError("lockfile_path must be a Path")
    if not lockfile_path.is_file():
        raise ValueError("lockfile must be an existing file")
    return _FingerprintSupport._hash_file(lockfile_path)


def capture_environment(
    source_root: Path,
    lockfile_path: Path,
    *,
    source_tree_spec: SourceTreeSpec | None = None,
) -> dict[str, JsonValue]:
    """捕获运行环境；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        source_root：所有派生路径必须位于其中的数据来源可信根目录。
        lockfile_path：经可信根边界校验后使用的依赖锁文件路径。
        source_tree_spec：数据来源源码树不可变规格。
    返回值：
        返回运行环境（``dict[str, JsonValue]``）。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    Return finite canonical environment disclosure without ambient variables.
    """
    root = _FingerprintSupport._source_root(source_root)
    if not isinstance(lockfile_path, Path):
        raise TypeError("lockfile_path must be a Path")
    try:
        resolved_lockfile = lockfile_path.resolve(strict=True)
        relative_lockfile = resolved_lockfile.relative_to(root)
    except (OSError, ValueError) as error:
        raise ValueError(
            "lockfile must exist inside the explicit source root"
        ) from error
    identity = resolve_source_identity(root, source_tree_spec=source_tree_spec)
    environment: dict[str, JsonValue] = {
        "source_identity_mode": identity.mode,
        "source_hash": identity.source_hash,
        "git_commit": identity.git_commit,
        "source_tree_hash": identity.source_tree_hash,
        "working_tree_dirty": identity.working_tree_dirty,
        "lockfile_path": relative_lockfile.as_posix(),
        "lockfile_hash": hash_lockfile(resolved_lockfile),
        "python_version": platform.python_version(),
    }
    canonical_json_bytes(environment)
    return environment


class _FingerprintSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _source_root(source_root: Path) -> Path:
        if not isinstance(source_root, Path):
            raise TypeError("source_root must be a Path")
        try:
            status = source_root.lstat()
            root = source_root.resolve(strict=True)
        except OSError as error:
            raise ValueError("source root must be an existing directory") from error
        if _FingerprintSupport._is_reparse(status) or not stat.S_ISDIR(status.st_mode):
            raise ValueError("source root must be a plain non-reparse directory")
        return root

    @staticmethod
    def _git_repository_root(root: Path) -> Path | None:
        try:
            completed = _FingerprintSupport._run_git(
                root, "rev-parse", "--show-toplevel"
            )
        except _GitUnavailable:
            return None
        if completed.returncode != 0:
            message = _FingerprintSupport._decode_git_text(
                completed.stderr, "Git error output"
            ).strip()
            if "not a git repository" in message.lower():
                return None
            _FingerprintSupport._raise_git_command_failure(
                completed, "rev-parse --show-toplevel"
            )
        try:
            repository_text = _FingerprintSupport._decode_git_text(
                completed.stdout, "Git repository path"
            ).strip()
            repository = Path(repository_text).resolve(strict=True)
        except OSError as error:
            raise ValueError("Git returned an inaccessible repository root") from error
        return repository if repository == root else None

    @staticmethod
    def _source_tree_identity(
        root: Path, source_tree_spec: SourceTreeSpec | None
    ) -> SourceIdentity:
        if source_tree_spec is None:
            raise ValueError("an explicit versioned source tree spec is required")
        paths = tuple(value.encode("utf-8") for value in source_tree_spec.include)
        tree_hash = _FingerprintSupport._hash_tree(root, paths)
        source_hash = _FingerprintSupport._source_identity_hash(
            "source_tree", None, tree_hash
        )
        return SourceIdentity("source_tree", source_hash, None, tree_hash, False)

    @staticmethod
    def _run_git(root: Path, *arguments: str) -> subprocess.CompletedProcess[bytes]:
        try:
            return subprocess.run(
                ["git", "-C", str(root), *arguments],
                check=False,
                capture_output=True,
            )
        except FileNotFoundError as error:
            raise _GitUnavailable("Git executable is unavailable") from error
        except OSError as error:
            raise ValueError("Git command could not start") from error

    @staticmethod
    def _git_output(root: Path, *arguments: str) -> bytes:
        completed = _FingerprintSupport._run_git(root, *arguments)
        if completed.returncode != 0:
            _FingerprintSupport._raise_git_command_failure(
                completed, " ".join(arguments)
            )
        return completed.stdout

    @staticmethod
    def _hash_tree(root: Path, raw_paths: Sequence[bytes]) -> str:
        digest = hashlib.sha256(_TREE_HASH_DOMAIN)
        for raw_path in sorted(raw_paths):
            try:
                decoded = raw_path.decode("utf-8")
            except UnicodeDecodeError as error:
                raise ValueError("source tree paths must be UTF-8") from error
            posix = _FingerprintSupport._portable_source_path(decoded, "source tree")
            relative = Path(*posix.parts)
            path = _FingerprintSupport._join_source_path(root, relative)
            _FingerprintSupport._hash_piece(
                digest, b"path", posix.as_posix().encode("utf-8")
            )
            status = _FingerprintSupport._safe_source_lstat(root, relative)
            if status is None:
                _FingerprintSupport._hash_piece(digest, b"missing", b"")
            elif stat.S_ISREG(status.st_mode):
                _FingerprintSupport._hash_piece(
                    digest, b"file", bytes.fromhex(_FingerprintSupport._hash_file(path))
                )
            else:
                raise ValueError("source tree contains an unsupported tracked path")
        return digest.hexdigest()

    @staticmethod
    def _portable_source_path(value: str, label: str) -> PurePosixPath:
        """Parse one portable, slash-only, relative source path without normalization."""
        segments = value.split("/")
        if (
            not value
            or "\\" in value
            or ":" in value
            or value.startswith("/")
            or any(segment in {"", ".", ".."} for segment in segments)
        ):
            raise ValueError(f"{label} contains an unsafe path")
        path = PurePosixPath(*segments)
        if path.is_absolute() or path.as_posix() != value:
            raise ValueError(f"{label} contains an unsafe path")
        return path

    @staticmethod
    def _join_source_path(root: Path, relative: Path) -> Path:
        candidate = root.joinpath(*relative.parts)
        try:
            common = Path(os.path.commonpath((root, candidate)))
        except ValueError as error:
            raise ValueError("source tree path is outside source root") from error
        if common != root:
            raise ValueError("source tree path is outside source root")
        return candidate

    @staticmethod
    def _source_identity_hash(
        mode: str, git_commit: str | None, source_tree_hash: str | None
    ) -> str:
        payload: dict[str, JsonValue] = {
            "mode": mode,
            "git_commit": git_commit,
            "source_tree_hash": source_tree_hash,
        }
        return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()

    @staticmethod
    def _hash_piece(digest: _Hash, label: bytes, value: bytes) -> None:
        digest.update(len(label).to_bytes(2, "big"))
        digest.update(label)
        digest.update(len(value).to_bytes(8, "big"))
        digest.update(value)

    @staticmethod
    def _hash_file(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _safe_source_lstat(root: Path, relative: Path) -> os.stat_result | None:
        current = root
        for index, part in enumerate(relative.parts):
            current /= part
            try:
                status = current.lstat()
            except FileNotFoundError:
                return None
            except OSError as error:
                raise ValueError("source tree path is inaccessible") from error
            if _FingerprintSupport._is_reparse(status):
                raise ValueError("source tree contains a symlink or reparse point")
            if index < len(relative.parts) - 1 and not stat.S_ISDIR(status.st_mode):
                raise ValueError("source tree path has a non-directory parent")
        return status

    @staticmethod
    def _is_reparse(status: os.stat_result) -> bool:
        reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
        return stat.S_ISLNK(status.st_mode) or bool(
            getattr(status, "st_file_attributes", 0) & reparse_flag
        )

    @staticmethod
    def _decode_git_text(payload: bytes, label: str) -> str:
        try:
            return payload.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValueError(f"{label} must be UTF-8") from error

    @staticmethod
    def _raise_git_command_failure(
        completed: subprocess.CompletedProcess[bytes], command: str
    ) -> None:
        message = _FingerprintSupport._decode_git_text(
            completed.stderr, "Git error output"
        ).strip()
        raise ValueError(
            f"Git command failed ({command}): {message or completed.returncode}"
        )

    @staticmethod
    def _is_unborn_repository(root: Path) -> bool:
        history = _FingerprintSupport._run_git(
            root, "rev-list", "--max-count=1", "--all"
        )
        if history.returncode != 0:
            _FingerprintSupport._raise_git_command_failure(
                history, "rev-list --max-count=1 --all"
            )
        _FingerprintSupport._git_output(
            root, "status", "--porcelain=v1", "-z", "--untracked-files=no"
        )
        return not history.stdout.strip()


class _GitUnavailable(ValueError):
    """Raised only when Git cannot be executed at all."""


class _Hash(Protocol):
    """The small structural surface used from hashlib under strict typing."""

    def update(self, value: bytes) -> object: ...
