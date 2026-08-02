"""Canonical experiment, source-code, and dependency identities."""

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

from quant_core.data.contracts import JsonValue, canonical_json_bytes

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_GIT_OID = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_FINGERPRINT_SCHEMA = "quant.experiment.fingerprint.v1"
_SOURCE_IDENTITY_SCHEMA = "quant.source-identity.v1"
_TREE_HASH_DOMAIN = b"quant.source-tree.v1\0"


@dataclass(frozen=True, slots=True)
class ExperimentFingerprintInput:
    """Every independently versioned domain that defines one research identity."""

    strategy_id: str
    strategy_version: str
    resolved_config: Mapping[str, JsonValue]
    snapshot_manifest_hash: str
    source_hash: str
    lockfile_hash: str
    rulebook_version: str

    def __post_init__(self) -> None:
        for name in ("strategy_id", "strategy_version", "rulebook_version"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be nonempty")
        if not isinstance(self.resolved_config, Mapping):
            raise TypeError("resolved_config must be a mapping")
        canonical_json_bytes(self.resolved_config)
        for name in (
            "snapshot_manifest_hash",
            "source_hash",
            "lockfile_hash",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not _SHA256.fullmatch(value):
                raise ValueError(f"{name} must be a lowercase SHA-256 hex digest")


@dataclass(frozen=True, slots=True)
class SourceIdentity:
    """A commit identity for clean Git or a deterministic current source tree."""

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
            raise ValueError(
                "source_tree_hash must be a lowercase SHA-256 hex digest"
            )
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
        expected_hash = _source_identity_hash(
            self.mode, self.git_commit, self.source_tree_hash
        )
        if self.source_hash != expected_hash:
            raise ValueError("source_hash does not match the disclosed source identity")


@dataclass(frozen=True, slots=True)
class SourceTreeSpec:
    """Versioned, explicit no-Git source-file inclusion contract."""

    schema_version: int
    include: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != 1:
            raise ValueError("source tree schema_version must be 1")
        if not isinstance(self.include, tuple) or not self.include:
            raise ValueError("source tree include must be a nonempty tuple")
        normalized: list[str] = []
        for value in self.include:
            if not isinstance(value, str):
                raise TypeError("source tree include entries must be strings")
            path = PurePosixPath(value)
            if (
                not value
                or path.is_absolute()
                or ".." in path.parts
                or not path.parts
                or path.as_posix() != value
            ):
                raise ValueError("source tree include contains an unsafe path")
            normalized.append(value)
        if tuple(sorted(set(normalized))) != self.include:
            raise ValueError("source tree include must be sorted and unique")


def compute_fingerprint(value: ExperimentFingerprintInput) -> str:
    """Return the canonical, domain-separated SHA-256 experiment fingerprint."""
    if not isinstance(value, ExperimentFingerprintInput):
        raise TypeError("value must be an ExperimentFingerprintInput")
    payload: dict[str, JsonValue] = {
        "schema": _FINGERPRINT_SCHEMA,
        "strategy_id": value.strategy_id,
        "strategy_version": value.strategy_version,
        "resolved_config": value.resolved_config,
        "snapshot_manifest_hash": value.snapshot_manifest_hash,
        "source_hash": value.source_hash,
        "lockfile_hash": value.lockfile_hash,
        "rulebook_version": value.rulebook_version,
    }
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def resolve_source_identity(
    source_root: Path, *, source_tree_spec: SourceTreeSpec | None = None
) -> SourceIdentity:
    """Resolve a source identity from an explicit repository or tree root."""
    root = _source_root(source_root)
    repository = _git_repository_root(root)
    if repository is not None:
        head = _run_git(repository, "rev-parse", "--verify", "HEAD")
        if head.returncode != 0:
            if _is_unborn_repository(repository):
                return _source_tree_identity(root, source_tree_spec)
            _raise_git_command_failure(head, "rev-parse --verify HEAD")
        commit = _decode_git_text(head.stdout, "Git commit identity").strip()
        if not _GIT_OID.fullmatch(commit):
            raise ValueError("Git returned an invalid full commit OID")
        dirty = bool(
            _git_output(
                repository,
                "status",
                "--porcelain=v1",
                "-z",
                "--untracked-files=no",
            )
        )
        if not dirty:
            source_hash = _source_identity_hash("git_commit", commit, None)
            return SourceIdentity("git_commit", source_hash, commit, None, False)
        tracked = _git_output(repository, "ls-files", "-z", "--cached").split(
            b"\0"
        )
        tracked_paths = tuple(path for path in tracked if path)
        tree_hash = _hash_tree(repository, tracked_paths)
        source_hash = _source_identity_hash("source_tree", commit, tree_hash)
        return SourceIdentity("source_tree", source_hash, commit, tree_hash, True)

    return _source_tree_identity(root, source_tree_spec)


def hash_lockfile(lockfile_path: Path) -> str:
    """Hash the actual lockfile bytes, failing closed when evidence is absent."""
    if not isinstance(lockfile_path, Path):
        raise TypeError("lockfile_path must be a Path")
    if not lockfile_path.is_file():
        raise ValueError("lockfile must be an existing file")
    return _hash_file(lockfile_path)


def capture_environment(
    source_root: Path,
    lockfile_path: Path,
    *,
    source_tree_spec: SourceTreeSpec | None = None,
) -> dict[str, JsonValue]:
    """Return finite canonical environment disclosure without ambient variables."""
    root = _source_root(source_root)
    if not isinstance(lockfile_path, Path):
        raise TypeError("lockfile_path must be a Path")
    try:
        resolved_lockfile = lockfile_path.resolve(strict=True)
        relative_lockfile = resolved_lockfile.relative_to(root)
    except (OSError, ValueError) as error:
        raise ValueError("lockfile must exist inside the explicit source root") from error
    identity = resolve_source_identity(root, source_tree_spec=source_tree_spec)
    environment: dict[str, JsonValue] = {
        "schema_version": 1,
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


def _source_root(source_root: Path) -> Path:
    if not isinstance(source_root, Path):
        raise TypeError("source_root must be a Path")
    try:
        status = source_root.lstat()
        root = source_root.resolve(strict=True)
    except OSError as error:
        raise ValueError("source root must be an existing directory") from error
    if _is_reparse(status) or not stat.S_ISDIR(status.st_mode):
        raise ValueError("source root must be a plain non-reparse directory")
    return root


def _git_repository_root(root: Path) -> Path | None:
    try:
        completed = _run_git(root, "rev-parse", "--show-toplevel")
    except _GitUnavailable:
        return None
    if completed.returncode != 0:
        message = _decode_git_text(completed.stderr, "Git error output").strip()
        if "not a git repository" in message.lower():
            return None
        _raise_git_command_failure(completed, "rev-parse --show-toplevel")
    try:
        repository_text = _decode_git_text(
            completed.stdout, "Git repository path"
        ).strip()
        repository = Path(repository_text).resolve(strict=True)
    except OSError as error:
        raise ValueError("Git returned an inaccessible repository root") from error
    return repository if repository == root else None


def _source_tree_identity(
    root: Path, source_tree_spec: SourceTreeSpec | None
) -> SourceIdentity:
    if source_tree_spec is None:
        raise ValueError("an explicit versioned source tree spec is required")
    paths = tuple(value.encode("utf-8") for value in source_tree_spec.include)
    tree_hash = _hash_tree(root, paths)
    source_hash = _source_identity_hash("source_tree", None, tree_hash)
    return SourceIdentity("source_tree", source_hash, None, tree_hash, False)


def _run_git(root: Path, *arguments: str) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            ["git", "-C", str(root), *arguments],
            check=False,
            capture_output=True,
        )
    except OSError as error:
        raise _GitUnavailable("Git executable is unavailable") from error


def _git_output(root: Path, *arguments: str) -> bytes:
    completed = _run_git(root, *arguments)
    if completed.returncode != 0:
        _raise_git_command_failure(completed, " ".join(arguments))
    return completed.stdout


def _hash_tree(root: Path, raw_paths: Sequence[bytes]) -> str:
    digest = hashlib.sha256(_TREE_HASH_DOMAIN)
    for raw_path in sorted(raw_paths):
        try:
            posix = PurePosixPath(raw_path.decode("utf-8"))
        except UnicodeDecodeError as error:
            raise ValueError("source tree paths must be UTF-8") from error
        if posix.is_absolute() or ".." in posix.parts or not posix.parts:
            raise ValueError("source tree contains an unsafe path")
        relative = Path(*posix.parts)
        path = root / relative
        _hash_piece(digest, b"path", posix.as_posix().encode("utf-8"))
        status = _safe_source_lstat(root, relative)
        if status is None:
            _hash_piece(digest, b"missing", b"")
        elif stat.S_ISREG(status.st_mode):
            _hash_piece(digest, b"file", bytes.fromhex(_hash_file(path)))
        else:
            raise ValueError("source tree contains an unsupported tracked path")
    return digest.hexdigest()


def _source_identity_hash(
    mode: str, git_commit: str | None, source_tree_hash: str | None
) -> str:
    payload: dict[str, JsonValue] = {
        "schema": _SOURCE_IDENTITY_SCHEMA,
        "mode": mode,
        "git_commit": git_commit,
        "source_tree_hash": source_tree_hash,
    }
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _hash_piece(digest: _Hash, label: bytes, value: bytes) -> None:
    digest.update(len(label).to_bytes(2, "big"))
    digest.update(label)
    digest.update(len(value).to_bytes(8, "big"))
    digest.update(value)


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
        if _is_reparse(status):
            raise ValueError("source tree contains a symlink or reparse point")
        if index < len(relative.parts) - 1 and not stat.S_ISDIR(status.st_mode):
            raise ValueError("source tree path has a non-directory parent")
    return status


def _is_reparse(status: os.stat_result) -> bool:
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return stat.S_ISLNK(status.st_mode) or bool(
        getattr(status, "st_file_attributes", 0) & reparse_flag
    )


def _decode_git_text(payload: bytes, label: str) -> str:
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError(f"{label} must be UTF-8") from error


def _raise_git_command_failure(
    completed: subprocess.CompletedProcess[bytes], command: str
) -> None:
    message = _decode_git_text(completed.stderr, "Git error output").strip()
    raise ValueError(f"Git command failed ({command}): {message or completed.returncode}")


def _is_unborn_repository(root: Path) -> bool:
    history = _run_git(root, "rev-list", "--max-count=1", "--all")
    if history.returncode != 0:
        _raise_git_command_failure(history, "rev-list --max-count=1 --all")
    _git_output(root, "status", "--porcelain=v1", "-z", "--untracked-files=no")
    return not history.stdout.strip()


class _GitUnavailable(ValueError):
    """Raised only when Git cannot be executed at all."""


class _Hash(Protocol):
    """The small structural surface used from hashlib under strict typing."""

    def update(self, value: bytes) -> object: ...
