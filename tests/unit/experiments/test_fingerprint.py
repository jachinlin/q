"""Stable experiment fingerprints and source-environment identity contracts."""

from __future__ import annotations

import json
import os
import re
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest
import yaml
from pydantic import BaseModel, ConfigDict

import quant_core.experiments.fingerprint as fingerprint_module
from quant_core.data.contracts import JsonValue, canonical_json_bytes
from quant_core.experiments.fingerprint import (
    ExperimentFingerprintInput,
    SourceTreeSpec,
    capture_environment,
    compute_fingerprint,
    hash_lockfile,
    resolve_source_identity,
)

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class _ResolvedConfig(BaseModel):
    """The strategy-owned model, rather than fingerprinting, resolves defaults."""

    model_config = ConfigDict(extra="forbid")

    universe: str
    rebalance_days: int = 5
    leverage: float = 1.0
    filters: dict[str, bool]


def _resolved(yaml_text: str) -> dict[str, JsonValue]:
    parsed = yaml.safe_load(yaml_text)
    return _ResolvedConfig.model_validate(parsed).model_dump(mode="json")


def _fingerprint_input(
    resolved_config: dict[str, JsonValue] | None = None,
) -> ExperimentFingerprintInput:
    return ExperimentFingerprintInput(
        strategy_id="quality-value",
        strategy_version="2.1.0",
        resolved_config=resolved_config
        or {
            "filters": {"exclude_st": True, "exclude_suspended": True},
            "leverage": 1.0,
            "rebalance_days": 5,
            "universe": "CSI300",
        },
        snapshot_manifest_hash="a" * 64,
        source_hash="b" * 64,
        lockfile_hash="c" * 64,
        rulebook_version="cn-a-v3",
    )


def _git(root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(root), *arguments],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return completed.stdout.strip()


def _committed_repo(root: Path) -> str:
    root.mkdir()
    _git(root, "init", "--quiet")
    _git(root, "config", "user.email", "task2@example.invalid")
    _git(root, "config", "user.name", "Task 2")
    _git(root, "config", "core.autocrlf", "false")
    (root / "strategy.py").write_bytes(b"SIGNAL = 1\n")
    (root / "package").mkdir()
    (root / "package" / "model.py").write_bytes(b"MODEL = 'v1'\n")
    _git(root, "add", "strategy.py", "package/model.py")
    _git(root, "commit", "--quiet", "-m", "fixture")
    return _git(root, "rev-parse", "HEAD")


def test_fingerprint_is_stable_after_strategy_model_resolves_yaml_variations() -> None:
    """Mapping order or YAML spelling must not alter one resolved configuration."""
    first = _resolved(
        """
        universe: CSI300
        filters: {exclude_st: true, exclude_suspended: true}
        """
    )
    second = _resolved(
        """
        filters:
          exclude_suspended: true
          exclude_st: true
        leverage: 1
        rebalance_days: 5
        universe: "CSI300"
        """
    )

    first_value = compute_fingerprint(_fingerprint_input(first))
    second_value = compute_fingerprint(_fingerprint_input(second))

    assert first == second
    assert first_value == second_value
    assert _SHA256.fullmatch(first_value)


@pytest.mark.parametrize(
    ("field", "changed"),
    [
        ("strategy_id", "quality-growth"),
        ("strategy_version", "2.1.1"),
        ("resolved_config", {"universe": "CSI500"}),
        ("snapshot_manifest_hash", "d" * 64),
        ("source_hash", "e" * 64),
        ("lockfile_hash", "f" * 64),
        ("rulebook_version", "cn-a-v4"),
    ],
)
def test_each_fingerprint_domain_changes_the_digest(field: str, changed: object) -> None:
    """Omitting any of the seven research identity domains would collide here."""
    baseline = _fingerprint_input()
    candidate = replace(baseline, **{field: changed})

    assert compute_fingerprint(candidate) != compute_fingerprint(baseline)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_fingerprint_rejects_nonfinite_nested_config_numbers(value: float) -> None:
    """JSON-incompatible research values must fail instead of receiving a digest."""
    with pytest.raises(ValueError, match="JSON serializable"):
        compute_fingerprint(_fingerprint_input({"nested": [1, {"invalid": value}]}))


@pytest.mark.parametrize(
    "field", ["strategy_id", "strategy_version", "rulebook_version"]
)
@pytest.mark.parametrize("invalid", ["", "   "])
def test_fingerprint_rejects_empty_identifiers(field: str, invalid: str) -> None:
    """Blank identity components cannot collapse distinct research identities."""
    with pytest.raises(ValueError, match=field):
        replace(_fingerprint_input(), **{field: invalid})


@pytest.mark.parametrize(
    "field", ["snapshot_manifest_hash", "source_hash", "lockfile_hash"]
)
@pytest.mark.parametrize("invalid", ["A" * 64, "a" * 63, "g" * 64])
def test_fingerprint_rejects_noncanonical_content_hashes(
    field: str, invalid: str
) -> None:
    """Malformed or uppercase hashes must never enter a reproducibility identity."""
    with pytest.raises(ValueError, match=field):
        replace(_fingerprint_input(), **{field: invalid})


def test_clean_git_uses_full_commit_identity_and_discloses_no_environment_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A clean checkout should bind its commit without copying ambient secrets."""
    root = tmp_path / "repo"
    commit = _committed_repo(root)
    lockfile = root / "uv.lock"
    lockfile.write_bytes(b"version = 1\n")
    _git(root, "add", "uv.lock")
    _git(root, "commit", "--quiet", "-m", "lock")
    commit = _git(root, "rev-parse", "HEAD")
    monkeypatch.setenv("TASK2_SECRET_TOKEN", "do-not-disclose-this-value")

    identity = resolve_source_identity(root)
    environment = capture_environment(root, lockfile)
    encoded = canonical_json_bytes(environment)

    assert identity.mode == "git_commit"
    assert identity.git_commit == commit
    assert identity.source_tree_hash is None
    assert identity.working_tree_dirty is False
    assert _SHA256.fullmatch(identity.source_hash)
    assert environment == {
        "schema_version": 1,
        "source_identity_mode": "git_commit",
        "source_hash": identity.source_hash,
        "git_commit": commit,
        "source_tree_hash": None,
        "working_tree_dirty": False,
        "lockfile_path": "uv.lock",
        "lockfile_hash": hash_lockfile(lockfile),
        "python_version": environment["python_version"],
    }
    assert environment["python_version"]
    assert b"do-not-disclose-this-value" not in encoded
    assert json.loads(encoded) == environment


def test_dirty_git_hashes_only_tracked_current_content(tmp_path: Path) -> None:
    """Tracked edits must bind identity while untracked runtime files stay excluded."""
    root = tmp_path / "repo"
    commit = _committed_repo(root)
    (root / "strategy.py").write_bytes(b"SIGNAL = 2\n")
    (root / "untracked-secret.txt").write_bytes(b"first secret")

    first = resolve_source_identity(root)
    (root / "untracked-secret.txt").write_bytes(b"different secret")
    (root / ".runtime-output").mkdir()
    (root / ".runtime-output" / "result.bin").write_bytes(b"runtime")
    second = resolve_source_identity(root)
    (root / "strategy.py").write_bytes(b"SIGNAL = 3\n")
    third = resolve_source_identity(root)

    assert first.mode == "source_tree"
    assert first.git_commit == commit
    assert first.working_tree_dirty is True
    assert _SHA256.fullmatch(first.source_tree_hash or "")
    assert first.source_hash == second.source_hash
    assert first.source_tree_hash == second.source_tree_hash
    assert third.source_hash != first.source_hash
    assert third.source_tree_hash != first.source_tree_hash


def test_untracked_runtime_files_do_not_change_clean_git_identity(tmp_path: Path) -> None:
    """Untracked diagnostics must not indirectly switch commit identity modes."""
    root = tmp_path / "repo"
    commit = _committed_repo(root)
    clean = resolve_source_identity(root)
    (root / ".staging-output").mkdir()
    (root / ".staging-output" / "run.log").write_bytes(b"runtime")
    (root / "untracked-secret.txt").write_bytes(b"not source evidence")

    with_untracked = resolve_source_identity(root)

    assert clean.mode == with_untracked.mode == "git_commit"
    assert clean.git_commit == with_untracked.git_commit == commit
    assert clean.source_hash == with_untracked.source_hash
    assert with_untracked.working_tree_dirty is False


def test_no_git_tree_hash_is_explicit_order_stable_and_ignores_dot_git(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fallback identity must use the supplied root, not traversal or CWD accidents."""
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    for root, order in (
        (first_root, ("b.py", "a.py")),
        (second_root, ("a.py", "b.py")),
    ):
        root.mkdir()
        for name in order:
            (root / name).write_bytes(f"{name}\n".encode())
        (root / ".git").mkdir()
        (root / ".git" / "runtime").write_bytes(order[0].encode())
    unrelated = tmp_path / "cwd"
    unrelated.mkdir()
    (unrelated / "noise.py").write_bytes(b"NOISE = True\n")
    monkeypatch.chdir(unrelated)

    spec = SourceTreeSpec(schema_version=1, include=("a.py", "b.py"))
    first = resolve_source_identity(first_root, source_tree_spec=spec)
    second = resolve_source_identity(second_root, source_tree_spec=spec)

    assert first.mode == second.mode == "source_tree"
    assert first.git_commit is second.git_commit is None
    assert first.working_tree_dirty is second.working_tree_dirty is False
    assert first.source_tree_hash == second.source_tree_hash
    assert first.source_hash == second.source_hash


def test_missing_git_executable_falls_back_to_explicit_source_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Git absence must not make source identity depend on process CWD or a placeholder."""
    root = tmp_path / "source"
    root.mkdir()
    (root / "strategy.py").write_bytes(b"SIGNAL = 1\n")

    def missing_git(*args: object, **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        del args, kwargs
        raise FileNotFoundError("git is unavailable")

    monkeypatch.setattr(fingerprint_module.subprocess, "run", missing_git)

    identity = resolve_source_identity(
        root,
        source_tree_spec=SourceTreeSpec(schema_version=1, include=("strategy.py",)),
    )

    assert identity.mode == "source_tree"
    assert identity.git_commit is None
    assert identity.source_tree_hash
    assert _SHA256.fullmatch(identity.source_hash)


def test_unborn_git_repository_falls_back_to_explicit_source_tree(
    tmp_path: Path,
) -> None:
    """A repository without HEAD still needs a deterministic non-placeholder identity."""
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init", "--quiet")
    (root / "strategy.py").write_bytes(b"SIGNAL = 1\n")

    identity = resolve_source_identity(
        root,
        source_tree_spec=SourceTreeSpec(schema_version=1, include=("strategy.py",)),
    )

    assert identity.mode == "source_tree"
    assert identity.git_commit is None
    assert identity.working_tree_dirty is False
    assert _SHA256.fullmatch(identity.source_tree_hash or "")


def test_lockfile_hash_uses_actual_bytes_and_missing_file_fails_closed(
    tmp_path: Path,
) -> None:
    """A constant placeholder would miss dependency changes or absent evidence."""
    lockfile = tmp_path / "uv.lock"
    lockfile.write_bytes(b"package-a==1\n")
    first = hash_lockfile(lockfile)
    lockfile.write_bytes(b"package-a==2\n")

    assert hash_lockfile(lockfile) != first
    with pytest.raises(ValueError, match="lockfile"):
        hash_lockfile(tmp_path / "missing.lock")


def test_environment_requires_lockfile_inside_explicit_source_root(
    tmp_path: Path,
) -> None:
    """Disclosure paths must be reproducible relative paths, never path escapes."""
    root = tmp_path / "source"
    root.mkdir()
    (root / "strategy.py").write_bytes(b"SIGNAL = 1\n")
    outside = tmp_path / "outside.lock"
    outside.write_bytes(b"outside")

    with pytest.raises(ValueError, match="source root"):
        capture_environment(root, outside)


def test_no_git_source_identity_requires_a_versioned_explicit_include_spec(
    tmp_path: Path,
) -> None:
    """No fallback may guess that runtime, artifact, or secret files are source."""
    root = tmp_path / "source"
    root.mkdir()
    (root / "strategy.py").write_bytes(b"SIGNAL = 1\n")

    with pytest.raises(ValueError, match="explicit.*source|source.*spec"):
        resolve_source_identity(root)

    with pytest.raises(ValueError, match="schema_version"):
        SourceTreeSpec(schema_version=2, include=("strategy.py",))


def test_explicit_source_spec_excludes_runtime_artifacts_and_binds_source_changes(
    tmp_path: Path,
) -> None:
    """Only declared source files contribute to no-Git identity."""
    root = tmp_path / "source"
    root.mkdir()
    source = root / "strategy.py"
    source.write_bytes(b"SIGNAL = 1\n")
    (root / "secret.env").write_bytes(b"TOKEN=first\n")
    (root / "runtime.log").write_bytes(b"first\n")
    artifacts = root / "artifacts"
    artifacts.mkdir()
    (artifacts / "result.bin").write_bytes(b"first")
    spec = SourceTreeSpec(schema_version=1, include=("strategy.py",))

    first = resolve_source_identity(root, source_tree_spec=spec)
    (root / "secret.env").write_bytes(b"TOKEN=second\n")
    (root / "runtime.log").write_bytes(b"second\n")
    (artifacts / "result.bin").write_bytes(b"second")
    second = resolve_source_identity(root, source_tree_spec=spec)
    source.write_bytes(b"SIGNAL = 2\n")
    third = resolve_source_identity(root, source_tree_spec=spec)

    assert first.source_hash == second.source_hash
    assert third.source_hash != first.source_hash


@pytest.mark.parametrize(
    ("failing_command", "stderr"),
    [
        ("rev-parse --show-toplevel", b"fatal: detected dubious ownership"),
        ("rev-parse --verify HEAD", b"fatal: corrupt loose object"),
        ("status --porcelain=v1 -z --untracked-files=no", b"permission denied"),
        ("ls-files -z --cached", b"fatal: index file smaller than expected"),
    ],
)
def test_git_operational_failures_never_fall_back_to_source_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failing_command: str,
    stderr: bytes,
) -> None:
    """Only proven non-repo/unborn states may use the explicit fallback."""
    root = tmp_path / "repo"
    _committed_repo(root)
    (root / "strategy.py").write_bytes(b"SIGNAL = 2\n")
    real_run = fingerprint_module.subprocess.run

    def fail_one(
        arguments: list[str], **kwargs: object
    ) -> subprocess.CompletedProcess[bytes]:
        command = " ".join(arguments[3:])
        if command == failing_command:
            return subprocess.CompletedProcess(arguments, 128, b"", stderr)
        return real_run(arguments, **kwargs)

    monkeypatch.setattr(fingerprint_module.subprocess, "run", fail_one)

    with pytest.raises(ValueError, match="Git"):
        resolve_source_identity(
            root,
            source_tree_spec=SourceTreeSpec(
                schema_version=1, include=("strategy.py",)
            ),
        )


def test_git_output_decode_failure_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Undecodable Git identity output is corruption, not a no-Git fallback."""
    root = tmp_path / "repo"
    _committed_repo(root)

    def undecodable(
        arguments: list[str], **kwargs: object
    ) -> subprocess.CompletedProcess[bytes]:
        del kwargs
        return subprocess.CompletedProcess(arguments, 0, b"\xff", b"")

    monkeypatch.setattr(fingerprint_module.subprocess, "run", undecodable)

    with pytest.raises(ValueError, match="UTF-8|decode"):
        resolve_source_identity(
            root,
            source_tree_spec=SourceTreeSpec(
                schema_version=1, include=("strategy.py",)
            ),
        )


@pytest.mark.skipif(os.name != "nt", reason="Windows junction capability test")
def test_explicit_source_spec_rejects_windows_junction_components(
    tmp_path: Path,
) -> None:
    """An included source path cannot traverse an NTFS reparse-point directory."""
    root = tmp_path / "source"
    root.mkdir()
    target = tmp_path / "junction-target"
    target.mkdir()
    (target / "strategy.py").write_bytes(b"SIGNAL = 1\n")
    junction = root / "linked"
    completed = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(junction), str(target)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode != 0:
        pytest.skip(
            "Windows junction creation is unavailable: "
            f"{completed.stderr.strip() or completed.stdout.strip()}"
        )

    with pytest.raises(ValueError, match="reparse"):
        resolve_source_identity(
            root,
            source_tree_spec=SourceTreeSpec(
                schema_version=1, include=("linked/strategy.py",)
            ),
        )
