"""Filesystem path validation for immutable data stores."""

from __future__ import annotations

import os
import stat
from pathlib import Path


def resolved_storage_root(path: Path) -> Path:
    """Return one absolute root after rejecting linked/reparse path components."""
    absolute = path.absolute()
    _reject_existing_reparse_components(absolute)
    return _normalize_windows_extended_path(absolute.resolve(strict=False))


def validate_storage_path(
    root: Path,
    path: Path,
    *,
    require_file: bool = False,
) -> Path:
    """Validate containment and reject every existing redirecting component."""
    absolute = path.absolute()
    try:
        absolute.relative_to(root)
    except ValueError as error:
        raise ValueError("storage path is outside its configured root") from error
    _reject_existing_reparse_components(root)
    _reject_existing_reparse_components(absolute, minimum=root)
    resolved = _normalize_windows_extended_path(absolute.resolve(strict=False))
    try:
        resolved.relative_to(root)
    except ValueError as error:
        raise ValueError("storage path resolves outside its configured root") from error
    if resolved != absolute:
        raise ValueError("storage path contains a link or reparse point")
    if require_file and not absolute.is_file():
        raise ValueError("storage path is not a regular file")
    return absolute


def _reject_existing_reparse_components(
    path: Path, *, minimum: Path | None = None
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
        if _is_reparse_point(component):
            raise ValueError("storage path contains a link or reparse point")


def _is_reparse_point(path: Path) -> bool:
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


def _normalize_windows_extended_path(path: Path) -> Path:
    if os.name != "nt":
        return path
    value = str(path)
    if value.startswith("\\\\?\\UNC\\"):
        value = "\\\\" + value[8:]
    elif value.startswith("\\\\?\\"):
        value = value[4:]
    return Path(value)
