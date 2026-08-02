"""Descriptor-bound reads beneath an explicitly trusted filesystem root."""

from __future__ import annotations

import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO


@dataclass(frozen=True, slots=True)
class VerifiedFile:
    """An open regular file whose path identity remains under verification."""

    file: BinaryIO
    size: int


@contextmanager
def open_verified_file(
    path: Path,
    *,
    trusted_root: Path,
    max_bytes: int,
) -> Iterator[VerifiedFile]:
    """Open one bounded regular file without following path indirection."""
    if type(max_bytes) is not int or max_bytes < 0:
        raise ValueError("max_bytes must be a nonnegative integer")
    absolute = path.absolute()
    root = trusted_root.absolute()
    try:
        relative = absolute.relative_to(root)
    except ValueError as error:
        raise ValueError("file path is outside the trusted root") from error
    if not relative.parts:
        raise ValueError("trusted file path must name a child of the trusted root")
    root_components = (*reversed(root.parents), root)
    target_components = tuple(
        root / Path(*relative.parts[:index])
        for index in range(1, len(relative.parts) + 1)
    )
    components = (
        *root_components,
        *(component for component in target_components if component != root),
    )
    identities = tuple(
        _component_identity(component, directory=index < len(components) - 1)
        for index, component in enumerate(components)
    )
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(absolute, flags)
    except OSError as error:
        raise ValueError("verified file is unavailable") from error
    file: BinaryIO | None = None
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise ValueError("verified file must be a regular non-link file")
        if _stat_identity(opened) != identities[-1]:
            raise ValueError("verified file descriptor identity changed while opening")
        if opened.st_size > max_bytes:
            raise ValueError("verified file exceeds the configured size limit")
        file = os.fdopen(descriptor, "rb", closefd=True)
        descriptor = -1
        yield VerifiedFile(file=file, size=opened.st_size)
        after_descriptor = os.fstat(file.fileno())
        if _stat_signature(after_descriptor) != _stat_signature(opened):
            raise ValueError("verified file descriptor identity changed while reading")
        after_identities = tuple(
            _component_identity(component, directory=index < len(components) - 1)
            for index, component in enumerate(components)
        )
        if after_identities != identities:
            raise ValueError("verified file path identity changed while reading")
    finally:
        if file is not None:
            file.close()
        elif descriptor >= 0:
            os.close(descriptor)


def _component_identity(path: Path, *, directory: bool) -> tuple[int, int, int]:
    try:
        observed = path.stat(follow_symlinks=False)
    except OSError as error:
        raise ValueError("verified file path is unavailable") from error
    reparse_point = getattr(observed, "st_file_attributes", 0) & 0x400
    if stat.S_ISLNK(observed.st_mode) or reparse_point:
        raise ValueError("verified file path contains a link or reparse point")
    if directory and not stat.S_ISDIR(observed.st_mode):
        raise ValueError("verified file root components must be directories")
    return _stat_identity(observed)


def _stat_identity(observed: os.stat_result) -> tuple[int, int, int]:
    return (observed.st_dev, observed.st_ino, observed.st_mode)


def _stat_signature(observed: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        *_stat_identity(observed),
        observed.st_size,
        observed.st_mtime_ns,
    )
