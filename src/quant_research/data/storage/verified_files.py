"""在显式可信根目录下提供绑定文件描述符的安全读取。"""

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
    """封装路径身份持续受校验的已打开普通文件。

    入参：
        file：构造对象所需的同名字段，约束见类型标注。
        size：构造对象所需的同名字段，约束见类型标注。
    返回值：
        构造并返回 ``VerifiedFile`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    file: BinaryIO
    size: int


@contextmanager
def open_verified_file(
    path: Path,
    *,
    trusted_root: Path,
    max_bytes: int,
) -> Iterator[VerifiedFile]:
    """在可信根目录下打开大小受限的普通文件；该上下文管理器是安全读取的稳定框架入口，因此保留为模块级函数。

    入参：
        path：待校验或读取的文件系统路径。
        trusted_root：显式信任的文件系统根目录。
        max_bytes：允许读取的最大字节数。
    返回值：
        返回已验证文件（``Iterator[VerifiedFile]``）。
    异常：
        ValueError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
    """
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
        _VerifiedFileSupport.component_identity(
            component, directory=index < len(components) - 1
        )
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
        if _VerifiedFileSupport.stat_identity(opened) != identities[-1]:
            raise ValueError("verified file descriptor identity changed while opening")
        if opened.st_size > max_bytes:
            raise ValueError("verified file exceeds the configured size limit")
        file = os.fdopen(descriptor, "rb", closefd=True)
        descriptor = -1
        yield VerifiedFile(file=file, size=opened.st_size)
        after_descriptor = os.fstat(file.fileno())
        if _VerifiedFileSupport.stat_signature(
            after_descriptor
        ) != _VerifiedFileSupport.stat_signature(opened):
            raise ValueError("verified file descriptor identity changed while reading")
        after_identities = tuple(
            _VerifiedFileSupport.component_identity(
                component, directory=index < len(components) - 1
            )
            for index, component in enumerate(components)
        )
        if after_identities != identities:
            raise ValueError("verified file path identity changed while reading")
    finally:
        if file is not None:
            file.close()
        elif descriptor >= 0:
            os.close(descriptor)


class _VerifiedFileSupport:
    """集中执行已验证文件的路径与描述符身份比较。"""

    @classmethod
    def component_identity(cls, path: Path, *, directory: bool) -> tuple[int, int, int]:
        try:
            observed = path.stat(follow_symlinks=False)
        except OSError as error:
            raise ValueError("verified file path is unavailable") from error
        reparse_point = getattr(observed, "st_file_attributes", 0) & 0x400
        if stat.S_ISLNK(observed.st_mode) or reparse_point:
            raise ValueError("verified file path contains a link or reparse point")
        if directory and not stat.S_ISDIR(observed.st_mode):
            raise ValueError("verified file root components must be directories")
        return cls.stat_identity(observed)

    @staticmethod
    def stat_identity(observed: os.stat_result) -> tuple[int, int, int]:
        return (observed.st_dev, observed.st_ino, observed.st_mode)

    @classmethod
    def stat_signature(cls, observed: os.stat_result) -> tuple[int, int, int, int, int]:
        return (
            *cls.stat_identity(observed),
            observed.st_size,
            observed.st_mtime_ns,
        )
