"""提供不可变数据存储使用的文件系统路径校验。"""

from __future__ import annotations

import os
import stat
from pathlib import Path


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
