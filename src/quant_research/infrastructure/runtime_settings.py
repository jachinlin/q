"""在数据可信根内原子维护 Dashboard 支持的 dotenv 设置。"""

from __future__ import annotations

import os
import stat
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from quant_research.application.settings import DataSourceTokenSetting


class DataRootEnvSettingsStore:
    """维护 ``$QUANT_DATA_ROOT/.env`` 中受支持的类型化设置。

    入参：已验证位于源码树外的数据根，以及可注入的进程环境变量映射。
    返回值：构造不缓存 Token、每次读取都按最新文件解析的设置存储。
    异常：路径逃逸、符号链接、重复键、未知键、非法值或文件过大时失败关闭。
    """

    _TOKEN_KEY = "QUANT_TUSHARE_TOKEN"
    _SUPPORTED_KEYS = frozenset({_TOKEN_KEY})
    _MAX_BYTES = 64 * 1024

    def __init__(
        self,
        data_root: Path,
        *,
        environment: Mapping[str, str] = os.environ,
    ) -> None:
        self._root = data_root.resolve()
        self._environment = environment

    @property
    def settings_path(self) -> Path:
        """返回数据根下唯一允许的 dotenv 文件路径。

        入参：无。
        返回值：规范化后的 ``$QUANT_DATA_ROOT/.env``。
        异常：数据根父目录已发生路径替换时抛出值错误。
        """
        path = self._root / ".env"
        if path.parent.resolve() != self._root:
            raise ValueError("runtime settings path escaped the trusted data root")
        return path

    def read_data_source_token(self) -> DataSourceTokenSetting:
        """读取数据根值，并在不存在时回退到进程环境变量。

        入参：无。
        返回值：包含内部明文、来源和更新时间的不可变设置。
        异常：dotenv 文件或任一候选 Token 非法时失败关闭。
        """
        path = self._validated_path(must_exist=False)
        lines, indexes = self._read_lines(path)
        token_index = indexes.get(self._TOKEN_KEY)
        if token_index is not None:
            token = self._line_value(lines[token_index])
            validated = DataSourceTokenSetting.validated_value(token)
            updated_at = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
            return DataSourceTokenSetting(validated, "DATA_ROOT_ENV", updated_at)
        environment_value = self._environment.get(self._TOKEN_KEY)
        if environment_value is not None:
            validated = DataSourceTokenSetting.validated_value(environment_value)
            return DataSourceTokenSetting(validated, "PROCESS_ENVIRONMENT", None)
        return DataSourceTokenSetting(None, "NONE", None)

    def write_data_source_token(self, value: str) -> DataSourceTokenSetting:
        """原子新增或替换数据根中的数据源 Token。

        入参：已经应用层校验的单行安全 Token。
        返回值：写入后重新解析的 Dashboard 优先状态。
        异常：路径、既有格式或文件系统操作失败时保持原语义。
        """
        validated = DataSourceTokenSetting.validated_value(value)
        self._prepare_root()
        path = self._validated_path(must_exist=False)
        lines, indexes = self._read_lines(path)
        replacement = f"{self._TOKEN_KEY}={validated}\n"
        index = indexes.get(self._TOKEN_KEY)
        if index is None:
            if lines and not lines[-1].endswith(("\n", "\r")):
                lines[-1] += "\n"
            lines.append(replacement)
        else:
            ending = "\r\n" if lines[index].endswith("\r\n") else "\n"
            lines[index] = f"{self._TOKEN_KEY}={validated}{ending}"
        self._atomic_write(path, "".join(lines))
        return self.read_data_source_token()

    def clear_data_source_token(self) -> DataSourceTokenSetting:
        """清除 Dashboard 管理 Token，保留注释并回退进程环境变量。

        入参：无。
        返回值：删除后的最新解析状态。
        异常：路径、既有格式或文件系统删除失败时保持原语义。
        """
        path = self._validated_path(must_exist=False)
        lines, indexes = self._read_lines(path)
        index = indexes.get(self._TOKEN_KEY)
        if index is None:
            return self.read_data_source_token()
        del lines[index]
        content = "".join(lines)
        if content.strip():
            self._atomic_write(path, content)
        else:
            path.unlink()
        return self.read_data_source_token()

    def _prepare_root(self) -> None:
        self._root.mkdir(parents=True, exist_ok=True)
        if self._root.resolve() != self._root:
            raise ValueError("runtime settings root changed after configuration")

    def _validated_path(self, *, must_exist: bool) -> Path:
        path = self.settings_path
        if path.is_symlink():
            raise ValueError("runtime settings file must not be a symbolic link")
        if path.exists():
            if not path.is_file():
                raise ValueError("runtime settings path must be a regular file")
            if path.resolve() != path:
                raise ValueError("runtime settings file escaped the trusted data root")
        elif must_exist:
            raise FileNotFoundError(path)
        return path

    def _read_lines(self, path: Path) -> tuple[list[str], dict[str, int]]:
        if not path.exists():
            return [], {}
        size = path.stat().st_size
        if size > self._MAX_BYTES:
            raise ValueError("runtime settings file exceeds the size limit")
        text = path.read_text(encoding="utf-8")
        lines = text.splitlines(keepends=True)
        indexes: dict[str, int] = {}
        for index, line in enumerate(lines):
            content = line.rstrip("\r\n")
            stripped = content.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if "=" not in content:
                raise ValueError("runtime settings line must use KEY=VALUE syntax")
            key, _ = content.split("=", 1)
            if key != key.strip() or not key.isidentifier() or key not in self._SUPPORTED_KEYS:
                raise ValueError("runtime settings file contains an unsupported key")
            if key in indexes:
                raise ValueError("runtime settings file contains a duplicate key")
            indexes[key] = index
        return lines, indexes

    @staticmethod
    def _line_value(line: str) -> str:
        """提取已完成结构校验的 dotenv 行值。"""
        return line.rstrip("\r\n").split("=", 1)[1]

    def _atomic_write(self, path: Path, content: str) -> None:
        temporary = self._root / f".env.tmp-{uuid4().hex}"
        if temporary.parent.resolve() != self._root:
            raise ValueError("runtime settings temporary path escaped the data root")
        try:
            with temporary.open("x", encoding="utf-8", newline="") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            os.chmod(temporary, stat.S_IRUSR | stat.S_IWUSR)
            os.replace(temporary, path)
        finally:
            if temporary.exists():
                temporary.unlink()


__all__ = ["DataRootEnvSettingsStore"]
