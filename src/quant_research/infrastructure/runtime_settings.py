"""在数据可信根内原子维护 Dashboard 支持的 dotenv 设置。"""

from __future__ import annotations

import os
import stat
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from quant_research.application.settings import (
    DataSourceConcurrencySetting,
    DataSourceProxySetting,
    DataSourceRateLimitSetting,
    DataSourceTokenSetting,
)


class DataRootEnvSettingsStore:
    """维护 ``$QUANT_DATA_ROOT/.env`` 中受支持的类型化设置。

    入参：已验证位于源码树外的数据根，以及可注入的进程环境变量映射。
    返回值：构造不缓存 Token、每次读取都按最新文件解析的设置存储。
    异常：路径逃逸、符号链接、重复键、未知键、非法值或文件过大时失败关闭。
    """

    _TOKEN_KEY = "QUANT_TUSHARE_TOKEN"
    _RATE_LIMIT_KEY = "QUANT_TUSHARE_REQUESTS_PER_MINUTE"
    _PROXY_KEY = "QUANT_TUSHARE_PROXY_URL"
    _CONCURRENCY_KEY = "QUANT_TUSHARE_MAX_CONCURRENT_REQUESTS"
    _SUPPORTED_KEYS = frozenset(
        {_TOKEN_KEY, _RATE_LIMIT_KEY, _PROXY_KEY, _CONCURRENCY_KEY}
    )
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

    def read_data_source_rate_limit(self) -> DataSourceRateLimitSetting:
        """读取每分钟请求数，并依次回退到进程环境和内置默认值。

        入参：无。
        返回值：完成严格整数校验的限流设置及非敏感来源证据。
        异常：dotenv、环境变量或数值范围非法时失败关闭。
        """
        path = self._validated_path(must_exist=False)
        lines, indexes = self._read_lines(path)
        rate_index = indexes.get(self._RATE_LIMIT_KEY)
        if rate_index is not None:
            value = self._rate_limit_value(self._line_value(lines[rate_index]))
            updated_at = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
            return DataSourceRateLimitSetting(value, "DATA_ROOT_ENV", updated_at)
        environment_value = self._environment.get(self._RATE_LIMIT_KEY)
        if environment_value is not None:
            value = self._rate_limit_value(environment_value)
            return DataSourceRateLimitSetting(value, "PROCESS_ENVIRONMENT", None)
        return DataSourceRateLimitSetting(
            DataSourceRateLimitSetting.DEFAULT_REQUESTS_PER_MINUTE,
            "DEFAULT",
            None,
        )

    def read_data_source_proxy(self) -> DataSourceProxySetting:
        """读取数据根优先、进程环境回退的 Tushare 代理 URL。

        入参：无。
        返回值：规范代理 URL 与来源；未配置时值为空。
        异常：dotenv、环境变量或 URL 非法时失败关闭。
        """
        path = self._validated_path(must_exist=False)
        lines, indexes = self._read_lines(path)
        proxy_index = indexes.get(self._PROXY_KEY)
        if proxy_index is not None:
            value = DataSourceProxySetting.validated_value(
                self._line_value(lines[proxy_index])
            )
            updated_at = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
            return DataSourceProxySetting(value, "DATA_ROOT_ENV", updated_at)
        environment_value = self._environment.get(self._PROXY_KEY)
        if environment_value is not None:
            value = DataSourceProxySetting.validated_value(environment_value)
            return DataSourceProxySetting(value, "PROCESS_ENVIRONMENT", None)
        return DataSourceProxySetting(None, "NONE", None)

    def read_data_source_concurrency(self) -> DataSourceConcurrencySetting:
        """读取最大并发请求数，并回退到进程环境或内置默认值。

        入参：无。
        返回值：完成严格整数校验的并发设置及非敏感来源证据。
        异常：dotenv、环境变量或数值范围非法时失败关闭。
        """
        path = self._validated_path(must_exist=False)
        lines, indexes = self._read_lines(path)
        index = indexes.get(self._CONCURRENCY_KEY)
        if index is not None:
            value = self._concurrency_value(self._line_value(lines[index]))
            updated_at = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
            return DataSourceConcurrencySetting(value, "DATA_ROOT_ENV", updated_at)
        environment_value = self._environment.get(self._CONCURRENCY_KEY)
        if environment_value is not None:
            value = self._concurrency_value(environment_value)
            return DataSourceConcurrencySetting(value, "PROCESS_ENVIRONMENT", None)
        return DataSourceConcurrencySetting(
            DataSourceConcurrencySetting.DEFAULT_MAX_CONCURRENT_REQUESTS,
            "DEFAULT",
            None,
        )

    def write_data_source_token(self, value: str) -> DataSourceTokenSetting:
        """原子新增或替换数据根中的数据源 Token。

        入参：已经应用层校验的单行安全 Token。
        返回值：写入后重新解析的 Dashboard 优先状态。
        异常：路径、既有格式或文件系统操作失败时保持原语义。
        """
        token, _, _, _ = self.apply_changes(
            token_operation="SET",
            token_value=DataSourceTokenSetting.validated_value(value),
            rate_limit_operation=None,
            requests_per_minute=None,
            proxy_operation=None,
            proxy_url=None,
            concurrency_operation=None,
            max_concurrent_requests=None,
        )
        return token

    def clear_data_source_token(self) -> DataSourceTokenSetting:
        """清除 Dashboard 管理 Token，保留注释并回退进程环境变量。

        入参：无。
        返回值：删除后的最新解析状态。
        异常：路径、既有格式或文件系统删除失败时保持原语义。
        """
        token, _, _, _ = self.apply_changes(
            token_operation="CLEAR",
            token_value=None,
            rate_limit_operation=None,
            requests_per_minute=None,
            proxy_operation=None,
            proxy_url=None,
            concurrency_operation=None,
            max_concurrent_requests=None,
        )
        return token

    def apply_changes(
        self,
        *,
        token_operation: str | None,
        token_value: str | None,
        rate_limit_operation: str | None,
        requests_per_minute: int | None,
        proxy_operation: str | None = None,
        proxy_url: str | None = None,
        concurrency_operation: str | None = None,
        max_concurrent_requests: int | None = None,
    ) -> tuple[
        DataSourceTokenSetting,
        DataSourceRateLimitSetting,
        DataSourceProxySetting,
        DataSourceConcurrencySetting,
    ]:
        """通过一次原子替换应用 Token、限流和/或代理修改。

        入参：三个设置各自可选的 ``SET``/``CLEAR`` 操作及对应值。
        返回值：写入后按完整优先级重新解析的 Token、限流与代理设置。
        异常：组合非法、既有文件损坏或发布失败时保持原语义且不部分写入。
        """
        if (
            token_operation is None
            and rate_limit_operation is None
            and proxy_operation is None
            and concurrency_operation is None
        ):
            raise ValueError("runtime settings change must not be empty")
        if token_operation == "SET":
            token_value = DataSourceTokenSetting.validated_value(token_value)
        elif token_operation == "CLEAR":
            if token_value is not None:
                raise ValueError("CLEAR data source token operation must omit value")
        elif token_operation is not None:
            raise ValueError("unsupported data source token operation")
        if rate_limit_operation == "SET":
            requests_per_minute = DataSourceRateLimitSetting.validated_value(
                requests_per_minute
            )
        elif rate_limit_operation == "CLEAR":
            if requests_per_minute is not None:
                raise ValueError("CLEAR data source rate limit must omit a value")
        elif rate_limit_operation is not None:
            raise ValueError("unsupported data source rate limit operation")
        if proxy_operation == "SET":
            proxy_url = DataSourceProxySetting.validated_value(proxy_url)
        elif proxy_operation == "CLEAR":
            if proxy_url is not None:
                raise ValueError("CLEAR data source proxy must omit a URL")
        elif proxy_operation is not None:
            raise ValueError("unsupported data source proxy operation")
        if concurrency_operation == "SET":
            max_concurrent_requests = DataSourceConcurrencySetting.validated_value(
                max_concurrent_requests
            )
        elif concurrency_operation == "CLEAR":
            if max_concurrent_requests is not None:
                raise ValueError("CLEAR data source concurrency must omit a value")
        elif concurrency_operation is not None:
            raise ValueError("unsupported data source concurrency operation")

        self._prepare_root()
        path = self._validated_path(must_exist=False)
        lines, indexes = self._read_lines(path)
        replacements: dict[str, str | None] = {}
        if token_operation is not None:
            replacements[self._TOKEN_KEY] = (
                None if token_operation == "CLEAR" else token_value
            )
        if rate_limit_operation is not None:
            replacements[self._RATE_LIMIT_KEY] = (
                None
                if rate_limit_operation == "CLEAR"
                else str(requests_per_minute)
            )
        if proxy_operation is not None:
            replacements[self._PROXY_KEY] = (
                None if proxy_operation == "CLEAR" else proxy_url
            )
        if concurrency_operation is not None:
            replacements[self._CONCURRENCY_KEY] = (
                None
                if concurrency_operation == "CLEAR"
                else str(max_concurrent_requests)
            )
        for key in sorted(replacements):
            value = replacements[key]
            index = indexes.get(key)
            if value is None:
                if index is not None:
                    del lines[index]
                    lines, indexes = self._read_content_lines("".join(lines))
                continue
            replacement = f"{key}={value}\n"
            if index is None:
                if lines and not lines[-1].endswith(("\n", "\r")):
                    lines[-1] += "\n"
                lines.append(replacement)
                indexes[key] = len(lines) - 1
            else:
                ending = "\r\n" if lines[index].endswith("\r\n") else "\n"
                lines[index] = f"{key}={value}{ending}"
        self._validate_resolved_settings(lines, indexes)
        content = "".join(lines)
        if content.strip():
            self._atomic_write(path, content)
        elif path.exists():
            path.unlink()
        return (
            self.read_data_source_token(),
            self.read_data_source_rate_limit(),
            self.read_data_source_proxy(),
            self.read_data_source_concurrency(),
        )

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
        return self._read_content_lines(text)

    def _read_content_lines(self, text: str) -> tuple[list[str], dict[str, int]]:
        """解析内存中的 dotenv 内容并返回行及受支持键位置。"""
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
            value = content.split("=", 1)[1]
            if key == self._TOKEN_KEY:
                DataSourceTokenSetting.validated_value(value)
            elif key == self._RATE_LIMIT_KEY:
                self._rate_limit_value(value)
            elif key == self._PROXY_KEY:
                DataSourceProxySetting.validated_value(value)
            elif key == self._CONCURRENCY_KEY:
                self._concurrency_value(value)
            indexes[key] = index
        return lines, indexes

    @staticmethod
    def _line_value(line: str) -> str:
        """提取已完成结构校验的 dotenv 行值。"""
        return line.rstrip("\r\n").split("=", 1)[1]

    @staticmethod
    def _rate_limit_value(value: str) -> int:
        """把 dotenv 单行值解析为严格十进制限流整数。"""
        if not value or not value.isascii() or not value.isdecimal():
            raise ValueError("data source requests per minute must be a decimal integer")
        return DataSourceRateLimitSetting.validated_value(int(value))

    @staticmethod
    def _concurrency_value(value: str) -> int:
        """把 dotenv 单行值解析为严格十进制并发整数。"""
        if not value or not value.isascii() or not value.isdecimal():
            raise ValueError("data source concurrency must be a decimal integer")
        return DataSourceConcurrencySetting.validated_value(int(value))

    def _validate_resolved_settings(
        self,
        lines: list[str],
        indexes: Mapping[str, int],
    ) -> None:
        """在发布前校验修改后文件及其进程环境回退值。"""
        token_index = indexes.get(self._TOKEN_KEY)
        if token_index is not None:
            DataSourceTokenSetting.validated_value(
                self._line_value(lines[token_index])
            )
        elif (token := self._environment.get(self._TOKEN_KEY)) is not None:
            DataSourceTokenSetting.validated_value(token)
        rate_index = indexes.get(self._RATE_LIMIT_KEY)
        if rate_index is not None:
            self._rate_limit_value(self._line_value(lines[rate_index]))
        elif (rate := self._environment.get(self._RATE_LIMIT_KEY)) is not None:
            self._rate_limit_value(rate)
        proxy_index = indexes.get(self._PROXY_KEY)
        if proxy_index is not None:
            DataSourceProxySetting.validated_value(
                self._line_value(lines[proxy_index])
            )
        elif (proxy := self._environment.get(self._PROXY_KEY)) is not None:
            DataSourceProxySetting.validated_value(proxy)
        concurrency_index = indexes.get(self._CONCURRENCY_KEY)
        if concurrency_index is not None:
            self._concurrency_value(self._line_value(lines[concurrency_index]))
        elif (
            concurrency := self._environment.get(self._CONCURRENCY_KEY)
        ) is not None:
            self._concurrency_value(concurrency)

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
