"""Typed application settings loaded from YAML configuration files."""

from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime paths and validated application settings."""

    model_config = SettingsConfigDict(extra="forbid")

    timezone: ZoneInfo
    data_root: Path

    @field_validator("timezone", mode="before")
    @classmethod
    def validate_timezone(cls, value: str | ZoneInfo) -> ZoneInfo:
        if isinstance(value, ZoneInfo):
            return value
        try:
            return ZoneInfo(value)
        except ZoneInfoNotFoundError as error:
            raise ValueError(f"unknown timezone: {value}") from error

    @property
    def raw_root(self) -> Path:
        return self.data_root / "data" / "raw"

    @property
    def curated_root(self) -> Path:
        return self.data_root / "data" / "curated"

    @property
    def feature_root(self) -> Path:
        return self.data_root / "data" / "features"

    @property
    def artifact_root(self) -> Path:
        return self.data_root / "artifacts"

    @property
    def state_db(self) -> Path:
        return self.data_root / "state" / "quant.db"

    @classmethod
    def load(
        cls,
        config_path: Path,
        *,
        data_root: Path,
        source_root: Path | None = None,
    ) -> "Settings":
        """Load YAML settings after ensuring runtime data is outside source tree."""
        resolved_data_root = data_root.resolve()
        resolved_source_root = (source_root or Path.cwd()).resolve()
        if resolved_data_root.is_relative_to(resolved_source_root):
            raise ValueError("data_root must be outside source_root")

        loaded = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        if loaded is None:
            loaded = {}
        if not isinstance(loaded, dict):
            raise TypeError("configuration must be a YAML mapping")

        return cls(**loaded, data_root=resolved_data_root)
