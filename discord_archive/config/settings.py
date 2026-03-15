"""Configuration management using pydantic-settings.

Provides validated configuration loaded from a TOML config file (config.toml).
"""

from __future__ import annotations

import tomllib
from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import BaseModel, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AccountConfig(BaseModel):
    """Configuration for a single Discord account."""

    name: str
    token: str
    user_agent: str = ""
    guilds: list[str]

    @field_validator("guilds", mode="before")
    @classmethod
    def ensure_string_list(cls, v: Any) -> list[str]:
        """Ensure guilds are strings (for snowflake IDs)."""
        if isinstance(v, list):
            return [str(g) for g in v]
        return v


class AppSettings(BaseSettings):
    """Application settings with validation."""

    database_url: str = ""
    readonly_database_url: str | None = None
    accounts: list[AccountConfig] = []

    model_config = SettingsConfigDict(
        extra="ignore",
    )

    @classmethod
    def from_file(cls, path: str | Path = "config.toml") -> "AppSettings":
        """Load settings from a TOML config file."""
        config_path = Path(path)
        if config_path.exists():
            with open(config_path, "rb") as f:
                data = tomllib.load(f)
            return cls(**data)
        return cls()


@lru_cache
def get_settings(config_path: str = "config.toml") -> AppSettings:
    """Get cached application settings."""
    return AppSettings.from_file(config_path)


# Backward compatibility
AppConfig = AppSettings
load_config = AppSettings.from_file
