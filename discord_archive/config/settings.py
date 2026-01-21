"""Configuration management using pydantic-settings.

Provides validated configuration with support for:
- JSON config file (config.json)
- Type coercion and validation
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AccountConfig(BaseModel):
    """Configuration for a single Discord account."""

    name: str
    token: str
    user_agent: str
    guilds: list[str]

    @field_validator("guilds", mode="before")
    @classmethod
    def ensure_string_list(cls, v: Any) -> list[str]:
        """Ensure guilds are strings (for snowflake IDs)."""
        if isinstance(v, list):
            return [str(g) for g in v]
        return v


class AppSettings(BaseSettings):
    """Application settings with validation.

    Settings are loaded from a JSON config file (config.json).
    """

    database_url: str = ""
    accounts: list[AccountConfig] = []

    model_config = SettingsConfigDict(
        extra="ignore",
    )

    @classmethod
    def from_json(cls, path: str | Path = "config.json") -> "AppSettings":
        """Load settings from a JSON config file.

        Args:
            path: Path to the JSON config file

        Returns:
            AppSettings instance with validated configuration
        """
        config_path = Path(path)
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return cls(**data)
        return cls()


@lru_cache
def get_settings(config_path: str = "config.json") -> AppSettings:
    """Get cached application settings.

    Args:
        config_path: Path to JSON config file (default: config.json)

    Returns:
        Cached AppSettings instance
    """
    return AppSettings.from_json(config_path)


# Backward compatibility aliases
AppConfig = AppSettings


def load_config(path: str | Path = "config.json") -> AppSettings:
    """Load configuration from file (backward compatible).

    Deprecated: Use get_settings() or AppSettings.from_json() instead.
    """
    # Clear cache when explicitly loading from a different path
    get_settings.cache_clear()
    return AppSettings.from_json(path)
