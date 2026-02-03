# discord_archive/utils/ids.py
from __future__ import annotations

from typing import Any


def parse_snowflake(value: str | int | None) -> int | None:
    if value is None:
        return None
    return int(value)


def parse_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)
