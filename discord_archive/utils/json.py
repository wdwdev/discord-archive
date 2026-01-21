# discord_archive/utils/json.py
from __future__ import annotations

from typing import Any


def compact_json(data: Any) -> Any:
    """
    Remove keys with None values (shallow).
    Useful before storing JSONB.
    """
    if not isinstance(data, dict):
        return data
    return {k: v for k, v in data.items() if v is not None}
