# discord_archive/utils/snowflake.py
from __future__ import annotations

from datetime import datetime, timezone

DISCORD_EPOCH = 1420070400000  # 2015-01-01 UTC (ms)


def snowflake_to_datetime(snowflake: int) -> datetime:
    ms = (snowflake >> 22) + DISCORD_EPOCH
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def datetime_to_snowflake(dt: datetime) -> int:
    if dt.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    ms = int(dt.timestamp() * 1000) - DISCORD_EPOCH
    return ms << 22
