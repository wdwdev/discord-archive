from datetime import datetime, timezone
from typing import Optional


def utcnow() -> datetime:
    """Return timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def parse_iso8601(value: str | None) -> Optional[datetime]:
    """
    Parse an ISO8601 timestamp from Discord into a timezone-aware UTC datetime.

    Discord timestamps are always UTC (Z or +00:00).
    """
    if not value:
        return None

    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))

    # Normalize to UTC timezone-aware
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return dt
