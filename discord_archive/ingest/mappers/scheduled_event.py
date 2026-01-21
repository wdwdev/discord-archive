"""GuildScheduledEvent API JSON to ORM mapper."""

from __future__ import annotations

from typing import Any

from discord_archive.db.models import GuildScheduledEvent
from discord_archive.utils.time import parse_iso8601


def map_scheduled_event(data: dict[str, Any]) -> GuildScheduledEvent:
    """Convert Discord API scheduled event JSON to GuildScheduledEvent ORM instance.

    Args:
        data: Raw scheduled event object from Discord API

    Returns:
        GuildScheduledEvent ORM instance (not yet added to session)
    """
    return GuildScheduledEvent(
        event_id=int(data["id"]),
        guild_id=int(data["guild_id"]),
        channel_id=int(data["channel_id"]) if data.get("channel_id") else None,
        creator_id=int(data["creator_id"]) if data.get("creator_id") else None,
        name=data["name"],
        description=data.get("description"),
        image=data.get("image"),
        scheduled_start_time=parse_iso8601(data["scheduled_start_time"]),
        scheduled_end_time=parse_iso8601(data.get("scheduled_end_time")),
        privacy_level=data["privacy_level"],
        status=data["status"],
        entity_type=data["entity_type"],
        entity_id=int(data["entity_id"]) if data.get("entity_id") else None,
        entity_metadata=data.get("entity_metadata"),
        user_count=data.get("user_count"),
        recurrence_rule=data.get("recurrence_rule"),
        raw=data,
    )
