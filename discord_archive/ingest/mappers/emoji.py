"""Emoji API JSON to ORM mapper."""

from __future__ import annotations

from typing import Any

from discord_archive.db.models import Emoji


def map_emoji(data: dict[str, Any], guild_id: int) -> Emoji:
    """Convert Discord API emoji JSON to Emoji ORM instance.

    Args:
        data: Raw emoji object from Discord API
        guild_id: Parent guild ID

    Returns:
        Emoji ORM instance (not yet added to session)
    """
    # Extract role IDs if present
    roles = None
    if data.get("roles"):
        roles = [int(r) for r in data["roles"]]

    # Extract creator user ID if present
    user_id = None
    if data.get("user"):
        user_id = int(data["user"]["id"])

    return Emoji(
        emoji_id=int(data["id"]),
        guild_id=guild_id,
        name=data.get("name"),
        animated=data.get("animated", False),
        available=data.get("available", True),
        managed=data.get("managed", False),
        require_colons=data.get("require_colons", True),
        roles=roles,
        user_id=user_id,
        raw=data,
    )
