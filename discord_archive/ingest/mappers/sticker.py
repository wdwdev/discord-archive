"""Sticker API JSON to ORM mapper."""

from __future__ import annotations

from typing import Any

from discord_archive.db.models import Sticker


def map_sticker(data: dict[str, Any]) -> Sticker:
    """Convert Discord API sticker JSON to Sticker ORM instance.

    Args:
        data: Raw sticker object from Discord API

    Returns:
        Sticker ORM instance (not yet added to session)
    """
    # Extract creator user ID if present
    user_id = None
    if data.get("user"):
        user_id = int(data["user"]["id"])

    return Sticker(
        sticker_id=int(data["id"]),
        guild_id=int(data["guild_id"]) if data.get("guild_id") else None,
        pack_id=int(data["pack_id"]) if data.get("pack_id") else None,
        name=data["name"],
        description=data.get("description"),
        tags=data.get("tags"),
        type=data["type"],
        format_type=data["format_type"],
        available=data.get("available"),
        user_id=user_id,
        sort_value=data.get("sort_value"),
        raw=data,
    )
