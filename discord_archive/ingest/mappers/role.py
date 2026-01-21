"""Role API JSON to ORM mapper."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from discord_archive.db.models import Role


def map_role(data: dict[str, Any], guild_id: int) -> Role:
    """Convert Discord API role JSON to Role ORM instance.

    Args:
        data: Raw role object from Discord API
        guild_id: Parent guild ID

    Returns:
        Role ORM instance (not yet added to session)
    """
    # Parse permissions as Decimal for large bitfield support
    permissions = Decimal(data.get("permissions", "0"))

    return Role(
        role_id=int(data["id"]),
        guild_id=guild_id,
        name=data["name"],
        color=data.get("color", 0),
        colors=data.get("colors"),
        hoist=data.get("hoist", False),
        position=data.get("position", 0),
        mentionable=data.get("mentionable", False),
        icon=data.get("icon"),
        unicode_emoji=data.get("unicode_emoji"),
        permissions=permissions,
        managed=data.get("managed", False),
        tags=data.get("tags"),
        flags=data.get("flags", 0),
        raw=data,
    )
