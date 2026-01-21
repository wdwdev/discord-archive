"""User API JSON to ORM mapper."""

from __future__ import annotations

from typing import Any

from discord_archive.db.models import User


def map_user(data: dict[str, Any]) -> User:
    """Convert Discord API user JSON to User ORM instance.

    Args:
        data: Raw user object from Discord API (may be partial)

    Returns:
        User ORM instance (not yet added to session)
    """
    return User(
        user_id=int(data["id"]),
        username=data.get("username"),
        discriminator=data.get("discriminator"),
        global_name=data.get("global_name"),
        avatar=data.get("avatar"),
        avatar_decoration_data=data.get("avatar_decoration_data"),
        banner=data.get("banner"),
        accent_color=data.get("accent_color"),
        bot=data.get("bot", False),
        system=data.get("system", False),
        public_flags=data.get("public_flags", 0),
        premium_type=data.get("premium_type"),
        raw=data,
    )


def extract_users_from_message(data: dict[str, Any]) -> list[User]:
    """Extract all user objects from a message API response.

    This extracts:
    - The message author
    - Users in mentions array

    Args:
        data: Raw message object from Discord API

    Returns:
        List of User ORM instances (may contain duplicates by user_id)
    """
    users: list[User] = []

    # Author
    if "author" in data:
        users.append(map_user(data["author"]))

    # Mentions
    for mention in data.get("mentions", []):
        users.append(map_user(mention))

    return users
