"""Message API JSON to ORM mapper."""

from __future__ import annotations

from typing import Any

from discord_archive.db.models import Attachment, Message, Reaction
from discord_archive.utils.time import parse_iso8601


def _sanitize_null_bytes(value: Any) -> Any:
    """Remove NULL bytes (0x00) from strings, which PostgreSQL doesn't accept.

    For dict/list types, recursively sanitize all string values.
    """
    if isinstance(value, str):
        return value.replace("\x00", "")
    elif isinstance(value, dict):
        return {k: _sanitize_null_bytes(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_sanitize_null_bytes(item) for item in value]
    return value


def map_message(data: dict[str, Any], guild_id: int | None = None) -> Message:
    """Convert Discord API message JSON to Message ORM instance.

    Args:
        data: Raw message object from Discord API
        guild_id: Guild ID (may not be in message payload for some endpoints)

    Returns:
        Message ORM instance (not yet added to session)
    """
    # Sanitize the entire data dict to remove NULL bytes
    data = _sanitize_null_bytes(data)

    # Extract guild_id from data or use provided value
    msg_guild_id = guild_id
    if data.get("guild_id"):
        msg_guild_id = int(data["guild_id"])

    # Extract mention user IDs
    mention_ids = [int(u["id"]) for u in data.get("mentions", [])]

    # Extract mention role IDs (already just IDs in the API)
    mention_role_ids = [int(r) for r in data.get("mention_roles", [])]

    return Message(
        message_id=int(data["id"]),
        channel_id=int(data["channel_id"]),
        author_id=int(data["author"]["id"]),
        guild_id=msg_guild_id,
        content=data.get("content", ""),
        # Timestamps
        created_at=parse_iso8601(data["timestamp"]),
        edited_timestamp=parse_iso8601(data.get("edited_timestamp")),
        # Metadata
        type=data.get("type", 0),
        tts=data.get("tts", False),
        flags=data.get("flags", 0),
        pinned=data.get("pinned", False),
        # Mentions
        mention_everyone=data.get("mention_everyone", False),
        mentions=mention_ids,
        mention_roles=mention_role_ids,
        mention_channels=data.get("mention_channels"),
        # Webhook & Application
        webhook_id=int(data["webhook_id"]) if data.get("webhook_id") else None,
        application=data.get("application"),
        application_id=(
            int(data["application_id"]) if data.get("application_id") else None
        ),
        # References
        message_reference=data.get("message_reference"),
        referenced_message_id=(
            int(data["message_reference"]["message_id"])
            if data.get("message_reference")
            and data["message_reference"].get("message_id")
            else None
        ),
        message_snapshots=data.get("message_snapshots"),
        # Interactions
        interaction_metadata=data.get("interaction_metadata")
        or data.get("interaction"),
        # Thread
        thread=data.get("thread"),
        # Rich content
        embeds=data.get("embeds", []),
        components=data.get("components"),
        sticker_items=data.get("sticker_items"),
        poll=data.get("poll"),
        # Specialized
        activity=data.get("activity"),
        call=data.get("call"),
        role_subscription_data=data.get("role_subscription_data"),
        # Raw
        raw=data,
    )


def map_attachment(data: dict[str, Any], message_id: int) -> Attachment:
    """Convert Discord API attachment JSON to Attachment ORM instance.

    Args:
        data: Raw attachment object from Discord API
        message_id: Parent message ID

    Returns:
        Attachment ORM instance (not yet added to session)
    """
    return Attachment(
        attachment_id=int(data["id"]),
        message_id=message_id,
        filename=data["filename"],
        description=data.get("description"),
        content_type=data.get("content_type"),
        size=data["size"],
        url=data["url"],
        proxy_url=data.get("proxy_url"),
        height=data.get("height"),
        width=data.get("width"),
        duration_secs=data.get("duration_secs"),
        waveform=data.get("waveform"),
        ephemeral=data.get("ephemeral"),
        flags=data.get("flags"),
        title=data.get("title"),
        raw=data,
    )


def map_reaction(data: dict[str, Any], message_id: int) -> Reaction:
    """Convert Discord API reaction JSON to Reaction ORM instance.

    Args:
        data: Raw reaction object from Discord API
        message_id: Parent message ID

    Returns:
        Reaction ORM instance (not yet added to session)
    """
    emoji = data["emoji"]
    emoji_id = int(emoji["id"]) if emoji.get("id") else None
    emoji_name = emoji.get("name")
    emoji_animated = emoji.get("animated")

    # Build canonical emoji_key for composite primary key
    # Convention: "custom:<id>" for custom emoji, "unicode:<name>" for unicode
    if emoji_id:
        emoji_key = f"custom:{emoji_id}"
    else:
        emoji_key = f"unicode:{emoji_name}"

    return Reaction(
        message_id=message_id,
        emoji_key=emoji_key,
        emoji_id=emoji_id,
        emoji_name=emoji_name,
        emoji_animated=emoji_animated,
        count=data.get("count", 1),
        count_details=data.get("count_details"),
        burst_colors=data.get("burst_colors"),
        raw=data,
    )


def map_messages(
    data_list: list[dict[str, Any]], guild_id: int | None = None
) -> tuple[list[Message], list[Attachment], list[Reaction]]:
    """Convert a list of message API responses to ORM instances.

    Args:
        data_list: List of raw message objects from Discord API
        guild_id: Guild ID to use if not in message payload

    Returns:
        Tuple of (messages, attachments, reactions) lists
    """
    messages: list[Message] = []
    attachments: list[Attachment] = []
    reactions: list[Reaction] = []

    for data in data_list:
        message = map_message(data, guild_id)
        messages.append(message)

        message_id = int(data["id"])

        # Map attachments
        for att_data in data.get("attachments", []):
            attachments.append(map_attachment(att_data, message_id))

        # Map reactions
        for react_data in data.get("reactions", []):
            reactions.append(map_reaction(react_data, message_id))

    return messages, attachments, reactions
