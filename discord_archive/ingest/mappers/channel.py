"""Channel API JSON to ORM mapper."""

from __future__ import annotations

from typing import Any

from discord_archive.db.models import Channel
from discord_archive.utils.time import parse_iso8601


def map_channel(data: dict[str, Any], valid_parent_ids: set[int] | None = None) -> Channel:
    """Convert Discord API channel JSON to Channel ORM instance.

    Args:
        data: Raw channel object from Discord API
        valid_parent_ids: If provided, only use parent_id if it's in this set

    Returns:
        Channel ORM instance (not yet added to session)
    """
    # Handle parent_id - null it if parent doesn't exist in our channel set
    parent_id = None
    if data.get("parent_id"):
        pid = int(data["parent_id"])
        if valid_parent_ids is None or pid in valid_parent_ids:
            parent_id = pid

    return Channel(
        channel_id=int(data["id"]),
        guild_id=int(data["guild_id"]) if data.get("guild_id") else None,
        type=data["type"],
        name=data.get("name"),
        topic=data.get("topic"),
        position=data.get("position"),
        permission_overwrites=data.get("permission_overwrites"),
        parent_id=parent_id,
        nsfw=data.get("nsfw"),
        last_message_id=int(data["last_message_id"]) if data.get("last_message_id") else None,
        # Voice settings
        bitrate=data.get("bitrate"),
        user_limit=data.get("user_limit"),
        rtc_region=data.get("rtc_region"),
        video_quality_mode=data.get("video_quality_mode"),
        # Slowmode
        rate_limit_per_user=data.get("rate_limit_per_user"),
        # Thread-specific
        owner_id=int(data["owner_id"]) if data.get("owner_id") else None,
        thread_metadata=data.get("thread_metadata"),
        message_count=data.get("message_count"),
        member_count=data.get("member_count"),
        total_message_sent=data.get("total_message_sent"),
        # Auto-archive
        default_auto_archive_duration=data.get("default_auto_archive_duration"),
        default_thread_rate_limit_per_user=data.get("default_thread_rate_limit_per_user"),
        # Forum/Media
        available_tags=data.get("available_tags"),
        applied_tags=[int(t) for t in data.get("applied_tags", [])] if data.get("applied_tags") else None,
        default_reaction_emoji=data.get("default_reaction_emoji"),
        default_sort_order=data.get("default_sort_order"),
        default_forum_layout=data.get("default_forum_layout"),
        # Flags
        flags=data.get("flags", 0),
        # DM/Group DM
        recipients=data.get("recipients"),
        icon=data.get("icon"),
        application_id=int(data["application_id"]) if data.get("application_id") else None,
        managed=data.get("managed"),
        # Last pin
        last_pin_timestamp=parse_iso8601(data["last_pin_timestamp"]) if data.get("last_pin_timestamp") else None,
        # Raw
        raw=data,
    )


# Channel type constants for reference
CHANNEL_TYPE_TEXT = 0
CHANNEL_TYPE_DM = 1
CHANNEL_TYPE_VOICE = 2
CHANNEL_TYPE_GROUP_DM = 3
CHANNEL_TYPE_CATEGORY = 4
CHANNEL_TYPE_ANNOUNCEMENT = 5
CHANNEL_TYPE_ANNOUNCEMENT_THREAD = 10
CHANNEL_TYPE_PUBLIC_THREAD = 11
CHANNEL_TYPE_PRIVATE_THREAD = 12
CHANNEL_TYPE_STAGE = 13
CHANNEL_TYPE_DIRECTORY = 14
CHANNEL_TYPE_FORUM = 15
CHANNEL_TYPE_MEDIA = 16


def is_text_based(channel_type: int) -> bool:
    """Check if channel type supports messages."""
    return channel_type in (
        CHANNEL_TYPE_TEXT,
        CHANNEL_TYPE_DM,
        CHANNEL_TYPE_GROUP_DM,
        CHANNEL_TYPE_ANNOUNCEMENT,
        CHANNEL_TYPE_ANNOUNCEMENT_THREAD,
        CHANNEL_TYPE_PUBLIC_THREAD,
        CHANNEL_TYPE_PRIVATE_THREAD,
        CHANNEL_TYPE_VOICE,  # Voice channels can have text too
        CHANNEL_TYPE_STAGE,  # Stage channels can have text too
    )


def is_thread(channel_type: int) -> bool:
    """Check if channel type is a thread."""
    return channel_type in (
        CHANNEL_TYPE_ANNOUNCEMENT_THREAD,
        CHANNEL_TYPE_PUBLIC_THREAD,
        CHANNEL_TYPE_PRIVATE_THREAD,
    )


def channel_type_name(channel_type: int) -> str:
    """Get human-readable channel type name."""
    names = {
        CHANNEL_TYPE_TEXT: "text",
        CHANNEL_TYPE_DM: "dm",
        CHANNEL_TYPE_VOICE: "voice",
        CHANNEL_TYPE_GROUP_DM: "group_dm",
        CHANNEL_TYPE_CATEGORY: "category",
        CHANNEL_TYPE_ANNOUNCEMENT: "announcement",
        CHANNEL_TYPE_ANNOUNCEMENT_THREAD: "announcement_thread",
        CHANNEL_TYPE_PUBLIC_THREAD: "public_thread",
        CHANNEL_TYPE_PRIVATE_THREAD: "private_thread",
        CHANNEL_TYPE_STAGE: "stage",
        CHANNEL_TYPE_DIRECTORY: "directory",
        CHANNEL_TYPE_FORUM: "forum",
        CHANNEL_TYPE_MEDIA: "media",
    }
    return names.get(channel_type, f"unknown({channel_type})")
