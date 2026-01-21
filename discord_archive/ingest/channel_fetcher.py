"""Channel and thread fetching utilities.

Handles fetching guild channels and archived threads with permission filtering.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from discord_archive.ingest.client import DiscordClient
from discord_archive.ingest.logger import logger
from discord_archive.ingest.mappers.channel import (
    CHANNEL_TYPE_FORUM,
    CHANNEL_TYPE_MEDIA,
)
from discord_archive.utils.permissions import (
    can_manage_threads,
    can_read_history,
    can_view_channel,
    compute_channel_permissions,
)


@dataclass
class PermissionContext:
    """Holds permission context for channel filtering."""

    user_id: int
    user_roles: list[int]
    base_permissions: int
    guild_id: int  # Used as @everyone role ID


async def fetch_all_channels(
    client: DiscordClient,
    permission_context: PermissionContext,
) -> list[dict[str, Any]]:
    """Fetch all channels including threads with permission filtering.

    Args:
        client: Discord client
        permission_context: Permission context for filtering

    Returns:
        List of channel data dictionaries
    """
    guild_id = permission_context.guild_id
    channels: list[dict[str, Any]] = []

    # Regular channels
    guild_channels = await client.get_guild_channels(guild_id)
    channels.extend(guild_channels)

    # Filter channels that need thread fetching AND we have permission for
    fetchable_channels = _filter_fetchable_channels(guild_channels, permission_context)

    if not fetchable_channels:
        return channels

    # Fetch archived threads
    await _fetch_archived_threads(client, fetchable_channels, channels)

    return channels


def _filter_fetchable_channels(
    guild_channels: list[dict[str, Any]],
    ctx: PermissionContext,
) -> list[tuple[dict[str, Any], int]]:
    """Filter channels that can have threads and we have permission to view.

    Returns:
        List of (channel_data, permissions) tuples
    """
    fetchable: list[tuple[dict[str, Any], int]] = []

    for c in guild_channels:
        # Only text, announcement, forum, and media channels can have threads
        if c["type"] not in (0, 5, CHANNEL_TYPE_FORUM, CHANNEL_TYPE_MEDIA):
            continue

        # Compute channel permissions
        channel_overwrites = c.get("permission_overwrites", [])
        channel_perms = compute_channel_permissions(
            user_id=ctx.user_id,
            base_permissions=ctx.base_permissions,
            channel_overwrites=channel_overwrites,
            user_roles=ctx.user_roles,
            everyone_role_id=ctx.guild_id,
        )

        if can_view_channel(channel_perms):
            fetchable.append((c, channel_perms))

    return fetchable


async def _fetch_archived_threads(
    client: DiscordClient,
    fetchable_channels: list[tuple[dict[str, Any], int]],
    channels: list[dict[str, Any]],
) -> None:
    """Fetch archived threads from all fetchable channels.

    Modifies channels list in place.
    """
    with logger.progress_context("Fetching threads") as progress:
        task = progress.add_task(
            "Fetching archived threads...",
            total=len(fetchable_channels),
        )

        for channel, channel_perms in fetchable_channels:
            channel_type = channel["type"]
            channel_id = int(channel["id"])

            # Text channels and announcement channels
            if channel_type in (0, 5):
                await _fetch_text_channel_threads(
                    client, channel_id, channel_perms, channels
                )

            # Forum and media channels
            if channel_type in (CHANNEL_TYPE_FORUM, CHANNEL_TYPE_MEDIA):
                await _fetch_forum_channel_threads(client, channel_id, channels)

            progress.advance(task)


async def _fetch_text_channel_threads(
    client: DiscordClient,
    channel_id: int,
    channel_perms: int,
    channels: list[dict[str, Any]],
) -> None:
    """Fetch archived threads from a text/announcement channel."""
    # Public archived threads
    try:
        before: str | None = None
        while True:
            result = await client.get_public_archived_threads(channel_id, before=before)
            threads = result.get("threads", [])
            channels.extend(threads)
            if not result.get("has_more", False) or not threads:
                break
            before = threads[-1]["thread_metadata"]["archive_timestamp"]
    except Exception:
        pass  # May not have permission

    # Private archived threads (requires MANAGE_THREADS + READ_MESSAGE_HISTORY)
    if can_manage_threads(channel_perms) and can_read_history(channel_perms):
        try:
            before = None
            while True:
                result = await client.get_private_archived_threads(
                    channel_id, before=before
                )
                threads = result.get("threads", [])
                channels.extend(threads)
                if not result.get("has_more", False) or not threads:
                    break
                before = threads[-1]["thread_metadata"]["archive_timestamp"]
        except Exception:
            pass  # May not have permission


async def _fetch_forum_channel_threads(
    client: DiscordClient,
    channel_id: int,
    channels: list[dict[str, Any]],
) -> None:
    """Fetch archived threads from a forum/media channel."""
    try:
        before: str | None = None
        while True:
            result = await client.get_public_archived_threads(channel_id, before=before)
            threads = result.get("threads", [])
            channels.extend(threads)
            if not result.get("has_more", False) or not threads:
                break
            before = threads[-1]["thread_metadata"]["archive_timestamp"]
    except Exception:
        pass
