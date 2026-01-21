"""Guild processing logic.

Handles per-guild processing including entity ingestion and channel processing.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from discord_archive.db.repositories import (
    bulk_upsert_channels,
    get_channel_message_count,
    upsert_guild,
)
from discord_archive.ingest.backfill import backfill_channel
from discord_archive.ingest.channel_fetcher import PermissionContext, fetch_all_channels
from discord_archive.ingest.client import DiscordAPIError, DiscordClient
from discord_archive.ingest.entity_ingestor import (
    ingest_emojis,
    ingest_roles,
    ingest_scheduled_events,
    ingest_stickers,
)
from discord_archive.ingest.incremental import incremental_channel
from discord_archive.ingest.logger import logger
from discord_archive.ingest.mappers import map_channel, map_guild
from discord_archive.ingest.mappers.channel import (
    CHANNEL_TYPE_CATEGORY,
    channel_type_name,
    is_text_based,
)
from discord_archive.ingest.state import IngestStateManager
from discord_archive.utils.permissions import (
    build_role_permissions_map,
    can_access_channel,
    compute_base_permissions,
    compute_channel_permissions,
)


@dataclass
class GuildProcessResult:
    """Result of processing a guild."""

    channels_processed: int
    messages_ingested: int


async def build_permission_context(
    client: DiscordClient,
    guild_id: int,
    guild_data: dict[str, Any],
) -> PermissionContext:
    """Build permission context for a guild.

    Args:
        client: Discord client
        guild_id: Guild ID
        guild_data: Guild data with roles

    Returns:
        PermissionContext for channel filtering
    """
    user_data = await client.get_current_user()
    user_id = int(user_data["id"])

    try:
        member_data = await client.get_current_user_guild_member(guild_id)
        user_roles = [int(r) for r in member_data.get("roles", [])]
    except DiscordAPIError:
        # If we can't get member info, we can't filter by permissions
        user_roles = []

    roles_data = guild_data.get("roles", [])
    role_permissions = build_role_permissions_map(roles_data)
    base_permissions = compute_base_permissions(user_roles, role_permissions, guild_id)

    return PermissionContext(
        user_id=user_id,
        user_roles=user_roles,
        base_permissions=base_permissions,
        guild_id=guild_id,
    )


async def process_guild(
    client: DiscordClient,
    session: AsyncSession,
    guild_id: int,
) -> GuildProcessResult:
    """Process a single guild: fetch metadata, entities, channels, and messages.

    Args:
        client: Discord client
        session: Database session
        guild_id: Guild ID to process

    Returns:
        Processing result with stats
    """
    result = GuildProcessResult(channels_processed=0, messages_ingested=0)

    # Fetch and upsert guild
    guild_data = await client.get_guild(guild_id)
    guild = map_guild(guild_data)
    await upsert_guild(session, guild)
    await session.commit()

    logger.guild_start(guild_id, guild.name)

    # Build permission context
    perm_ctx = await build_permission_context(client, guild_id, guild_data)

    # Ingest entities
    await ingest_roles(session, guild_data, guild_id)
    await session.commit()

    await ingest_emojis(client, session, guild_id)
    await session.commit()

    await ingest_stickers(client, session, guild_id)
    await session.commit()

    await ingest_scheduled_events(client, session, guild_id)
    await session.commit()

    # Fetch all channels (including archived threads)
    logger.entity_start("Channels")
    channels_data = await fetch_all_channels(client, perm_ctx)

    # Map and upsert channels (two-pass for FK constraints)
    channels = [map_channel(c, valid_parent_ids=set()) for c in channels_data]
    known_channel_ids = {int(c["id"]) for c in channels_data}
    await bulk_upsert_channels(session, channels, known_channel_ids)
    logger.entity_complete(len(channels_data))

    # Filter and process text channels
    text_channels = _filter_viewable_text_channels(channels_data, perm_ctx)

    for channel_data in text_channels:
        channel_result = await process_channel(
            client=client,
            session=session,
            channel_data=channel_data,
            guild_id=guild_id,
        )
        result.channels_processed += 1
        result.messages_ingested += channel_result.messages_ingested

    return result


def _filter_viewable_text_channels(
    channels: list[dict[str, Any]],
    ctx: PermissionContext,
) -> list[dict[str, Any]]:
    """Filter channels to only viewable text-based channels."""
    text_channels = []
    skipped_no_permission = 0

    for c in channels:
        if not is_text_based(c["type"]) or c["type"] == CHANNEL_TYPE_CATEGORY:
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

        if can_access_channel(channel_perms, c["type"]):
            text_channels.append(c)
        else:
            skipped_no_permission += 1

    if skipped_no_permission > 0:
        logger.info(f"Skipped {skipped_no_permission} channels (no permission)")

    return text_channels


@dataclass
class ChannelProcessResult:
    """Result of processing a channel."""

    messages_ingested: int


async def process_channel(
    client: DiscordClient,
    session: AsyncSession,
    channel_data: dict[str, Any],
    guild_id: int,
) -> ChannelProcessResult:
    """Process a single channel: backfill then incremental.

    Args:
        client: Discord client
        session: Database session
        channel_data: Channel data dictionary
        guild_id: Guild ID

    Returns:
        Processing result with message count
    """
    result = ChannelProcessResult(messages_ingested=0)

    channel_id = int(channel_data["id"])
    channel_name = channel_data.get("name") or f"Channel {channel_id}"
    channel_type = channel_data["type"]
    type_name = channel_type_name(channel_type)

    state = IngestStateManager(session)
    checkpoint = await state.get_checkpoint(channel_id)

    # Determine mode
    needs_backfill = not checkpoint or not checkpoint.backfill_complete

    if needs_backfill:
        with logger.block(channel_name) as block:
            block.field("channel ID", channel_id)
            block.field("channel type", type_name)
            block.field("mode", "backfill", color="magenta")

            try:
                backfill_result = await backfill_channel(
                    client=client,
                    session=session,
                    channel_id=channel_id,
                    guild_id=guild_id,
                )
            except DiscordAPIError as e:
                if e.status_code == 403:
                    block.skip("no access")
                    return result
                raise

            result.messages_ingested += backfill_result.messages_count

            if backfill_result.messages_count == 0 and backfill_result.is_complete:
                block.empty()
                return result  # Skip incremental for empty channels
            else:
                block.result(f"ingested {backfill_result.messages_count:,} messages")

    # Incremental sync after backfill
    with logger.block(channel_name) as block:
        block.field("channel ID", channel_id)
        block.field("channel type", type_name)

        # Show current archived message count
        archived_count = await get_channel_message_count(session, channel_id)
        block.field("archived messages", f"{archived_count:,}")

        block.field("mode", "incremental", color="green")

        try:
            incr_result = await incremental_channel(
                client=client,
                session=session,
                channel_id=channel_id,
                guild_id=guild_id,
            )
        except DiscordAPIError as e:
            if e.status_code == 403:
                block.skip("no access")
                return result
            raise

        result.messages_ingested += incr_result.messages_count

        if incr_result.messages_count == 0:
            block.skip("already up to date")
        else:
            block.result(f"ingested {incr_result.messages_count:,} messages")

    return result
