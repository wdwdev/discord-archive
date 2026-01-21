"""Channel repository for database operations."""

from __future__ import annotations

from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from discord_archive.db.models import Channel


async def upsert_channel(session: AsyncSession, channel: Channel) -> None:
    """Upsert a channel record.

    Inserts a new channel or updates existing on conflict (channel_id).

    Args:
        session: Database session
        channel: Channel ORM model instance to upsert
    """
    stmt = (
        pg_insert(Channel)
        .values(
            channel_id=channel.channel_id,
            guild_id=channel.guild_id,
            type=channel.type,
            name=channel.name,
            topic=channel.topic,
            position=channel.position,
            permission_overwrites=channel.permission_overwrites,
            parent_id=channel.parent_id,
            nsfw=channel.nsfw,
            last_message_id=channel.last_message_id,
            bitrate=channel.bitrate,
            user_limit=channel.user_limit,
            rtc_region=channel.rtc_region,
            video_quality_mode=channel.video_quality_mode,
            rate_limit_per_user=channel.rate_limit_per_user,
            owner_id=channel.owner_id,
            thread_metadata=channel.thread_metadata,
            message_count=channel.message_count,
            member_count=channel.member_count,
            total_message_sent=channel.total_message_sent,
            default_auto_archive_duration=channel.default_auto_archive_duration,
            default_thread_rate_limit_per_user=channel.default_thread_rate_limit_per_user,
            available_tags=channel.available_tags,
            applied_tags=channel.applied_tags,
            default_reaction_emoji=channel.default_reaction_emoji,
            default_sort_order=channel.default_sort_order,
            default_forum_layout=channel.default_forum_layout,
            flags=channel.flags,
            recipients=channel.recipients,
            icon=channel.icon,
            application_id=channel.application_id,
            managed=channel.managed,
            last_pin_timestamp=channel.last_pin_timestamp,
            raw=channel.raw,
        )
        .on_conflict_do_update(
            index_elements=["channel_id"],
            set_={
                "name": channel.name,
                "topic": channel.topic,
                "position": channel.position,
                "last_message_id": channel.last_message_id,
                "thread_metadata": channel.thread_metadata,
                "message_count": channel.message_count,
                "raw": channel.raw,
            },
        )
    )
    await session.execute(stmt)


async def update_channel_parent(
    session: AsyncSession, channel_id: int, parent_id: int
) -> None:
    """Update the parent_id of a channel.

    Used in two-pass insertion to avoid FK constraint violations.

    Args:
        session: Database session
        channel_id: Channel to update
        parent_id: New parent channel ID
    """
    await session.execute(
        update(Channel)
        .where(Channel.channel_id == channel_id)
        .values(parent_id=parent_id)
    )


async def bulk_upsert_channels(
    session: AsyncSession,
    channels: list[Channel],
    known_parent_ids: set[int],
) -> None:
    """Bulk upsert channels with two-pass insertion for FK constraints.

    Pass 1: Insert all channels with parent_id = NULL
    Pass 2: Update parent_id for channels that have valid parents

    Args:
        session: Database session
        channels: List of Channel ORM model instances
        known_parent_ids: Set of valid parent channel IDs
    """
    # Pass 1: Insert all channels with parent_id = NULL
    for channel in channels:
        # Temporarily clear parent_id to avoid FK constraint
        original_parent_id = channel.parent_id
        channel.parent_id = None
        await upsert_channel(session, channel)
        channel.parent_id = original_parent_id  # Restore for pass 2
    await session.commit()

    # Pass 2: Update parent_id for channels that have valid parents
    for channel in channels:
        if channel.parent_id and channel.parent_id in known_parent_ids:
            await update_channel_parent(session, channel.channel_id, channel.parent_id)
    await session.commit()
