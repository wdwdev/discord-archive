"""Message repository for bulk database operations.

Handles bulk upserts for messages and related entities:
- Users
- Messages
- Attachments
- Reactions
"""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from discord_archive.db.models import Attachment, Message, Reaction, User
from discord_archive.ingest.mappers import map_messages
from discord_archive.ingest.mappers.user import extract_users_from_message


async def get_channel_message_count(session: AsyncSession, channel_id: int) -> int:
    """Get the count of messages in a channel.

    Args:
        session: Database session
        channel_id: The channel ID to count messages for

    Returns:
        Number of messages in the channel
    """
    stmt = (
        select(func.count())
        .select_from(Message)
        .where(Message.channel_id == channel_id)
    )
    result = await session.execute(stmt)
    return result.scalar() or 0


async def bulk_upsert_users(session: AsyncSession, users: list[User]) -> None:
    """Bulk upsert users with deduplication.

    Args:
        session: Database session
        users: List of User ORM instances to upsert
    """
    if not users:
        return

    # Deduplicate by user_id
    seen_ids: set[int] = set()
    unique_users: list[User] = []
    for user in users:
        if user.user_id not in seen_ids:
            seen_ids.add(user.user_id)
            unique_users.append(user)

    if not unique_users:
        return

    values = [
        {
            "user_id": u.user_id,
            "username": u.username,
            "discriminator": u.discriminator,
            "global_name": u.global_name,
            "avatar": u.avatar,
            "avatar_decoration_data": u.avatar_decoration_data,
            "banner": u.banner,
            "accent_color": u.accent_color,
            "bot": u.bot,
            "system": u.system,
            "public_flags": u.public_flags,
            "premium_type": u.premium_type,
            "raw": u.raw,
        }
        for u in unique_users
    ]

    insert_stmt = pg_insert(User).values(values)
    stmt = insert_stmt.on_conflict_do_update(
        index_elements=["user_id"],
        set_={
            "username": insert_stmt.excluded.username,
            "discriminator": insert_stmt.excluded.discriminator,
            "global_name": insert_stmt.excluded.global_name,
            "avatar": insert_stmt.excluded.avatar,
            "avatar_decoration_data": insert_stmt.excluded.avatar_decoration_data,
            "banner": insert_stmt.excluded.banner,
            "accent_color": insert_stmt.excluded.accent_color,
            "bot": insert_stmt.excluded.bot,
            "system": insert_stmt.excluded.system,
            "public_flags": insert_stmt.excluded.public_flags,
            "premium_type": insert_stmt.excluded.premium_type,
            "raw": insert_stmt.excluded.raw,
        },
    )
    await session.execute(stmt)


async def bulk_insert_messages(session: AsyncSession, messages: list[Message]) -> None:
    """Bulk insert messages (on conflict do nothing).

    Args:
        session: Database session
        messages: List of Message ORM instances to insert
    """
    if not messages:
        return

    values = [
        {
            "message_id": m.message_id,
            "channel_id": m.channel_id,
            "author_id": m.author_id,
            "guild_id": m.guild_id,
            "content": m.content,
            "created_at": m.created_at,
            "edited_timestamp": m.edited_timestamp,
            "type": m.type,
            "tts": m.tts,
            "flags": m.flags,
            "pinned": m.pinned,
            "mention_everyone": m.mention_everyone,
            "mentions": m.mentions,
            "mention_roles": m.mention_roles,
            "mention_channels": m.mention_channels,
            "webhook_id": m.webhook_id,
            "application": m.application,
            "application_id": m.application_id,
            "message_reference": m.message_reference,
            "referenced_message_id": m.referenced_message_id,
            "message_snapshots": m.message_snapshots,
            "interaction_metadata": m.interaction_metadata,
            "thread": m.thread,
            "embeds": m.embeds,
            "components": m.components,
            "sticker_items": m.sticker_items,
            "poll": m.poll,
            "activity": m.activity,
            "call": m.call,
            "role_subscription_data": m.role_subscription_data,
            "raw": m.raw,
        }
        for m in messages
    ]

    stmt = (
        pg_insert(Message)
        .values(values)
        .on_conflict_do_nothing(index_elements=["message_id"])
    )
    await session.execute(stmt)


async def bulk_insert_attachments(
    session: AsyncSession, attachments: list[Attachment]
) -> None:
    """Bulk insert attachments (on conflict do nothing).

    Args:
        session: Database session
        attachments: List of Attachment ORM instances to insert
    """
    if not attachments:
        return

    values = [
        {
            "attachment_id": a.attachment_id,
            "message_id": a.message_id,
            "filename": a.filename,
            "description": a.description,
            "content_type": a.content_type,
            "size": a.size,
            "url": a.url,
            "proxy_url": a.proxy_url,
            "height": a.height,
            "width": a.width,
            "duration_secs": a.duration_secs,
            "waveform": a.waveform,
            "ephemeral": a.ephemeral,
            "flags": a.flags,
            "title": a.title,
            "raw": a.raw,
        }
        for a in attachments
    ]

    stmt = (
        pg_insert(Attachment)
        .values(values)
        .on_conflict_do_nothing(index_elements=["attachment_id"])
    )
    await session.execute(stmt)


async def bulk_upsert_reactions(
    session: AsyncSession, reactions: list[Reaction]
) -> None:
    """Bulk upsert reactions.

    Args:
        session: Database session
        reactions: List of Reaction ORM instances to upsert
    """
    if not reactions:
        return

    values = [
        {
            "message_id": r.message_id,
            "emoji_key": r.emoji_key,
            "emoji_id": r.emoji_id,
            "emoji_name": r.emoji_name,
            "emoji_animated": r.emoji_animated,
            "count": r.count,
            "count_details": r.count_details,
            "burst_colors": r.burst_colors,
            "raw": r.raw,
        }
        for r in reactions
    ]

    stmt = (
        pg_insert(Reaction)
        .values(values)
        .on_conflict_do_update(
            index_elements=["message_id", "emoji_key"],
            set_={
                "count": pg_insert(Reaction).excluded.count,
                "count_details": pg_insert(Reaction).excluded.count_details,
                "burst_colors": pg_insert(Reaction).excluded.burst_colors,
                "raw": pg_insert(Reaction).excluded.raw,
            },
        )
    )
    await session.execute(stmt)


async def persist_messages_batch(
    session: AsyncSession,
    messages_data: list[dict],
    guild_id: int,
) -> int:
    """Persist a batch of messages and related entities to database.

    Uses PostgreSQL bulk upserts for performance and idempotency.

    Args:
        session: Database session
        messages_data: Raw message dicts from Discord API
        guild_id: Guild ID for the messages

    Returns:
        Number of messages processed
    """
    if not messages_data:
        return 0

    # Map to ORM objects
    messages, attachments, reactions = map_messages(messages_data, guild_id)

    # Extract users from messages
    users: list[User] = []
    for msg_data in messages_data:
        users.extend(extract_users_from_message(msg_data))

    # Bulk upsert all entities
    await bulk_upsert_users(session, users)
    await bulk_insert_messages(session, messages)
    await bulk_insert_attachments(session, attachments)
    await bulk_upsert_reactions(session, reactions)

    return len(messages)
