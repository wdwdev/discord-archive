"""Entity ingestion for guilds.

Handles ingestion of roles, emojis, stickers, and scheduled events.
Uses inline upsert logic to avoid dependency on entity_repository.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from discord_archive.db.models import Emoji, GuildScheduledEvent, Role, Sticker
from discord_archive.ingest.client import DiscordAPIError, DiscordClient
from discord_archive.ingest.logger import logger
from discord_archive.ingest.mappers import (
    map_emoji,
    map_role,
    map_scheduled_event,
    map_sticker,
)


async def _upsert_role(session: AsyncSession, role: Role) -> None:
    """Upsert a role record."""
    stmt = (
        pg_insert(Role)
        .values(
            role_id=role.role_id,
            guild_id=role.guild_id,
            name=role.name,
            color=role.color,
            colors=role.colors,
            hoist=role.hoist,
            position=role.position,
            mentionable=role.mentionable,
            icon=role.icon,
            unicode_emoji=role.unicode_emoji,
            permissions=role.permissions,
            managed=role.managed,
            tags=role.tags,
            flags=role.flags,
            raw=role.raw,
        )
        .on_conflict_do_update(
            index_elements=["role_id"],
            set_={"name": role.name, "color": role.color, "raw": role.raw},
        )
    )
    await session.execute(stmt)


async def _upsert_emoji(session: AsyncSession, emoji: Emoji) -> None:
    """Upsert an emoji record."""
    stmt = (
        pg_insert(Emoji)
        .values(
            emoji_id=emoji.emoji_id,
            guild_id=emoji.guild_id,
            name=emoji.name,
            animated=emoji.animated,
            available=emoji.available,
            managed=emoji.managed,
            require_colons=emoji.require_colons,
            roles=emoji.roles,
            user_id=emoji.user_id,
            raw=emoji.raw,
        )
        .on_conflict_do_update(
            index_elements=["emoji_id"],
            set_={"name": emoji.name, "available": emoji.available, "raw": emoji.raw},
        )
    )
    await session.execute(stmt)


async def _upsert_sticker(session: AsyncSession, sticker: Sticker) -> None:
    """Upsert a sticker record."""
    stmt = (
        pg_insert(Sticker)
        .values(
            sticker_id=sticker.sticker_id,
            guild_id=sticker.guild_id,
            pack_id=sticker.pack_id,
            name=sticker.name,
            description=sticker.description,
            tags=sticker.tags,
            type=sticker.type,
            format_type=sticker.format_type,
            available=sticker.available,
            user_id=sticker.user_id,
            sort_value=sticker.sort_value,
            raw=sticker.raw,
        )
        .on_conflict_do_update(
            index_elements=["sticker_id"],
            set_={
                "name": sticker.name,
                "available": sticker.available,
                "raw": sticker.raw,
            },
        )
    )
    await session.execute(stmt)


async def _upsert_scheduled_event(
    session: AsyncSession, event: GuildScheduledEvent
) -> None:
    """Upsert a scheduled event record."""
    stmt = (
        pg_insert(GuildScheduledEvent)
        .values(
            event_id=event.event_id,
            guild_id=event.guild_id,
            channel_id=event.channel_id,
            creator_id=event.creator_id,
            name=event.name,
            description=event.description,
            image=event.image,
            scheduled_start_time=event.scheduled_start_time,
            scheduled_end_time=event.scheduled_end_time,
            privacy_level=event.privacy_level,
            status=event.status,
            entity_type=event.entity_type,
            entity_id=event.entity_id,
            entity_metadata=event.entity_metadata,
            user_count=event.user_count,
            recurrence_rule=event.recurrence_rule,
            raw=event.raw,
        )
        .on_conflict_do_update(
            index_elements=["event_id"],
            set_={"name": event.name, "status": event.status, "raw": event.raw},
        )
    )
    await session.execute(stmt)


async def ingest_roles(
    session: AsyncSession,
    guild_data: dict[str, Any],
    guild_id: int,
) -> int:
    """Ingest roles from guild data.

    Returns:
        Number of roles ingested
    """
    roles_data = guild_data.get("roles", [])

    with logger.block("Roles") as block:
        if not roles_data:
            block.result("0 ingested")
            return 0

        count = 0
        for role_data in roles_data:
            role = map_role(role_data, guild_id)
            await _upsert_role(session, role)
            count += 1
        block.result(f"{count:,} ingested")
        return count


async def ingest_emojis(
    client: DiscordClient,
    session: AsyncSession,
    guild_id: int,
) -> int:
    """Ingest guild emojis.

    Returns:
        Number of emojis ingested
    """
    with logger.block("Emojis") as block:
        try:
            emojis_data = await client.get_guild_emojis(guild_id)
        except DiscordAPIError as e:
            if e.status_code == 403:
                block.skip("no access")
                return 0
            raise

        if not emojis_data:
            block.result("0 ingested")
            return 0

        count = 0
        for emoji_data in emojis_data:
            emoji = map_emoji(emoji_data, guild_id)
            await _upsert_emoji(session, emoji)
            count += 1
        block.result(f"{count:,} ingested")
        return count


async def ingest_stickers(
    client: DiscordClient,
    session: AsyncSession,
    guild_id: int,
) -> int:
    """Ingest guild stickers.

    Returns:
        Number of stickers ingested
    """
    with logger.block("Stickers") as block:
        try:
            stickers_data = await client.get_guild_stickers(guild_id)
        except DiscordAPIError as e:
            if e.status_code == 403:
                block.skip("no access")
                return 0
            raise

        if not stickers_data:
            block.result("0 ingested")
            return 0

        count = 0
        for sticker_data in stickers_data:
            sticker = map_sticker(sticker_data)
            await _upsert_sticker(session, sticker)
            count += 1
        block.result(f"{count:,} ingested")
        return count


async def ingest_scheduled_events(
    client: DiscordClient,
    session: AsyncSession,
    guild_id: int,
) -> int:
    """Ingest guild scheduled events.

    Returns:
        Number of events ingested
    """
    with logger.block("Scheduled Events") as block:
        try:
            events_data = await client.get_guild_scheduled_events(guild_id)
        except DiscordAPIError as e:
            if e.status_code == 403:
                block.skip("no access")
                return 0
            raise

        if not events_data:
            block.result("0 ingested")
            return 0

        count = 0
        for event_data in events_data:
            event = map_scheduled_event(event_data)
            await _upsert_scheduled_event(session, event)
            count += 1
        block.result(f"{count:,} ingested")
        return count
