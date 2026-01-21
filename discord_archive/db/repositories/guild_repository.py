"""Guild repository for database operations."""

from __future__ import annotations

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from discord_archive.db.models import Guild


async def upsert_guild(session: AsyncSession, guild: Guild) -> None:
    """Upsert a guild record.

    Inserts a new guild or updates existing on conflict (guild_id).

    Args:
        session: Database session
        guild: Guild ORM model instance to upsert
    """
    stmt = (
        pg_insert(Guild)
        .values(
            guild_id=guild.guild_id,
            name=guild.name,
            icon=guild.icon,
            icon_hash=guild.icon_hash,
            splash=guild.splash,
            discovery_splash=guild.discovery_splash,
            banner=guild.banner,
            description=guild.description,
            owner_id=guild.owner_id,
            afk_channel_id=guild.afk_channel_id,
            afk_timeout=guild.afk_timeout,
            widget_enabled=guild.widget_enabled,
            widget_channel_id=guild.widget_channel_id,
            system_channel_id=guild.system_channel_id,
            rules_channel_id=guild.rules_channel_id,
            public_updates_channel_id=guild.public_updates_channel_id,
            safety_alerts_channel_id=guild.safety_alerts_channel_id,
            verification_level=guild.verification_level,
            default_message_notifications=guild.default_message_notifications,
            explicit_content_filter=guild.explicit_content_filter,
            mfa_level=guild.mfa_level,
            nsfw_level=guild.nsfw_level,
            system_channel_flags=guild.system_channel_flags,
            features=guild.features,
            premium_tier=guild.premium_tier,
            premium_subscription_count=guild.premium_subscription_count,
            premium_progress_bar_enabled=guild.premium_progress_bar_enabled,
            vanity_url_code=guild.vanity_url_code,
            preferred_locale=guild.preferred_locale,
            application_id=guild.application_id,
            max_presences=guild.max_presences,
            max_members=guild.max_members,
            max_video_channel_users=guild.max_video_channel_users,
            max_stage_video_channel_users=guild.max_stage_video_channel_users,
            approximate_member_count=guild.approximate_member_count,
            approximate_presence_count=guild.approximate_presence_count,
            welcome_screen=guild.welcome_screen,
            incidents_data=guild.incidents_data,
            raw=guild.raw,
        )
        .on_conflict_do_update(
            index_elements=["guild_id"],
            set_={
                "name": guild.name,
                "icon": guild.icon,
                "raw": guild.raw,
            },
        )
    )
    await session.execute(stmt)
