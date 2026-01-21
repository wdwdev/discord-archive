"""Guild API JSON to ORM mapper."""

from __future__ import annotations

from typing import Any

from discord_archive.db.models import Guild


def map_guild(data: dict[str, Any]) -> Guild:
    """Convert Discord API guild JSON to Guild ORM instance.

    Args:
        data: Raw guild object from Discord API

    Returns:
        Guild ORM instance (not yet added to session)
    """
    return Guild(
        guild_id=int(data["id"]),
        name=data["name"],
        # Visual assets
        icon=data.get("icon"),
        icon_hash=data.get("icon_hash"),
        splash=data.get("splash"),
        discovery_splash=data.get("discovery_splash"),
        banner=data.get("banner"),
        description=data.get("description"),
        # Owner
        owner_id=int(data["owner_id"]),
        # Channel references (soft)
        afk_channel_id=int(data["afk_channel_id"]) if data.get("afk_channel_id") else None,
        afk_timeout=data.get("afk_timeout", 300),
        widget_enabled=data.get("widget_enabled"),
        widget_channel_id=int(data["widget_channel_id"]) if data.get("widget_channel_id") else None,
        system_channel_id=int(data["system_channel_id"]) if data.get("system_channel_id") else None,
        rules_channel_id=int(data["rules_channel_id"]) if data.get("rules_channel_id") else None,
        public_updates_channel_id=int(data["public_updates_channel_id"]) if data.get("public_updates_channel_id") else None,
        safety_alerts_channel_id=int(data["safety_alerts_channel_id"]) if data.get("safety_alerts_channel_id") else None,
        # Moderation levels
        verification_level=data.get("verification_level", 0),
        default_message_notifications=data.get("default_message_notifications", 0),
        explicit_content_filter=data.get("explicit_content_filter", 0),
        mfa_level=data.get("mfa_level", 0),
        nsfw_level=data.get("nsfw_level", 0),
        # Flags
        system_channel_flags=data.get("system_channel_flags", 0),
        # Features
        features=data.get("features", []),
        # Nitro boost
        premium_tier=data.get("premium_tier", 0),
        premium_subscription_count=data.get("premium_subscription_count"),
        premium_progress_bar_enabled=data.get("premium_progress_bar_enabled", False),
        # Vanity and locale
        vanity_url_code=data.get("vanity_url_code"),
        preferred_locale=data.get("preferred_locale", "en-US"),
        # Application and limits
        application_id=int(data["application_id"]) if data.get("application_id") else None,
        max_presences=data.get("max_presences"),
        max_members=data.get("max_members"),
        max_video_channel_users=data.get("max_video_channel_users"),
        max_stage_video_channel_users=data.get("max_stage_video_channel_users"),
        approximate_member_count=data.get("approximate_member_count"),
        approximate_presence_count=data.get("approximate_presence_count"),
        # JSONB fields
        welcome_screen=data.get("welcome_screen"),
        incidents_data=data.get("incidents_data"),
        # Raw payload
        raw=data,
    )
