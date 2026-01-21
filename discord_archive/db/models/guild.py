"""Discord Guild (Server) ORM model.

This module defines the Guild entity for a Discord guild message archival system.
Guild is the ROOT AGGREGATE for most other entities (channels, roles, members, etc.).

LATEST-STATE SNAPSHOT: Each re-ingestion overwrites the previous state.
No historical tracking of guild settings, boosts, features, etc.

Design principles:
- guild_id (snowflake) is the ONLY authoritative identity
- All other fields are best-effort metadata from Discord API
- Child entities (Channel, Role, Member, Emoji, Sticker, Webhook, ScheduledEvent)
  use hard FKs with CASCADE to this table
- References FROM guild (owner_id, channel IDs) are SOFT (no FK) because
  referenced entities may not exist, may be deleted, or may be partial
- JSONB is used for evolving/nested structures (welcome_screen, incidents_data)
"""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Boolean, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from discord_archive.db.base import Base, TZDateTime, utcnow

if TYPE_CHECKING:
    from discord_archive.db.models.channel import Channel
    from discord_archive.db.models.emoji import Emoji
    from discord_archive.db.models.guild_scheduled_event import GuildScheduledEvent
    from discord_archive.db.models.role import Role
    from discord_archive.db.models.sticker import Sticker


class Guild(Base):
    """
    Discord Guild (Server) entity.

    ROOT AGGREGATE: Most other entities in the archive reference this table.
    LATEST-STATE SNAPSHOT: Overwrites on each re-ingestion; no historical tracking.

    Key design decisions:
    - Child entities (Channel, Role, etc.) have hard FK → guild_id with CASCADE
    - References FROM this table (owner_id, channel IDs) are SOFT (no FK)
      because users/channels may not exist in archive or may be deleted
    - JSONB fields are raw API snapshots for forward compatibility only
    """

    __tablename__ = "guilds"

    # -------------------------------------------------------------------------
    # Primary Key (Authoritative Identity)
    # -------------------------------------------------------------------------

    # Discord snowflake ID. This is the ONLY authoritative identity.
    guild_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    # -------------------------------------------------------------------------
    # Basic Guild Info
    # -------------------------------------------------------------------------

    # Guild name. Required by Discord API.
    name: Mapped[str] = mapped_column(String(400), nullable=False)

    # Visual assets (all optional, stored as CDN hashes)
    icon: Mapped[str | None] = mapped_column(String(255), nullable=True)
    icon_hash: Mapped[str | None] = mapped_column(String(255), nullable=True)
    splash: Mapped[str | None] = mapped_column(String(255), nullable=True)
    discovery_splash: Mapped[str | None] = mapped_column(String(255), nullable=True)
    banner: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # Guild description (for discoverable/community guilds)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # -------------------------------------------------------------------------
    # Owner Reference (SOFT - No FK)
    # -------------------------------------------------------------------------

    # SOFT REFERENCE: Owner user_id. No FK constraint because:
    #   - User may not be ingested yet (ordering)
    #   - User may have left/been banned after ownership transfer
    #   - User row may be pruned from archive
    # This is informational metadata, not an integrity constraint.
    owner_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # -------------------------------------------------------------------------
    # Channel References (SOFT - No FK)
    # -------------------------------------------------------------------------

    # All channel references are SOFT (no FK) because:
    #   - Channels may be deleted
    #   - Channels may not be ingested (permissions, ordering)
    #   - Setting to a non-existent channel is a valid Discord state

    # AFK voice channel and timeout (seconds)
    afk_channel_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    afk_timeout: Mapped[int] = mapped_column(Integer, nullable=False, default=300)

    # Widget settings
    widget_enabled: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    widget_channel_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # System messages channel (welcome, boost notifications)
    system_channel_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # Community guild channels
    rules_channel_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    public_updates_channel_id: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    safety_alerts_channel_id: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )

    # -------------------------------------------------------------------------
    # Moderation & Security Levels (Integer Enums)
    # -------------------------------------------------------------------------

    # Verification level: 0=NONE, 1=LOW, 2=MEDIUM, 3=HIGH, 4=VERY_HIGH
    verification_level: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Default notification setting: 0=ALL_MESSAGES, 1=ONLY_MENTIONS
    default_message_notifications: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )

    # Explicit content filter: 0=DISABLED, 1=MEMBERS_WITHOUT_ROLES, 2=ALL_MEMBERS
    explicit_content_filter: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )

    # MFA requirement for moderation: 0=NONE, 1=ELEVATED
    mfa_level: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # NSFW level: 0=DEFAULT, 1=EXPLICIT, 2=SAFE, 3=AGE_RESTRICTED
    nsfw_level: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # -------------------------------------------------------------------------
    # Flags (Bitfields)
    # -------------------------------------------------------------------------

    # System channel flags bitfield. Controls which system messages are sent.
    # 0 = all enabled. See Discord API for flag definitions.
    system_channel_flags: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )

    # -------------------------------------------------------------------------
    # Features & Capabilities
    # -------------------------------------------------------------------------

    # Guild feature flags (COMMUNITY, VERIFIED, PARTNERED, etc.)
    # Stored as array of strings for flexibility as Discord adds features.
    features: Mapped[list[str]] = mapped_column(
        ARRAY(String), nullable=False, default=list
    )

    # -------------------------------------------------------------------------
    # Nitro Boost Info
    # -------------------------------------------------------------------------

    # Boost tier: 0=NONE, 1=TIER_1, 2=TIER_2, 3=TIER_3
    premium_tier: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Number of active boosts. NULL if not provided by API.
    premium_subscription_count: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )

    # Whether boost progress bar is shown.
    premium_progress_bar_enabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )

    # -------------------------------------------------------------------------
    # Vanity & Locale
    # -------------------------------------------------------------------------

    # Custom invite URL code (e.g., "discord-developers")
    vanity_url_code: Mapped[str | None] = mapped_column(String(32), nullable=True)

    # Preferred locale for Community guilds (e.g., "en-US", "ja")
    preferred_locale: Mapped[str] = mapped_column(
        String(16), nullable=False, default="en-US"
    )

    # -------------------------------------------------------------------------
    # Application & Limits
    # -------------------------------------------------------------------------

    # Application ID if guild was created by a bot.
    application_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # Member/presence limits. NULL if not provided by API.
    max_presences: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_members: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_video_channel_users: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_stage_video_channel_users: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )

    # Approximate counts (only present when fetched with with_counts=true)
    approximate_member_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    approximate_presence_count: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )

    # -------------------------------------------------------------------------
    # JSONB Fields (Raw API Snapshots)
    # -------------------------------------------------------------------------

    # Welcome screen configuration. JSONB because:
    #   - Nested structure (description, welcome_channels[])
    #   - Rarely queried by internal fields
    #   - Structure may evolve
    # This is a RAW SNAPSHOT, not authoritative for channel validity.
    welcome_screen: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # Incidents/raid data. JSONB for forward compatibility.
    # Structure is underdocumented and may change.
    incidents_data: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # -------------------------------------------------------------------------
    # Forward Compatibility & Archival Metadata
    # -------------------------------------------------------------------------

    # Complete raw API response for forward compatibility.
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # When our system ingested this guild snapshot.
    archived_at: Mapped[datetime] = mapped_column(
        TZDateTime, nullable=False, default=utcnow
    )

    # When this row was last updated (re-ingestion timestamp).
    updated_at: Mapped[datetime] = mapped_column(
        TZDateTime, nullable=False, default=utcnow, onupdate=utcnow
    )

    # -------------------------------------------------------------------------
    # Relationships (Hard FK from children, CASCADE delete)
    # -------------------------------------------------------------------------

    # All child entities have hard FK to guild_id with ondelete=CASCADE.
    # If a guild is deleted from the archive, all its children go too.

    roles: Mapped[list["Role"]] = relationship(
        "Role", back_populates="guild", lazy="selectin", cascade="all, delete-orphan"
    )
    channels: Mapped[list["Channel"]] = relationship(
        "Channel", back_populates="guild", lazy="selectin", cascade="all, delete-orphan"
    )
    emojis: Mapped[list["Emoji"]] = relationship(
        "Emoji", back_populates="guild", lazy="selectin", cascade="all, delete-orphan"
    )
    stickers: Mapped[list["Sticker"]] = relationship(
        "Sticker", back_populates="guild", lazy="selectin", cascade="all, delete-orphan"
    )
    scheduled_events: Mapped[list["GuildScheduledEvent"]] = relationship(
        "GuildScheduledEvent",
        back_populates="guild",
        lazy="selectin",
        cascade="all, delete-orphan",
    )

    # -------------------------------------------------------------------------
    # Indexes
    # -------------------------------------------------------------------------

    __table_args__ = (
        # Guild name search (optional, for admin queries)
        Index("ix_guilds_name", "name"),
        # Find guilds by owner (useful for multi-guild bot scenarios)
        Index("ix_guilds_owner_id", "owner_id"),
    )

    def __repr__(self) -> str:
        return f"<Guild(guild_id={self.guild_id}, name='{self.name}')>"
