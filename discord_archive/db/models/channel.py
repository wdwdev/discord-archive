"""Discord Channel ORM model.

This module defines the Channel entity for a Discord guild message archival system.
Channels are LATEST-STATE SNAPSHOTS: each re-ingestion overwrites the previous state.
Messages within channels are append-only (see message.py).

Design principles:
- JSONB is used for evolving/nested Discord structures (tags, thread_metadata)
- Self-referential parent_id supports category and thread parent relationships
- Permission overwrites are stored both as raw JSONB (API snapshot) and in the
  PermissionOverwrite table (authoritative, queryable)
"""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Boolean, ForeignKey, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from discord_archive.db.base import Base, TZDateTime, utcnow

if TYPE_CHECKING:
    from discord_archive.db.models.guild import Guild
    from discord_archive.db.models.message import Message


class Channel(Base):
    """
    Discord Channel entity.

    LATEST-STATE SNAPSHOT: Supports all channel types (text, voice, category,
    thread, forum, stage, media, DM, group DM). Overwrites on each re-ingestion.

    Key semantic notes:
    - `name` is NULL only for DM/group DM channels; always present for guild channels
    - `parent_id` references a category (for guild channels) or parent text channel
      (for threads); self-referential FK with SET NULL on delete
    - `permission_overwrites` JSONB is a raw API snapshot for forward compatibility;
      the `PermissionOverwrite` table is the authoritative source for querying
    """

    __tablename__ = "channels"

    # Primary key - Discord snowflake
    channel_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    # Guild reference (nullable for DM channels)
    guild_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), nullable=True
    )

    # Channel type (0=text, 2=voice, 4=category, 5=announcement, 10-12=threads, 13=stage, 15=forum, 16=media)
    type: Mapped[int] = mapped_column(Integer, nullable=False)

    # Channel name. NULL only for DM (type=1) and group DM (type=3) channels.
    # Guild channels always have a name.
    name: Mapped[str | None] = mapped_column(String(400), nullable=True)

    # Channel topic/description. Optional for all channel types.
    topic: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Position in channel list
    position: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Raw API snapshot of permission overwrites. Preserved for forward compatibility
    # and to capture any new fields Discord adds. For querying and permission logic,
    # use the PermissionOverwrite table which is the authoritative normalized source.
    permission_overwrites: Mapped[list[dict] | None] = mapped_column(
        JSONB, nullable=True
    )

    # Parent channel: category ID for guild channels, or parent text channel ID
    # for threads. Self-referential FK with SET NULL on parent deletion.
    parent_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("channels.channel_id", ondelete="SET NULL"),
        nullable=True,
    )

    # NSFW flag
    nsfw: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    # Last message ID (may not point to existing message)
    last_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # Voice channel settings
    bitrate: Mapped[int | None] = mapped_column(Integer, nullable=True)
    user_limit: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rtc_region: Mapped[str | None] = mapped_column(String(64), nullable=True)
    video_quality_mode: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Slowmode
    rate_limit_per_user: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Thread-specific fields
    owner_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    thread_metadata: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    message_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    member_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_message_sent: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Auto-archive settings
    default_auto_archive_duration: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    default_thread_rate_limit_per_user: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )

    # Forum/Media channel settings
    available_tags: Mapped[list[dict] | None] = mapped_column(JSONB, nullable=True)
    applied_tags: Mapped[list[int] | None] = mapped_column(
        ARRAY(BigInteger), nullable=True
    )
    default_reaction_emoji: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    default_sort_order: Mapped[int | None] = mapped_column(Integer, nullable=True)
    default_forum_layout: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Channel flags bitfield (PINNED, REQUIRE_TAG, HIDE_MEDIA_DOWNLOAD_OPTIONS, etc.)
    # Default to 0 (no flags) for consistent bitwise operations.
    flags: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # DM/Group DM fields
    recipients: Mapped[list[dict] | None] = mapped_column(JSONB, nullable=True)
    icon: Mapped[str | None] = mapped_column(String(255), nullable=True)
    application_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    managed: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    # Last pin timestamp
    last_pin_timestamp: Mapped[datetime | None] = mapped_column(
        TZDateTime, nullable=True
    )

    # Raw API payload for forward compatibility
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # Archival metadata
    archived_at: Mapped[datetime] = mapped_column(
        TZDateTime, nullable=False, default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        TZDateTime, nullable=False, default=utcnow, onupdate=utcnow
    )

    # Relationships
    guild: Mapped["Guild | None"] = relationship("Guild", back_populates="channels")
    messages: Mapped[list["Message"]] = relationship(
        "Message", back_populates="channel", lazy="selectin"
    )

    __table_args__ = (
        # Single-column indexes
        Index("ix_channels_guild_id", "guild_id"),
        Index("ix_channels_type", "type"),
        Index("ix_channels_parent_id", "parent_id"),
        # Composite indexes for common query patterns
        Index("ix_channels_guild_position", "guild_id", "position"),
        # "All text channels in guild" or "all threads in guild"
        Index("ix_channels_guild_type", "guild_id", "type"),
        # "All channels under category X" or "all threads under channel X"
        Index("ix_channels_guild_parent", "guild_id", "parent_id"),
    )

    def __repr__(self) -> str:
        return f"<Channel(channel_id={self.channel_id}, name='{self.name}', type={self.type})>"
