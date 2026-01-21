"""Discord Sticker ORM model.

This module defines the Sticker entity for a Discord guild message archival system.
Stickers are LATEST-STATE SNAPSHOTS: each re-ingestion overwrites the previous state.

SCOPE: This table stores BOTH sticker types in a SINGLE-TABLE design:
- Standard stickers (type=1): Nitro/pack-based, guild_id=NULL, pack_id set
- Guild stickers (type=2): Custom guild stickers, guild_id set, pack_id=NULL

IMPORTANT: Stickers are NOT emojis.
- Stickers do NOT appear in message content or reactions
- Stickers appear only via Message.sticker_items (JSONB)
- For emojis, see the Emoji table (custom) and Reaction.emoji_key (unicode)

Design principles:
- sticker_id (snowflake) is the authoritative identity
- No sticker_packs table; pack_id is informational only for standard stickers
- user_id (creator) is a soft reference; creator may not exist in archive
- tags is a comma-separated string, not parsed into structured data
"""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Boolean, ForeignKey, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from discord_archive.db.base import Base, TZDateTime, utcnow

if TYPE_CHECKING:
    from discord_archive.db.models.guild import Guild


class Sticker(Base):
    """
    Discord Sticker entity.

    LATEST-STATE SNAPSHOT: Stores both standard (pack-based) and guild custom
    stickers. Overwrites on each re-ingestion; no historical tracking.

    Single-table design:
    - Standard stickers: type=1, pack_id set, guild_id NULL
    - Guild stickers: type=2, guild_id set, pack_id NULL

    Stickers vs Emojis:
    - Stickers are standalone media sent in messages (via sticker_items)
    - Emojis appear inline in message content and as reactions
    - This table is for stickers ONLY; see Emoji table for custom emojis
    """

    __tablename__ = "stickers"

    # -------------------------------------------------------------------------
    # Primary Key
    # -------------------------------------------------------------------------

    # Discord snowflake ID. Unique identifier for this sticker.
    sticker_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    # -------------------------------------------------------------------------
    # Guild Reference (Nullable - Guild Stickers Only)
    # -------------------------------------------------------------------------

    # For GUILD stickers (type=2): the owning guild. Hard FK with CASCADE.
    # For STANDARD stickers (type=1): NULL (not guild-owned).
    # This allows a single table to store both sticker types.
    guild_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), nullable=True
    )

    # -------------------------------------------------------------------------
    # Pack Reference (Standard Stickers Only)
    # -------------------------------------------------------------------------

    # For STANDARD stickers (type=1): the sticker pack ID.
    # For GUILD stickers (type=2): NULL.
    # NOTE: We do NOT have a sticker_packs table. This is informational only,
    # used to group standard stickers by their Nitro pack for display purposes.
    pack_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # -------------------------------------------------------------------------
    # Sticker Properties
    # -------------------------------------------------------------------------

    # Sticker name. Required by Discord API.
    # Note: String(128) to handle Unicode characters.
    name: Mapped[str] = mapped_column(String(128), nullable=False)

    # Sticker description. Optional; may be empty or NULL.
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Autocomplete/search tags. Comma-separated keyword string (e.g., "happy,smile").
    # Used for client-side search hints only. NOT parsed into structured data.
    # NOT a taxonomy. May contain arbitrary user-entered text.
    tags: Mapped[str | None] = mapped_column(String(200), nullable=True)

    # -------------------------------------------------------------------------
    # Type & Format (Integer Enums)
    # -------------------------------------------------------------------------

    # Sticker type:
    #   1 = STANDARD (Nitro pack sticker, available to Nitro subscribers)
    #   2 = GUILD (custom sticker uploaded to a guild)
    type: Mapped[int] = mapped_column(Integer, nullable=False)

    # Sticker format type (image/animation format):
    #   1 = PNG (static image)
    #   2 = APNG (animated PNG)
    #   3 = LOTTIE (vector animation, deprecated for new uploads)
    #   4 = GIF (animated GIF)
    format_type: Mapped[int] = mapped_column(Integer, nullable=False)

    # -------------------------------------------------------------------------
    # Availability State
    # -------------------------------------------------------------------------

    # Whether this sticker is currently usable.
    # May be false if a guild lost boost tier that enabled custom stickers.
    # NOTE: This is Discord's current state, not a deletion flag.
    # NULL if not provided by API (partial sticker objects).
    available: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    # -------------------------------------------------------------------------
    # Creator Reference (SOFT - No FK)
    # -------------------------------------------------------------------------

    # User who uploaded this sticker. SOFT REFERENCE (no FK) because:
    #   - Creator may not be in the users table (not ingested, left guild)
    #   - Creator may be a bot or integration
    #   - Creator account may be deleted
    # Informational metadata only; not required for sticker functionality.
    user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # -------------------------------------------------------------------------
    # Pack Ordering (Standard Stickers Only)
    # -------------------------------------------------------------------------

    # Sort order within a sticker pack. Only meaningful for standard stickers.
    # NULL for guild stickers or if not provided by API.
    sort_value: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # -------------------------------------------------------------------------
    # Forward Compatibility & Archival Metadata
    # -------------------------------------------------------------------------

    # Complete raw API response for forward compatibility.
    # Preserves any new fields Discord adds without schema migration.
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # When our system ingested this sticker snapshot.
    archived_at: Mapped[datetime] = mapped_column(
        TZDateTime, nullable=False, default=utcnow
    )

    # When this row was last updated (re-ingestion timestamp).
    updated_at: Mapped[datetime] = mapped_column(
        TZDateTime, nullable=False, default=utcnow, onupdate=utcnow
    )

    # -------------------------------------------------------------------------
    # Relationships
    # -------------------------------------------------------------------------

    # Nullable relationship because standard stickers have no guild.
    guild: Mapped["Guild | None"] = relationship("Guild", back_populates="stickers")

    # -------------------------------------------------------------------------
    # Indexes
    # -------------------------------------------------------------------------

    __table_args__ = (
        # Find all stickers for a guild (guild stickers only)
        Index("ix_stickers_guild_id", "guild_id"),
        # Find all stickers in a pack (standard stickers only)
        Index("ix_stickers_pack_id", "pack_id"),
    )

    def __repr__(self) -> str:
        return f"<Sticker(sticker_id={self.sticker_id}, name='{self.name}')>"
