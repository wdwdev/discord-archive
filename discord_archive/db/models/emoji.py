"""Discord Custom Emoji ORM model.

This module defines the Emoji entity for a Discord guild message archival system.
Emojis are LATEST-STATE SNAPSHOTS: each re-ingestion overwrites the previous state.

SCOPE: This table stores CUSTOM GUILD EMOJIS ONLY.
- Custom emojis have a snowflake ID and belong to a specific guild
- Unicode/standard emojis are NOT represented here
- For unicode emojis in reactions, see Reaction.emoji_key ("unicode:<name>")

Design principles:
- emoji_id (snowflake) is the authoritative identity
- Roles allowlist stored as ARRAY for snapshot semantics (no join table)
- user_id (creator) is a soft reference; creator may not exist in archive
- No historical tracking of emoji availability, role restrictions, or properties
"""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Boolean, ForeignKey, Index, String
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from discord_archive.db.base import Base, TZDateTime, utcnow

if TYPE_CHECKING:
    from discord_archive.db.models.guild import Guild


class Emoji(Base):
    """
    Discord Custom Emoji entity.

    LATEST-STATE SNAPSHOT: Stores custom guild emojis only. Overwrites on each
    re-ingestion; no historical tracking of availability or role restrictions.

    IMPORTANT: This table does NOT include Unicode/standard emojis.
    Unicode emojis appear only in message reactions (Reaction table) where
    they are identified by emoji_key = "unicode:<emoji_name>".

    Custom emojis have:
    - A unique snowflake ID (emoji_id)
    - A parent guild (guild_id)
    - Optional role restrictions (roles array)
    - A creator (user_id, soft reference)
    """

    __tablename__ = "emojis"

    # -------------------------------------------------------------------------
    # Primary Key
    # -------------------------------------------------------------------------

    # Discord snowflake ID. Unique identifier for this custom emoji.
    emoji_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    # -------------------------------------------------------------------------
    # Guild Reference (Hard FK)
    # -------------------------------------------------------------------------

    # Custom emojis always belong to a guild. Hard FK with CASCADE:
    # if guild is deleted from archive, its emojis go too.
    guild_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), nullable=False
    )

    # -------------------------------------------------------------------------
    # Emoji Properties
    # -------------------------------------------------------------------------

    # Emoji name (e.g., "smile", "custom_emoji").
    # Note: String(128) to handle Unicode characters.
    name: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # Whether this is an animated emoji (GIF vs static image).
    animated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Whether this emoji is currently usable by guild members.
    # False when emoji is disabled (e.g., guild lost boost tier that enabled it).
    # NOTE: This is Discord's current state, not a deletion flag. An unavailable
    # emoji may become available again if the guild regains boost levels.
    available: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    # Whether this emoji is managed by an integration (Twitch, YouTube, etc.).
    # Managed emojis cannot be modified by users; they're controlled by the
    # integration that created them.
    managed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Whether this emoji must be wrapped in colons to use.
    # Almost always true; false only for certain legacy emojis.
    require_colons: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    # -------------------------------------------------------------------------
    # Role Restrictions (Allowlist)
    # -------------------------------------------------------------------------

    # Array of role IDs that are allowed to use this emoji.
    # Semantics:
    #   - NULL or empty array: emoji is usable by everyone in the guild
    #   - Non-empty array: only members with at least one of these roles can use it
    # Stored as ARRAY(BigInteger) rather than a join table because:
    #   1. This is a SNAPSHOT model; we don't track role restriction history
    #   2. Rarely queried by individual role ("emojis restricted to role X")
    #   3. Role IDs are soft references; roles may be deleted without cascade
    roles: Mapped[list[int] | None] = mapped_column(ARRAY(BigInteger), nullable=True)

    # -------------------------------------------------------------------------
    # Creator Reference (SOFT - No FK)
    # -------------------------------------------------------------------------

    # User who created/uploaded this emoji. SOFT REFERENCE (no FK) because:
    #   - Creator may not be in the users table (not ingested, left guild)
    #   - Creator may be an integration or bot (not a regular user)
    #   - Creator account may be deleted
    # This is informational metadata only; not required for emoji functionality.
    user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # -------------------------------------------------------------------------
    # Forward Compatibility & Archival Metadata
    # -------------------------------------------------------------------------

    # Complete raw API response for forward compatibility.
    # Preserves any new fields Discord adds without schema migration.
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # When our system ingested this emoji snapshot.
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

    guild: Mapped["Guild"] = relationship("Guild", back_populates="emojis")

    # -------------------------------------------------------------------------
    # Indexes
    # -------------------------------------------------------------------------

    __table_args__ = (
        # Find all emojis for a guild
        Index("ix_emojis_guild_id", "guild_id"),
        # Search emojis by name
        Index("ix_emojis_name", "name"),
    )

    def __repr__(self) -> str:
        return f"<Emoji(emoji_id={self.emoji_id}, name='{self.name}')>"
