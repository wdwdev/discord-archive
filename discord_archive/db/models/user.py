"""Discord User ORM model.

This module defines the User entity for a Discord guild message archival system.
Users are LATEST-STATE SNAPSHOTS: each re-ingestion overwrites the previous state.

Design principles:
- user_id (snowflake) is the ONLY authoritative identity
- Display fields (username, discriminator, global_name, avatar) are best-effort metadata
- No historical tracking of username changes, avatar history, etc.
- Partial user objects from API (e.g., in mentions) may have missing fields

Key differences from GuildMember:
- User is global Discord identity; GuildMember is per-guild membership
- User fields may be incomplete; GuildMember always has a valid user_id FK
- User is referenced softly from Message.author_id; GuildMember has hard FK
"""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Boolean, Index, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from discord_archive.db.base import Base, TZDateTime, utcnow

if TYPE_CHECKING:
    from discord_archive.db.models.message import Message


class User(Base):
    """
    Discord User entity.

    LATEST-STATE SNAPSHOT: Stores the most recent known state of a Discord user.
    Overwrites on each re-ingestion; no historical tracking.

    Important: user_id is the ONLY authoritative identity. All other fields
    (username, discriminator, global_name, avatar, etc.) are best-effort
    display metadata that may be:
    - Missing (partial user objects from mentions, reactions, etc.)
    - Outdated (user changed their name since last ingestion)
    - NULL (system users, deleted accounts, webhook pseudo-users)

    For guild-specific identity (nick, roles, etc.), see GuildMember.
    """

    __tablename__ = "users"

    # -------------------------------------------------------------------------
    # Primary Key (Authoritative Identity)
    # -------------------------------------------------------------------------

    # Discord snowflake ID. This is the ONLY authoritative identity.
    # All other fields are best-effort display metadata.
    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    # -------------------------------------------------------------------------
    # Display Identity (Best-Effort Metadata)
    # -------------------------------------------------------------------------

    # Username. Nullable because:
    #   - Partial user objects (from mentions, reactions) may omit this
    #   - Deleted users may have no username
    #   - System/webhook pseudo-users may lack this
    # This is the latest-known value; we do NOT track username history.
    # Note: String(128) to handle Unicode characters (Discord allows 32 chars,
    # but each Unicode char can be up to 4 bytes).
    username: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # Legacy discriminator (e.g., "1234"). Nullable because:
    #   - New username system uses "0" or omits discriminator entirely
    #   - Partial user objects may omit this
    discriminator: Mapped[str | None] = mapped_column(String(4), nullable=True)

    # Display name (new username system). May differ from username.
    # Note: String(128) to handle Unicode characters.
    global_name: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # -------------------------------------------------------------------------
    # Visual Identity (Best-Effort Metadata)
    # -------------------------------------------------------------------------

    # Avatar hash. NULL if user has no custom avatar (uses default).
    avatar: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # Avatar decoration data (premium feature). JSONB for evolving structure.
    avatar_decoration_data: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # Profile banner hash. NULL if user has no banner.
    banner: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # Profile accent color (integer representation of hex color).
    accent_color: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # -------------------------------------------------------------------------
    # Account Type Flags
    # -------------------------------------------------------------------------

    # Whether this is a bot account.
    bot: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    # Whether this is an Official Discord System user.
    system: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    # -------------------------------------------------------------------------
    # Public Flags (Bitfield)
    # -------------------------------------------------------------------------

    # Public flags bitfield (STAFF, PARTNER, HYPESQUAD, BUG_HUNTER, etc.)
    # Default to 0 (no flags) for consistent bitwise operations.
    # See Discord API docs for flag definitions.
    public_flags: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)

    # -------------------------------------------------------------------------
    # Nitro / Premium
    # -------------------------------------------------------------------------

    # Premium type (0=None, 1=Nitro Classic, 2=Nitro, 3=Nitro Basic).
    # NULL if not known (not included in partial user objects).
    premium_type: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # -------------------------------------------------------------------------
    # Forward Compatibility & Archival Metadata
    # -------------------------------------------------------------------------

    # Raw API payload for forward compatibility
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # Archival metadata
    archived_at: Mapped[datetime] = mapped_column(
        TZDateTime, nullable=False, default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        TZDateTime, nullable=False, default=utcnow, onupdate=utcnow
    )

    # -------------------------------------------------------------------------
    # Relationships
    # -------------------------------------------------------------------------

    # Messages authored by this user. IMPORTANT CAVEATS:
    #   - This is a CONVENIENCE JOIN only, not authoritative
    #   - Messages with author_id matching this user_id will appear here
    #   - Messages authored by webhooks, system, or deleted users will NOT
    #     appear here (they reference author_id values not in users table)
    #   - viewonly=True because Message.author_id is a soft reference
    messages: Mapped[list["Message"]] = relationship(
        "Message",
        back_populates="author",
        primaryjoin="User.user_id == foreign(Message.author_id)",
        lazy="selectin",
        viewonly=True,
    )

    # -------------------------------------------------------------------------
    # Indexes
    # -------------------------------------------------------------------------

    __table_args__ = (
        Index("ix_users_username", "username"),
        Index("ix_users_bot", "bot"),
    )

    def __repr__(self) -> str:
        return f"<User(user_id={self.user_id}, username='{self.username}')>"
