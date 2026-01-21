"""Discord Role ORM model."""

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Boolean, ForeignKey, Index, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from discord_archive.db.base import Base, PermissionBitfield, TZDateTime, utcnow

if TYPE_CHECKING:
    from discord_archive.db.models.guild import Guild


class Role(Base):
    """
    Discord Role entity.

    Stores the latest-state snapshot of a Discord role.
    Updated on each re-ingestion.
    """

    __tablename__ = "roles"

    # Primary key - Discord snowflake
    role_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    # Hard FK to guilds. Roles cannot exist without a guild.
    # CASCADE: if guild is deleted from archive, its roles go too.
    guild_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), nullable=False
    )

    # Role properties
    name: Mapped[str] = mapped_column(String(400), nullable=False)
    color: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Role colors object (JSONB for gradient support)
    colors: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # Display settings
    hoist: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    position: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    mentionable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Icon (for guilds with ROLE_ICONS feature)
    icon: Mapped[str | None] = mapped_column(String(255), nullable=True)
    unicode_emoji: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # Permissions bitfield. Stored as NUMERIC(20,0) to handle Discord's
    # 64-bit permission flags. Use Decimal for Python-side handling.
    permissions: Mapped[Decimal] = mapped_column(
        PermissionBitfield, nullable=False, default=Decimal("0")
    )

    # Managed by integration
    managed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Tags (integration info, boost role, etc.)
    tags: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # Role flags (bitfield)
    flags: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

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
    guild: Mapped["Guild"] = relationship("Guild", back_populates="roles")

    __table_args__ = (
        Index("ix_roles_guild_id", "guild_id"),
        Index("ix_roles_position", "guild_id", "position"),
    )

    def __repr__(self) -> str:
        return f"<Role(role_id={self.role_id}, name='{self.name}')>"
