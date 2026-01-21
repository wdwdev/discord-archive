"""Discord Reaction ORM model."""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Boolean, ForeignKey, Index, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from discord_archive.db.base import Base, TZDateTime, utcnow

if TYPE_CHECKING:
    from discord_archive.db.models.message import Message


class Reaction(Base):
    """
    Discord Message Reaction entity.

    Stores aggregated reaction data per message.
    Note: Does NOT store per-user reaction state (reaction.me is token-dependent).
    """

    __tablename__ = "reactions"

    # Composite primary key: message + emoji identifier
    message_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("messages.message_id", ondelete="CASCADE"),
        primary_key=True,
    )

    # Canonical emoji identifier for composite primary key.
    # Convention (must be followed consistently during ingestion):
    #   - Custom emoji:  "custom:<emoji_id>"   e.g. "custom:123456789"
    #   - Unicode emoji: "unicode:<emoji_name>" e.g. "unicode:👍" or "unicode:thumbsup"
    # Notes:
    #   - emoji_name is stored exactly as provided by Discord API (no normalization)
    #   - This convention guarantees deterministic uniqueness across ingestion runs
    #   - Do NOT hash unicode emoji; use the literal name/character
    emoji_key: Mapped[str] = mapped_column(String(64), primary_key=True)

    # Emoji identification (denormalized for convenience; emoji_key is authoritative)
    emoji_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    emoji_name: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # Whether this is an animated custom emoji. NULL for unicode emoji
    # (only custom emoji can be animated).
    emoji_animated: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    # Reaction counts
    count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    count_details: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # Burst reaction colors (super reactions)
    burst_colors: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)

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
    message: Mapped["Message"] = relationship("Message", back_populates="reactions")

    __table_args__ = (
        Index("ix_reactions_message_id", "message_id"),
        Index("ix_reactions_emoji_id", "emoji_id"),
    )

    def __repr__(self) -> str:
        emoji_display = self.emoji_name or str(self.emoji_id)
        return f"<Reaction(message_id={self.message_id}, emoji='{emoji_display}', count={self.count})>"

