"""Chunking checkpoint model for tracking chunking progress."""

from datetime import datetime

from sqlalchemy import BigInteger, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from discord_archive.db.base import Base, TZDateTime, utcnow


class ChunkingCheckpoint(Base):
    """Tracks chunking progress per channel.

    One row per channel. The last_message_id indicates the last message
    that was processed by chunking.
    """

    __tablename__ = "chunking_checkpoints"

    # Primary key (one row per channel)
    channel_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("channels.channel_id", ondelete="CASCADE"),
        primary_key=True,
    )

    # Progress tracking
    last_message_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # Metadata
    updated_at: Mapped[datetime] = mapped_column(
        TZDateTime,
        nullable=False,
        default=utcnow,
        onupdate=utcnow,
    )

    def __repr__(self) -> str:
        return (
            f"ChunkingCheckpoint(channel={self.channel_id}, "
            f"last_message={self.last_message_id})"
        )
