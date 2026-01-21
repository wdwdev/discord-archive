"""Discord Ingest Checkpoint ORM model.

This module defines the IngestCheckpoint entity for tracking ingestion progress.
Each channel has exactly one checkpoint row that tracks:
- Backfill progress (oldest_message_id): how far back we've downloaded
- Incremental progress (newest_message_id): how recent we've downloaded
- Completion status (backfill_complete): whether we've reached the channel's first message

Design principles:
- One row per channel (hard FK)
- Updated after each batch of messages is persisted
- Enables resumable backfill and incremental sync
"""

from datetime import datetime

from sqlalchemy import BigInteger, Boolean, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column

from discord_archive.db.base import Base, TZDateTime, utcnow


class IngestCheckpoint(Base):
    """
    Tracks ingestion progress for a Discord channel.

    One row per channel. Used to implement resumable backfill and incremental sync:
    - Backfill: fetch messages with `before=oldest_message_id` until no more remain
    - Incremental: fetch messages with `after=newest_message_id` until caught up

    Lifecycle:
    - Created when first message batch is ingested for a channel
    - Updated after each successful batch persist
    - backfill_complete=True once we receive an empty response from Discord API
    """

    __tablename__ = "ingest_checkpoints"

    # -------------------------------------------------------------------------
    # Primary Key
    # -------------------------------------------------------------------------

    # Hard FK to channels table. If channel is deleted, checkpoint is deleted.
    channel_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("channels.channel_id", ondelete="CASCADE"),
        primary_key=True,
    )

    # -------------------------------------------------------------------------
    # Guild Reference (Denormalized for query convenience)
    # -------------------------------------------------------------------------

    # Denormalized guild_id for efficient "all checkpoints in guild" queries.
    # Soft reference (no FK) since we query this for progress reporting.
    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # -------------------------------------------------------------------------
    # Backfill Progress
    # -------------------------------------------------------------------------

    # The oldest message ID we have ingested for this channel.
    # Backfill fetches messages with `before=oldest_message_id`.
    # NULL means no messages have been ingested yet.
    oldest_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # Whether backfill has completed (reached the channel's first message).
    # True = Discord API returned empty response when we asked for older messages.
    # False = there may be more historical messages to fetch.
    backfill_complete: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )

    # -------------------------------------------------------------------------
    # Incremental Progress
    # -------------------------------------------------------------------------

    # The newest message ID we have ingested for this channel.
    # Incremental fetches messages with `after=newest_message_id`.
    # NULL means no messages have been ingested yet.
    newest_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # -------------------------------------------------------------------------
    # Sync Metadata
    # -------------------------------------------------------------------------

    # When this checkpoint was last updated (successful batch persist).
    last_synced_at: Mapped[datetime] = mapped_column(
        TZDateTime, nullable=False, default=utcnow
    )

    # When this checkpoint was first created.
    created_at: Mapped[datetime] = mapped_column(
        TZDateTime, nullable=False, default=utcnow
    )

    # -------------------------------------------------------------------------
    # Indexes
    # -------------------------------------------------------------------------

    __table_args__ = (
        # Find all checkpoints for a guild (progress reporting)
        Index("ix_ingest_checkpoints_guild_id", "guild_id"),
        # Find incomplete backfills
        Index("ix_ingest_checkpoints_backfill_complete", "backfill_complete"),
    )

    def __repr__(self) -> str:
        return (
            f"<IngestCheckpoint(channel_id={self.channel_id}, "
            f"backfill_complete={self.backfill_complete})>"
        )
