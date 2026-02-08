"""Chunk model for RAG."""

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Index,
    Text,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from discord_archive.db.base import Base, TZDateTime, utcnow


class Chunk(Base):
    """A chunk of messages for RAG retrieval.

    Chunks are derived from messages and come in three types:
    - sliding_window: Overlapping windows of consecutive messages
    - author_group: Consecutive messages from the same author
    - reply_chain: A reply chain from root to leaf message

    Chunks can be 'open' (still accepting messages) or 'closed' (immutable).
    """

    __tablename__ = "chunks"

    # Primary key
    chunk_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # Chunk type
    chunk_type: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    # References (no FK to allow independent operation)
    guild_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    channel_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # Message content
    message_ids: Mapped[list[int]] = mapped_column(
        ARRAY(BigInteger),
        nullable=False,
    )
    author_ids: Mapped[list[int]] = mapped_column(
        ARRAY(BigInteger),
        nullable=False,
    )

    # Mention metadata (for filtering)
    mentioned_user_ids: Mapped[list[int]] = mapped_column(
        ARRAY(BigInteger),
        nullable=False,
        default=list,
    )
    mentioned_role_ids: Mapped[list[int]] = mapped_column(
        ARRAY(BigInteger),
        nullable=False,
        default=list,
    )
    has_attachments: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
    )

    # Chunk state
    chunk_state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    # Deterministic identity anchors
    start_message_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    leaf_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # Cross-channel reference (for reply chains that hit channel boundary)
    cross_channel_ref: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # Embedding status for incremental embedding
    embedding_status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default="pending",
    )

    # Message time range (for filtering by when messages were sent)
    first_message_at: Mapped[datetime] = mapped_column(TZDateTime, nullable=False)
    last_message_at: Mapped[datetime] = mapped_column(TZDateTime, nullable=False)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        TZDateTime,
        nullable=False,
        default=utcnow,
    )
    updated_at: Mapped[datetime] = mapped_column(
        TZDateTime,
        nullable=False,
        default=utcnow,
        onupdate=utcnow,
    )

    __table_args__ = (
        # Check constraints
        CheckConstraint(
            "chunk_type IN ('reply_chain', 'author_group', 'sliding_window')",
            name="ck_chunks_chunk_type",
        ),
        CheckConstraint(
            "chunk_state IN ('open', 'closed')",
            name="ck_chunks_chunk_state",
        ),
        CheckConstraint(
            "embedding_status IN ('pending', 'embedded')",
            name="ck_chunks_embedding_status",
        ),
        # Deterministic identity indexes (partial unique indexes)
        Index(
            "uq_chunks_reply_chain",
            "chunk_type",
            "leaf_message_id",
            unique=True,
            postgresql_where="chunk_type = 'reply_chain'",
        ),
        Index(
            "uq_chunks_author_group",
            "chunk_type",
            "channel_id",
            "start_message_id",
            unique=True,
            postgresql_where="chunk_type = 'author_group'",
        ),
        Index(
            "uq_chunks_sliding_window",
            "chunk_type",
            "channel_id",
            "start_message_id",
            unique=True,
            postgresql_where="chunk_type = 'sliding_window'",
        ),
        # Operational indexes
        Index(
            "ix_chunks_open_by_channel",
            "channel_id",
            "created_at",
            postgresql_where="chunk_state = 'open'",
        ),
        Index(
            "ix_chunks_author_group_open",
            "channel_id",
            postgresql_where="chunk_type = 'author_group' AND chunk_state = 'open'",
        ),
        Index(
            "ix_chunks_pending",
            "channel_id",
            postgresql_where="embedding_status = 'pending'",
        ),
        Index("ix_chunks_updated", "updated_at"),
        Index("ix_chunks_channel_type", "channel_id", "chunk_type"),
    )

    def __repr__(self) -> str:
        return (
            f"Chunk(id={self.chunk_id}, type={self.chunk_type}, "
            f"channel={self.channel_id}, messages={len(self.message_ids)}, "
            f"state={self.chunk_state})"
        )
