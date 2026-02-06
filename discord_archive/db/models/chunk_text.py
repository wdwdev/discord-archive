"""ChunkText model for storing built chunk texts.

This module defines the ChunkText entity which stores pre-built text
representations of chunks for use in embedding.

Design principles:
- 1:1 relationship with Chunk (chunk_id is both PK and FK)
- Text is built during chunking to reduce query complexity at embedding time
- Token count is pre-computed for embedding optimization
"""

from datetime import datetime

from sqlalchemy import BigInteger, ForeignKey, Integer, Text
from sqlalchemy.orm import Mapped, mapped_column

from discord_archive.db.base import Base, TZDateTime, utcnow


class ChunkText(Base):
    """Pre-built text representation of a chunk.

    Each row corresponds exactly to one Chunk and contains:
    - The formatted text for embedding
    - Pre-computed token count
    - Timestamp of when the text was built

    The text format varies by chunk type:
    - sliding_window/author_group: Sequential messages with author/date headers
    - reply_chain: Threaded format with reply indicators
    """

    __tablename__ = "chunk_texts"

    # Primary key (1:1 with chunks table)
    chunk_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("chunks.chunk_id", ondelete="CASCADE"),
        primary_key=True,
    )

    # The formatted text for embedding
    text: Mapped[str] = mapped_column(Text, nullable=False)

    # Pre-computed token count using the embedding model's tokenizer
    token_count: Mapped[int] = mapped_column(Integer, nullable=False)

    # When this text was built
    built_at: Mapped[datetime] = mapped_column(
        TZDateTime, nullable=False, default=utcnow
    )

    def __repr__(self) -> str:
        text_preview = self.text[:50] + "..." if len(self.text) > 50 else self.text
        return (
            f"ChunkText(chunk_id={self.chunk_id}, "
            f"tokens={self.token_count}, text='{text_preview}')"
        )
