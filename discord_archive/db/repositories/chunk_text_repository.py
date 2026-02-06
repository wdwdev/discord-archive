"""Repository for ChunkText operations."""

import logging

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from discord_archive.db.models.chunk_text import ChunkText
from discord_archive.utils.time import utcnow

logger = logging.getLogger(__name__)

# Batch size for bulk inserts (500 rows * 3 params = 1500, well under 32767 limit)
BATCH_SIZE = 500


async def bulk_insert_chunk_texts(
    session: AsyncSession,
    chunk_texts: list[tuple[int, str, int]],
) -> None:
    """Bulk insert chunk texts with ON CONFLICT DO UPDATE.

    Args:
        session: Database session
        chunk_texts: List of (chunk_id, text, token_count) tuples

    Uses ON CONFLICT DO UPDATE to handle re-chunking scenarios where
    a chunk's text may need to be rebuilt.
    """
    if not chunk_texts:
        return

    now = utcnow()

    for i in range(0, len(chunk_texts), BATCH_SIZE):
        batch = chunk_texts[i : i + BATCH_SIZE]
        values_list = [
            {
                "chunk_id": chunk_id,
                "text": text,
                "token_count": token_count,
                "built_at": now,
            }
            for chunk_id, text, token_count in batch
        ]

        stmt = (
            pg_insert(ChunkText)
            .values(values_list)
            .on_conflict_do_update(
                index_elements=["chunk_id"],
                set_={
                    "text": pg_insert(ChunkText).excluded.text,
                    "token_count": pg_insert(ChunkText).excluded.token_count,
                    "built_at": now,
                },
            )
        )
        await session.execute(stmt)
