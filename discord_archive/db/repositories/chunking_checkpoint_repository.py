"""Repository for chunking checkpoint operations."""

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from discord_archive.db.models.chunking_checkpoint import ChunkingCheckpoint
from discord_archive.utils.time import utcnow


async def get_chunking_checkpoint(
    session: AsyncSession,
    channel_id: int,
) -> ChunkingCheckpoint | None:
    """Get the chunking checkpoint for a channel."""
    stmt = select(ChunkingCheckpoint).where(
        ChunkingCheckpoint.channel_id == channel_id
    )
    return await session.scalar(stmt)


async def upsert_chunking_checkpoint(
    session: AsyncSession,
    channel_id: int,
    last_message_id: int,
) -> None:
    """Create or update a chunking checkpoint."""
    now = utcnow()
    stmt = (
        pg_insert(ChunkingCheckpoint)
        .values(
            channel_id=channel_id,
            last_message_id=last_message_id,
            updated_at=now,
        )
        .on_conflict_do_update(
            index_elements=["channel_id"],
            set_={
                "last_message_id": last_message_id,
                "updated_at": now,
            },
        )
    )
    await session.execute(stmt)


async def get_all_chunking_checkpoints(
    session: AsyncSession,
) -> list[ChunkingCheckpoint]:
    """Get all chunking checkpoints."""
    stmt = select(ChunkingCheckpoint)
    result = await session.scalars(stmt)
    return list(result.all())
