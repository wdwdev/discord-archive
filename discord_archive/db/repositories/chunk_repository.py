"""Repository for chunk operations."""

import logging
from datetime import datetime

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from discord_archive.db.models.chunk import Chunk
from discord_archive.utils.time import utcnow

logger = logging.getLogger(__name__)


async def get_open_chunks_by_channel(
    session: AsyncSession,
    channel_id: int,
) -> list[Chunk]:
    """Get all open chunks for a channel."""
    stmt = (
        select(Chunk)
        .where(Chunk.channel_id == channel_id)
        .where(Chunk.chunk_state == "open")
    )
    result = await session.scalars(stmt)
    return list(result.all())


async def get_open_sliding_window(
    session: AsyncSession,
    channel_id: int,
) -> Chunk | None:
    """Get the open sliding window chunk for a channel (at most one)."""
    stmt = (
        select(Chunk)
        .where(Chunk.channel_id == channel_id)
        .where(Chunk.chunk_type == "sliding_window")
        .where(Chunk.chunk_state == "open")
    )
    return await session.scalar(stmt)


async def get_open_author_groups(
    session: AsyncSession,
    channel_id: int,
) -> dict[int, Chunk]:
    """Get all open author group chunks for a channel.

    Returns a dict mapping author_id to the open chunk.
    """
    stmt = (
        select(Chunk)
        .where(Chunk.channel_id == channel_id)
        .where(Chunk.chunk_type == "author_group")
        .where(Chunk.chunk_state == "open")
    )
    result = await session.scalars(stmt)
    chunks = result.all()

    # author_group chunks have exactly one author
    return {chunk.author_ids[0]: chunk for chunk in chunks}


async def upsert_chunk(
    session: AsyncSession,
    chunk: Chunk,
) -> Chunk:
    """Insert or update a chunk.

    Uses chunk_id if available. For new chunks, checks if a matching chunk
    exists by unique key and updates it, otherwise inserts.
    """
    values = {
        "chunk_type": chunk.chunk_type,
        "guild_id": chunk.guild_id,
        "channel_id": chunk.channel_id,
        "message_ids": chunk.message_ids,
        "author_ids": chunk.author_ids,
        "mentioned_user_ids": chunk.mentioned_user_ids,
        "mentioned_role_ids": chunk.mentioned_role_ids,
        "has_attachments": chunk.has_attachments,
        "chunk_state": chunk.chunk_state,
        "start_message_id": chunk.start_message_id,
        "leaf_message_id": chunk.leaf_message_id,
        "cross_channel_ref": chunk.cross_channel_ref,
        "embedding_status": chunk.embedding_status,
        "first_message_at": chunk.first_message_at,
        "last_message_at": chunk.last_message_at,
        "updated_at": utcnow(),
    }

    if chunk.chunk_id:
        # Update existing chunk by ID
        stmt = (
            update(Chunk)
            .where(Chunk.chunk_id == chunk.chunk_id)
            .values(**values)
            .returning(Chunk)
        )
        result = await session.execute(stmt)
        return result.scalar_one()

    # For new chunks, check if matching chunk exists
    if chunk.chunk_type == "reply_chain":
        # reply_chain: unique on (chunk_type, leaf_message_id)
        existing_stmt = (
            select(Chunk.chunk_id)
            .where(Chunk.chunk_type == "reply_chain")
            .where(Chunk.leaf_message_id == chunk.leaf_message_id)
        )
    else:
        # sliding_window/author_group: unique on (chunk_type, channel_id, start_message_id)
        existing_stmt = (
            select(Chunk.chunk_id)
            .where(Chunk.chunk_type == chunk.chunk_type)
            .where(Chunk.channel_id == chunk.channel_id)
            .where(Chunk.start_message_id == chunk.start_message_id)
        )

    existing_id = await session.scalar(existing_stmt)

    if existing_id:
        # Update existing chunk
        stmt = (
            update(Chunk)
            .where(Chunk.chunk_id == existing_id)
            .values(**values)
            .returning(Chunk)
        )
        result = await session.execute(stmt)
        return result.scalar_one()
    else:
        # Insert new chunk
        values["created_at"] = utcnow()
        stmt = pg_insert(Chunk).values(**values).returning(Chunk)
        result = await session.execute(stmt)
        return result.scalar_one()


async def insert_chunk_on_conflict_do_nothing(
    session: AsyncSession,
    chunk: Chunk,
) -> int | None:
    """Insert a chunk, doing nothing on conflict.

    Used for reply_chain chunks which are immutable.
    Returns the chunk_id if inserted, None if already existed.
    """
    now = utcnow()
    values = {
        "chunk_type": chunk.chunk_type,
        "guild_id": chunk.guild_id,
        "channel_id": chunk.channel_id,
        "message_ids": chunk.message_ids,
        "author_ids": chunk.author_ids,
        "mentioned_user_ids": chunk.mentioned_user_ids,
        "mentioned_role_ids": chunk.mentioned_role_ids,
        "has_attachments": chunk.has_attachments,
        "chunk_state": chunk.chunk_state,
        "start_message_id": chunk.start_message_id,
        "leaf_message_id": chunk.leaf_message_id,
        "cross_channel_ref": chunk.cross_channel_ref,
        "embedding_status": chunk.embedding_status,
        "first_message_at": chunk.first_message_at,
        "last_message_at": chunk.last_message_at,
        "created_at": now,
        "updated_at": now,
    }

    stmt = (
        pg_insert(Chunk)
        .values(**values)
        .on_conflict_do_nothing(
            index_elements=["chunk_type", "leaf_message_id"],
            index_where=text("chunk_type = 'reply_chain'"),
        )
        .returning(Chunk.chunk_id)
    )
    result = await session.execute(stmt)
    row = result.first()
    return row[0] if row else None


async def close_chunk(
    session: AsyncSession,
    chunk_id: int,
) -> None:
    """Close a chunk (mark as immutable)."""
    stmt = (
        update(Chunk)
        .where(Chunk.chunk_id == chunk_id)
        .values(
            chunk_state="closed",
            updated_at=utcnow(),
        )
    )
    await session.execute(stmt)


async def update_chunk_messages(
    session: AsyncSession,
    chunk_id: int,
    message_ids: list[int],
    author_ids: list[int],
    last_message_at: datetime | None = None,
) -> None:
    """Update a chunk's message_ids and author_ids.

    Also sets embedding_status to 'pending' for re-embedding.
    """
    values: dict = {
        "message_ids": message_ids,
        "author_ids": author_ids,
        "embedding_status": "pending",
        "updated_at": utcnow(),
    }
    if last_message_at is not None:
        values["last_message_at"] = last_message_at
    stmt = update(Chunk).where(Chunk.chunk_id == chunk_id).values(**values)
    await session.execute(stmt)


async def get_chunks_by_channel(
    session: AsyncSession,
    channel_id: int,
    chunk_type: str | None = None,
) -> list[Chunk]:
    """Get all chunks for a channel, optionally filtered by type."""
    stmt = select(Chunk).where(Chunk.channel_id == channel_id)
    if chunk_type:
        stmt = stmt.where(Chunk.chunk_type == chunk_type)
    result = await session.scalars(stmt)
    return list(result.all())


async def count_chunks_by_channel(
    session: AsyncSession,
    channel_id: int,
) -> dict[str, int]:
    """Count chunks by type for a channel."""
    from sqlalchemy import func

    stmt = (
        select(Chunk.chunk_type, func.count(Chunk.chunk_id))
        .where(Chunk.channel_id == channel_id)
        .group_by(Chunk.chunk_type)
    )
    result = await session.execute(stmt)
    return {row[0]: row[1] for row in result.all()}


async def bulk_insert_reply_chains(
    session: AsyncSession,
    chunks: list[Chunk],
) -> None:
    """Bulk insert reply_chain chunks, skipping existing ones.

    Much faster than individual inserts for large batches.
    First checks which leaf_message_ids already exist, then only inserts new ones.
    """
    if not chunks:
        return

    now = utcnow()

    # Batch lookup of existing leaf_message_ids to avoid duplicates
    # PostgreSQL IN clause limit is ~32767 elements, use 10000 for safety
    LOOKUP_BATCH_SIZE = 10000
    INSERT_BATCH_SIZE = 500  # 500 chunks * 12 params = 6000, under 32767 limit

    all_leaf_ids = [c.leaf_message_id for c in chunks]
    existing_leaf_ids: set[int] = set()

    for i in range(0, len(all_leaf_ids), LOOKUP_BATCH_SIZE):
        batch_ids = all_leaf_ids[i : i + LOOKUP_BATCH_SIZE]
        stmt = (
            select(Chunk.leaf_message_id)
            .where(Chunk.chunk_type == "reply_chain")
            .where(Chunk.leaf_message_id.in_(batch_ids))
        )
        result = await session.scalars(stmt)
        existing_leaf_ids.update(result.all())

    # Filter to only new chunks
    new_chunks = [c for c in chunks if c.leaf_message_id not in existing_leaf_ids]

    if not new_chunks:
        return

    # Insert new chunks in batches
    for i in range(0, len(new_chunks), INSERT_BATCH_SIZE):
        batch = new_chunks[i : i + INSERT_BATCH_SIZE]
        values_list = [
            {
                "chunk_type": c.chunk_type,
                "guild_id": c.guild_id,
                "channel_id": c.channel_id,
                "message_ids": c.message_ids,
                "author_ids": c.author_ids,
                "mentioned_user_ids": c.mentioned_user_ids,
                "mentioned_role_ids": c.mentioned_role_ids,
                "has_attachments": c.has_attachments,
                "chunk_state": c.chunk_state,
                "start_message_id": c.start_message_id,
                "leaf_message_id": c.leaf_message_id,
                "cross_channel_ref": c.cross_channel_ref,
                "embedding_status": c.embedding_status,
                "first_message_at": c.first_message_at,
                "last_message_at": c.last_message_at,
                "created_at": now,
                "updated_at": now,
            }
            for c in batch
        ]

        stmt = (
            pg_insert(Chunk)
            .values(values_list)
            .on_conflict_do_nothing(
                index_elements=["chunk_type", "leaf_message_id"],
                index_where=text("chunk_type = 'reply_chain'"),
            )
        )
        await session.execute(stmt)


async def bulk_upsert_chunks(
    session: AsyncSession,
    chunks: list[Chunk],
) -> dict[tuple, int]:
    """Bulk upsert sliding_window/author_group chunks using PostgreSQL UPSERT.

    Returns a dict mapping (chunk_type, channel_id, start_message_id) to chunk_id.
    Uses INSERT ... ON CONFLICT DO UPDATE for true bulk upsert (single query per batch).
    """
    if not chunks:
        return {}

    now = utcnow()
    result_map: dict[tuple, int] = {}

    # Separate by chunk type
    sw_chunks = [c for c in chunks if c.chunk_type == "sliding_window"]
    ag_chunks = [c for c in chunks if c.chunk_type == "author_group"]

    if sw_chunks:
        result_map.update(
            await _bulk_upsert_by_type_optimized(session, sw_chunks, "sliding_window", now)
        )

    if ag_chunks:
        result_map.update(
            await _bulk_upsert_by_type_optimized(session, ag_chunks, "author_group", now)
        )

    return result_map


async def _bulk_upsert_by_type_optimized(
    session: AsyncSession,
    chunks: list[Chunk],
    chunk_type: str,
    now: datetime,
) -> dict[tuple, int]:
    """Bulk upsert chunks using PostgreSQL's INSERT ... ON CONFLICT DO UPDATE.

    This is much faster than the old approach (lookup + separate updates).
    Uses a single batched UPSERT query instead of N individual UPDATEs.
    """
    if not chunks:
        return {}

    result_map: dict[tuple, int] = {}
    channel_id = chunks[0].channel_id

    # Batch size to avoid PostgreSQL parameter limit (32767)
    # Each chunk has ~14 fields, so 500 * 14 = 7000 params (safe)
    BATCH_SIZE = 500

    for i in range(0, len(chunks), BATCH_SIZE):
        batch = chunks[i : i + BATCH_SIZE]
        values_list = [
            {
                "chunk_type": c.chunk_type,
                "guild_id": c.guild_id,
                "channel_id": c.channel_id,
                "message_ids": c.message_ids,
                "author_ids": c.author_ids,
                "mentioned_user_ids": c.mentioned_user_ids,
                "mentioned_role_ids": c.mentioned_role_ids,
                "has_attachments": c.has_attachments,
                "chunk_state": c.chunk_state,
                "start_message_id": c.start_message_id,
                "leaf_message_id": c.leaf_message_id,
                "cross_channel_ref": c.cross_channel_ref,
                "embedding_status": c.embedding_status,
                "first_message_at": c.first_message_at,
                "last_message_at": c.last_message_at,
                "created_at": now,
                "updated_at": now,
            }
            for c in batch
        ]

        # Use PostgreSQL's ON CONFLICT DO UPDATE for true bulk upsert
        # This handles both inserts and updates in a single query
        # Note: The unique index is a partial index with a WHERE clause,
        # so we need to specify index_where to match the index condition
        stmt = (
            pg_insert(Chunk)
            .values(values_list)
            .on_conflict_do_update(
                # Conflict on the unique constraint (partial index)
                index_elements=["chunk_type", "channel_id", "start_message_id"],
                index_where=text(f"chunk_type = '{chunk_type}'"),
                # Update all mutable fields on conflict
                set_={
                    "message_ids": pg_insert(Chunk).excluded.message_ids,
                    "author_ids": pg_insert(Chunk).excluded.author_ids,
                    "mentioned_user_ids": pg_insert(Chunk).excluded.mentioned_user_ids,
                    "mentioned_role_ids": pg_insert(Chunk).excluded.mentioned_role_ids,
                    "has_attachments": pg_insert(Chunk).excluded.has_attachments,
                    "chunk_state": pg_insert(Chunk).excluded.chunk_state,
                    "embedding_status": pg_insert(Chunk).excluded.embedding_status,
                    "last_message_at": pg_insert(Chunk).excluded.last_message_at,
                    "updated_at": now,
                },
            )
            .returning(Chunk.chunk_id, Chunk.start_message_id)
        )

        result = await session.execute(stmt)
        for row in result.all():
            result_map[(chunk_type, channel_id, row.start_message_id)] = row.chunk_id

    return result_map
