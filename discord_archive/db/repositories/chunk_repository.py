"""Repository for chunk operations."""

import logging
from datetime import datetime

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
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
        "chunk_state": chunk.chunk_state,
        "start_message_id": chunk.start_message_id,
        "leaf_message_id": chunk.leaf_message_id,
        "cross_channel_ref": chunk.cross_channel_ref,
        "embedding_status": chunk.embedding_status,
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
        "chunk_state": chunk.chunk_state,
        "start_message_id": chunk.start_message_id,
        "leaf_message_id": chunk.leaf_message_id,
        "cross_channel_ref": chunk.cross_channel_ref,
        "embedding_status": chunk.embedding_status,
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
) -> None:
    """Update a chunk's message_ids and author_ids.

    Also sets embedding_status to 'pending' for re-embedding.
    """
    stmt = (
        update(Chunk)
        .where(Chunk.chunk_id == chunk_id)
        .values(
            message_ids=message_ids,
            author_ids=author_ids,
            embedding_status="pending",
            updated_at=utcnow(),
        )
    )
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
                "chunk_state": c.chunk_state,
                "start_message_id": c.start_message_id,
                "leaf_message_id": c.leaf_message_id,
                "cross_channel_ref": c.cross_channel_ref,
                "embedding_status": c.embedding_status,
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
    """Bulk upsert sliding_window/author_group chunks.

    Returns a dict mapping (chunk_type, channel_id, start_message_id) to chunk_id.
    Uses batch lookup + batch insert for new chunks.
    """
    if not chunks:
        return {}

    now = utcnow()
    result_map: dict[tuple, int] = {}

    # Separate chunks with IDs (just need update) from new ones
    chunks_with_id = [c for c in chunks if c.chunk_id is not None]
    chunks_without_id = [c for c in chunks if c.chunk_id is None]

    # Update chunks with known IDs (still individual but fewer of them)
    for chunk in chunks_with_id:
        result_map[(chunk.chunk_type, chunk.channel_id, chunk.start_message_id)] = chunk.chunk_id
        stmt = (
            update(Chunk)
            .where(Chunk.chunk_id == chunk.chunk_id)
            .values(
                message_ids=chunk.message_ids,
                author_ids=chunk.author_ids,
                chunk_state=chunk.chunk_state,
                embedding_status=chunk.embedding_status,
                updated_at=now,
            )
        )
        await session.execute(stmt)

    if not chunks_without_id:
        return result_map

    # For chunks without IDs, batch lookup and then batch insert/update
    sw_chunks = [c for c in chunks_without_id if c.chunk_type == "sliding_window"]
    ag_chunks = [c for c in chunks_without_id if c.chunk_type == "author_group"]

    if sw_chunks:
        result_map.update(
            await _bulk_upsert_by_type(session, sw_chunks, "sliding_window", now)
        )

    if ag_chunks:
        result_map.update(
            await _bulk_upsert_by_type(session, ag_chunks, "author_group", now)
        )

    return result_map


async def _bulk_upsert_by_type(
    session: AsyncSession,
    chunks: list[Chunk],
    chunk_type: str,
    now: datetime,
) -> dict[tuple, int]:
    """Bulk upsert chunks of a specific type.

    Uses batch lookup + batch insert for new chunks.
    Updates are done individually (ARRAY types don't work well with batch UPDATE).
    """
    result_map: dict[tuple, int] = {}
    channel_id = chunks[0].channel_id

    assert all(
        c.channel_id == channel_id for c in chunks
    ), "_bulk_upsert_by_type requires all chunks to be from the same channel"

    # Batch lookup existing chunks (1 query)
    start_ids = [c.start_message_id for c in chunks]
    stmt = (
        select(Chunk.chunk_id, Chunk.start_message_id)
        .where(Chunk.chunk_type == chunk_type)
        .where(Chunk.channel_id == channel_id)
        .where(Chunk.start_message_id.in_(start_ids))
    )
    result = await session.execute(stmt)
    existing = {row.start_message_id: row.chunk_id for row in result.all()}

    # Separate into updates and inserts
    to_update = []
    to_insert = []

    for chunk in chunks:
        if chunk.start_message_id in existing:
            chunk.chunk_id = existing[chunk.start_message_id]
            to_update.append(chunk)
        else:
            to_insert.append(chunk)

    # Updates - individual queries (typically few per batch)
    for chunk in to_update:
        stmt = (
            update(Chunk)
            .where(Chunk.chunk_id == chunk.chunk_id)
            .values(
                message_ids=chunk.message_ids,
                author_ids=chunk.author_ids,
                chunk_state=chunk.chunk_state,
                embedding_status=chunk.embedding_status,
                updated_at=now,
            )
        )
        await session.execute(stmt)
        result_map[(chunk_type, chunk.channel_id, chunk.start_message_id)] = chunk.chunk_id

    # Batch INSERT - batched to avoid PostgreSQL parameter limit
    if to_insert:
        BATCH_SIZE = 500  # 500 chunks * 12 params = 6000, well under 32767 limit

        for i in range(0, len(to_insert), BATCH_SIZE):
            batch = to_insert[i : i + BATCH_SIZE]
            values_list = [
                {
                    "chunk_type": c.chunk_type,
                    "guild_id": c.guild_id,
                    "channel_id": c.channel_id,
                    "message_ids": c.message_ids,
                    "author_ids": c.author_ids,
                    "chunk_state": c.chunk_state,
                    "start_message_id": c.start_message_id,
                    "leaf_message_id": c.leaf_message_id,
                    "cross_channel_ref": c.cross_channel_ref,
                    "embedding_status": c.embedding_status,
                    "created_at": now,
                    "updated_at": now,
                }
                for c in batch
            ]

            try:
                stmt = (
                    pg_insert(Chunk)
                    .values(values_list)
                    .returning(Chunk.chunk_id, Chunk.start_message_id)
                )
                result = await session.execute(stmt)

                for row in result.all():
                    result_map[(chunk_type, channel_id, row.start_message_id)] = row.chunk_id
            except IntegrityError:
                # Race condition: another process inserted some chunks between
                # our SELECT and INSERT. Roll back and handle individually.
                await session.rollback()
                logger.debug(
                    "Race condition in bulk insert for %s, falling back to individual upserts",
                    chunk_type,
                )

                for chunk in batch:
                    # Re-check if chunk exists now
                    existing_stmt = (
                        select(Chunk.chunk_id)
                        .where(Chunk.chunk_type == chunk_type)
                        .where(Chunk.channel_id == channel_id)
                        .where(Chunk.start_message_id == chunk.start_message_id)
                    )
                    existing_id = await session.scalar(existing_stmt)

                    if existing_id:
                        # Update existing
                        upd_stmt = (
                            update(Chunk)
                            .where(Chunk.chunk_id == existing_id)
                            .values(
                                message_ids=chunk.message_ids,
                                author_ids=chunk.author_ids,
                                chunk_state=chunk.chunk_state,
                                embedding_status=chunk.embedding_status,
                                updated_at=now,
                            )
                        )
                        await session.execute(upd_stmt)
                        result_map[(chunk_type, channel_id, chunk.start_message_id)] = existing_id
                    else:
                        # Insert new (should succeed now)
                        ins_stmt = (
                            pg_insert(Chunk)
                            .values(
                                chunk_type=chunk.chunk_type,
                                guild_id=chunk.guild_id,
                                channel_id=chunk.channel_id,
                                message_ids=chunk.message_ids,
                                author_ids=chunk.author_ids,
                                chunk_state=chunk.chunk_state,
                                start_message_id=chunk.start_message_id,
                                leaf_message_id=chunk.leaf_message_id,
                                cross_channel_ref=chunk.cross_channel_ref,
                                embedding_status=chunk.embedding_status,
                                created_at=now,
                                updated_at=now,
                            )
                            .returning(Chunk.chunk_id)
                        )
                        result = await session.execute(ins_stmt)
                        chunk_id = result.scalar_one()
                        result_map[(chunk_type, channel_id, chunk.start_message_id)] = chunk_id

    return result_map
