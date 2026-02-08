"""Embedding processor for RAG.

Handles two-level batching: DB-level fetch batches and GPU-level
token-budget mini-batches. Coordinates between the embedding model,
LanceDB store, and PostgreSQL for status updates.

Uses adaptive batching: when a GPU batch causes an out-of-memory
error, the token budget is reduced so that all subsequent batches
stay within safe GPU memory limits.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np
import pyarrow as pa
import torch
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from discord_archive.db.models.chunk import Chunk
from discord_archive.db.models.chunk_text import ChunkText
from discord_archive.rag.embedding.lancedb_store import CHUNKS_SCHEMA, LanceDBStore
from discord_archive.rag.embedding.model import EmbeddingModel, EmbeddingModelConfig

if TYPE_CHECKING:
    from collections.abc import Callable

_log = logging.getLogger(__name__)

# Embedding status values (must match Chunk.embedding_status check constraint)
STATUS_PENDING = "pending"
STATUS_EMBEDDED = "embedded"


@dataclass
class _PendingChunkRow:
    """Lightweight row from the pending chunks query."""

    chunk_id: int
    text: str
    token_count: int
    guild_id: int
    channel_id: int
    author_ids: list[int]
    mentioned_user_ids: list[int]
    mentioned_role_ids: list[int]
    has_attachments: bool
    first_message_at: object  # datetime
    last_message_at: object  # datetime


@dataclass
class EmbeddingConfig:
    """Configuration for embedding."""

    model: EmbeddingModelConfig
    db_batch_size: int = 1000
    token_budget: int = 8_000
    max_batch_size: int = 32
    lancedb_data_dir: str = "data/lancedb"

    @classmethod
    def default(cls) -> EmbeddingConfig:
        return cls(model=EmbeddingModelConfig())


@dataclass
class EmbeddingStats:
    """Statistics from an embedding run."""

    chunks_processed: int = 0
    chunks_skipped: int = 0


class EmbeddingProcessor:
    """Embedding processor with two-level batching.

    Level 1: Fetch pending chunks from PostgreSQL in db_batch_size batches.
    Level 2: Pack chunks into GPU mini-batches using a token budget.

    Batching is adaptive: when an OOM occurs, the token budget is
    reduced based on the failed batch, keeping total_padded even
    across all token sizes.
    """

    def __init__(self, config: EmbeddingConfig) -> None:
        self.config = config

    async def process_channel(
        self,
        session: AsyncSession,
        channel_id: int,
        model: EmbeddingModel,
        lancedb_store: LanceDBStore,
        progress_callback: Callable[[int], None] | None = None,
    ) -> EmbeddingStats:
        """Process all pending chunks for a channel.

        Args:
            session: Database session.
            channel_id: Channel to process.
            model: Loaded embedding model.
            lancedb_store: Connected LanceDB store.
            progress_callback: Optional callback(chunks_embedded_so_far).

        Returns:
            EmbeddingStats with counts of processed/skipped chunks.
        """
        stats = EmbeddingStats()
        last_chunk_id = 0
        effective_budget = self.config.token_budget

        # LanceDB writes run in a background thread so the GPU
        # can encode the next batch in parallel.
        lance_pool = ThreadPoolExecutor(max_workers=1)
        prev_flush: Future | None = None
        prev_chunk_ids: list[int] = []

        # Tracked outside `try` so the KeyboardInterrupt handler
        # can flush any rows that were encoded but not yet written.
        all_vectors: list[np.ndarray] = []
        all_rows: list[_PendingChunkRow] = []

        try:
            while True:
                rows = await self._fetch_pending_chunks(
                    session, channel_id, last_chunk_id
                )
                if not rows:
                    break

                # Sort by token count for efficient GPU batching
                rows.sort(key=lambda r: r.token_count)

                # Encode all mini-batches (GPU work runs while the
                # previous LanceDB write completes in background).
                pending = list(rows)
                all_vectors = []
                all_rows = []
                batch_num = 0
                while pending:
                    batch, pending = self._take_batch(
                        pending, token_budget=effective_budget,
                    )
                    batch_num += 1
                    max_tok = max(r.token_count for r in batch)
                    total_padded = len(batch) * max_tok
                    _log.debug(
                        "batch %d: %d chunks, max_tok=%d, "
                        "total_padded=%d, budget=%d, "
                        "remaining=%d",
                        batch_num, len(batch), max_tok,
                        total_padded, effective_budget, len(pending),
                    )

                    try:
                        texts = [r.text for r in batch]
                        t0 = time.perf_counter()
                        vectors = model.encode_documents(texts)
                        t_encode = time.perf_counter() - t0
                    except torch.cuda.OutOfMemoryError:
                        torch.cuda.empty_cache()
                        if len(batch) == 1:
                            _log.error(
                                "OOM on single chunk %d (%d tokens) — skipping",
                                batch[0].chunk_id, batch[0].token_count,
                            )
                            stats.chunks_skipped += 1
                            continue
                        effective_budget = (len(batch) // 2) * max_tok
                        _log.warning(
                            "OOM on batch of %d chunks (max %d tokens), "
                            "reducing token_budget to %d",
                            len(batch), max_tok, effective_budget,
                        )
                        pending = batch + pending
                        continue

                    _log.debug("  -> encode=%.1fs", t_encode)

                    all_vectors.append(vectors)
                    all_rows.extend(batch)
                    stats.chunks_processed += len(batch)
                    if progress_callback:
                        progress_callback(stats.chunks_processed)

                # Finalize previous flush (should already be done
                # since encoding above took ~25s, overlapping the
                # ~28s LanceDB write).
                await self._finalize_flush(
                    prev_flush, session, prev_chunk_ids
                )
                prev_flush = None
                prev_chunk_ids = []

                # Start LanceDB write in background thread
                if all_rows:
                    table = self._build_arrow_table(
                        all_rows, np.vstack(all_vectors)
                    )
                    prev_chunk_ids = [r.chunk_id for r in all_rows]
                    prev_flush = lance_pool.submit(lancedb_store.add, table)
                    all_rows = []
                    all_vectors = []

                last_chunk_id = max(r.chunk_id for r in rows)

            # Finalize the last flush
            await self._finalize_flush(prev_flush, session, prev_chunk_ids)
        except KeyboardInterrupt:
            await self._flush_on_interrupt(
                session, lancedb_store,
                prev_flush, prev_chunk_ids,
                all_rows, all_vectors,
            )
            raise
        finally:
            lance_pool.shutdown(wait=True)

        return stats

    @staticmethod
    async def _flush_on_interrupt(
        session: AsyncSession,
        lancedb_store: LanceDBStore,
        prev_flush: Future | None,
        prev_chunk_ids: list[int],
        acc_rows: list[_PendingChunkRow],
        acc_vectors: list[np.ndarray],
    ) -> None:
        """Save all pending data on graceful shutdown.

        Handles two sources of unsaved work:
        1. In-flight LanceDB write (prev_flush) — wait for it and
           mark those chunks as embedded in PostgreSQL.
        2. Accumulated rows that were encoded but not yet written
           to LanceDB — write them synchronously and mark embedded.
        """
        saved = 0

        if prev_flush is not None:
            try:
                prev_flush.result(timeout=60)
                await EmbeddingProcessor._mark_embedded(session, prev_chunk_ids)
                await session.commit()
                saved += len(prev_chunk_ids)
            except Exception:
                _log.error("Failed to save in-flight chunks", exc_info=True)

        if acc_rows:
            try:
                table = EmbeddingProcessor._build_arrow_table(
                    acc_rows, np.vstack(acc_vectors)
                )
                lancedb_store.add(table)
                chunk_ids = [r.chunk_id for r in acc_rows]
                await EmbeddingProcessor._mark_embedded(session, chunk_ids)
                await session.commit()
                saved += len(chunk_ids)
            except Exception:
                _log.error(
                    "Failed to save accumulated chunks", exc_info=True
                )

        _log.warning("Interrupted — saved %d chunks before exit", saved)

    @staticmethod
    async def _finalize_flush(
        flush_future: Future | None,
        session: AsyncSession,
        chunk_ids: list[int],
    ) -> None:
        """Wait for a background LanceDB write and mark chunks embedded."""
        if flush_future is None:
            return
        t0 = time.perf_counter()
        flush_future.result()  # raises if the write failed
        t_lance = time.perf_counter() - t0
        await EmbeddingProcessor._mark_embedded(session, chunk_ids)
        await session.commit()
        _log.debug(
            "flush: %d records, lancedb=%.1fs",
            len(chunk_ids), t_lance,
        )

    async def _fetch_pending_chunks(
        self,
        session: AsyncSession,
        channel_id: int,
        last_chunk_id: int,
    ) -> list[_PendingChunkRow]:
        """Fetch pending chunks joined with their texts.

        Returns lightweight dataclass rows (not ORM objects).
        Skips chunks that have no text in chunk_texts.
        """
        stmt = (
            select(
                Chunk.chunk_id,
                ChunkText.text,
                ChunkText.token_count,
                Chunk.guild_id,
                Chunk.channel_id,
                Chunk.author_ids,
                Chunk.mentioned_user_ids,
                Chunk.mentioned_role_ids,
                Chunk.has_attachments,
                Chunk.first_message_at,
                Chunk.last_message_at,
            )
            .join(ChunkText, Chunk.chunk_id == ChunkText.chunk_id)
            .where(Chunk.channel_id == channel_id)
            .where(Chunk.embedding_status == STATUS_PENDING)
            .where(Chunk.chunk_id > last_chunk_id)
            .order_by(Chunk.chunk_id)
            .limit(self.config.db_batch_size)
        )
        result = await session.execute(stmt)
        return [
            _PendingChunkRow(
                chunk_id=row.chunk_id,
                text=row.text,
                token_count=row.token_count,
                guild_id=row.guild_id,
                channel_id=row.channel_id,
                author_ids=list(row.author_ids),
                mentioned_user_ids=list(row.mentioned_user_ids),
                mentioned_role_ids=list(row.mentioned_role_ids),
                has_attachments=row.has_attachments,
                first_message_at=row.first_message_at,
                last_message_at=row.last_message_at,
            )
            for row in result.all()
        ]

    def _take_batch(
        self,
        rows: list[_PendingChunkRow],
        max_batch_size: int | None = None,
        token_budget: int | None = None,
    ) -> tuple[list[_PendingChunkRow], list[_PendingChunkRow]]:
        """Extract the first batch from sorted rows.

        Constraints:
        - batch_size * max_token_in_batch <= token_budget
        - batch_size <= max_batch_size

        Returns (batch, remaining) tuple.
        """
        if max_batch_size is None:
            max_batch_size = self.config.max_batch_size
        if token_budget is None:
            token_budget = self.config.token_budget

        batch: list[_PendingChunkRow] = []
        max_tokens = 0

        for i, row in enumerate(rows):
            candidate_max = max(max_tokens, row.token_count)
            candidate_size = len(batch) + 1
            exceeds_budget = candidate_size * candidate_max > token_budget
            exceeds_batch = candidate_size > max_batch_size

            if batch and (exceeds_budget or exceeds_batch):
                return batch, rows[i:]

            batch.append(row)
            max_tokens = candidate_max

        return batch, []

    @staticmethod
    def _build_arrow_table(
        rows: list[_PendingChunkRow], vectors: np.ndarray
    ) -> pa.Table:
        """Build a PyArrow table from rows and their vectors.

        Constructs the table directly from numpy arrays to avoid
        the overhead of Python float conversion via tolist().

        Args:
            rows: Chunk rows with metadata.
            vectors: numpy array of shape (n, 4096), dtype float32.

        Returns:
            PyArrow table matching the LanceDB chunks schema.
        """
        flat = vectors.reshape(-1).astype(np.float32)
        vector_col = pa.FixedSizeListArray.from_arrays(
            pa.array(flat, type=pa.float32()), 4096
        )

        return pa.table(
            {
                "chunk_id": pa.array(
                    [r.chunk_id for r in rows], type=pa.int64()
                ),
                "vector": vector_col,
                "guild_id": pa.array(
                    [r.guild_id for r in rows], type=pa.int64()
                ),
                "channel_id": pa.array(
                    [r.channel_id for r in rows], type=pa.int64()
                ),
                "author_ids": pa.array(
                    [r.author_ids for r in rows],
                    type=pa.list_(pa.int64()),
                ),
                "mentioned_user_ids": pa.array(
                    [r.mentioned_user_ids for r in rows],
                    type=pa.list_(pa.int64()),
                ),
                "mentioned_role_ids": pa.array(
                    [r.mentioned_role_ids for r in rows],
                    type=pa.list_(pa.int64()),
                ),
                "has_attachments": pa.array(
                    [r.has_attachments for r in rows], type=pa.bool_()
                ),
                "first_message_at": pa.array(
                    [r.first_message_at for r in rows],
                    type=pa.timestamp("us", tz="UTC"),
                ),
                "last_message_at": pa.array(
                    [r.last_message_at for r in rows],
                    type=pa.timestamp("us", tz="UTC"),
                ),
            },
            schema=CHUNKS_SCHEMA,
        )

    @staticmethod
    async def _mark_embedded(
        session: AsyncSession, chunk_ids: list[int]
    ) -> None:
        """Bulk update embedding_status to 'embedded' for given chunk IDs."""
        stmt = (
            update(Chunk)
            .where(Chunk.chunk_id.in_(chunk_ids))
            .values(embedding_status=STATUS_EMBEDDED)
        )
        await session.execute(stmt)
