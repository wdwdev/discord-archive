"""Embedding orchestrator.

Coordinates the embedding of chunk texts into vectors using NV-Embed-v2
and stores them in LanceDB for vector search during retrieval.
"""

from __future__ import annotations

import time

from rich.progress import (
    BarColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    Task,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.text import Text
from sqlalchemy import func, select

from discord_archive.core import BaseOrchestrator
from discord_archive.db.models.chunk import Chunk
from discord_archive.db.models.chunk_text import ChunkText
from discord_archive.rag.embedding.lancedb_store import LanceDBStore
from discord_archive.rag.embedding.logger import logger
from discord_archive.rag.embedding.model import EmbeddingModel
from discord_archive.rag.embedding.processor import (
    STATUS_PENDING,
    EmbeddingConfig,
    EmbeddingProcessor,
)


class ChunkSpeedColumn(ProgressColumn):
    """Custom column to display chunk processing speed, handling None values."""

    def render(self, task: Task) -> Text:
        """Render the speed with proper None handling."""
        # Always use current speed (it persists after completion)
        speed = task.speed

        # If no speed calculated yet, show placeholder
        if speed is None:
            return Text("     -- chunks/s", style="dim cyan")

        # Show speed with proper formatting
        return Text(f"{speed:>6.0f} chunks/s", style="cyan")


class EmbeddingOrchestrator(BaseOrchestrator):
    """Orchestrator for embedding.

    Loads the embedding model, connects to LanceDB, and processes
    all pending chunks globally sorted by token count.
    """

    def __init__(
        self,
        database_url: str,
        config: EmbeddingConfig | None = None,
    ) -> None:
        super().__init__(database_url)
        self.config = config or EmbeddingConfig.default()
        self.processor = EmbeddingProcessor(self.config)
        self.chunks_embedded = 0

    async def _run(
        self,
        guild_id: int | None = None,
        channel_id: int | None = None,
    ) -> None:
        """Execute embedding.

        Loads the model, connects to LanceDB, then processes all
        pending chunks in one pass sorted by token count.
        Model is unloaded in a finally block.
        """
        async with self.async_session() as session:
            chunk_count, total_tokens, oversized = await self._get_pending_stats(
                session, guild_id=guild_id, channel_id=channel_id
            )

        if oversized:
            logger.warning(
                f"{oversized} chunks exceed max_length "
                f"({self.config.model.max_length}) — skipped"
            )
        if chunk_count == 0:
            logger.info("No pending chunks to embed")
            return

        logger.info(f"{chunk_count:,} chunks ({total_tokens:,} tokens) to embed")

        logger.model_loading()
        t0 = time.time()
        model = EmbeddingModel(self.config.model)
        model.load()
        logger.model_loaded(time.time() - t0)

        lancedb_store = LanceDBStore(self.config.lancedb_data_dir)
        lancedb_store.connect()

        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("•"),
            TextColumn("{task.completed}/{task.total} chunks"),
            TextColumn("•"),
            ChunkSpeedColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=logger.console,
        )

        try:
            async with self.async_session() as session:
                with progress:
                    task_id = progress.add_task("Embedding", total=chunk_count)

                    def on_progress(chunks_processed: int) -> None:
                        progress.update(task_id, completed=chunks_processed)

                    stats = await self.processor.process(
                        session, model, lancedb_store,
                        progress_callback=on_progress,
                        guild_id=guild_id, channel_id=channel_id,
                    )

            self.chunks_embedded = stats.chunks_processed
        finally:
            model.unload()

    async def _get_pending_stats(
        self,
        session,
        *,
        guild_id: int | None = None,
        channel_id: int | None = None,
    ) -> tuple[int, int, int]:
        """Get global stats for pending chunks ready for embedding.

        Returns:
            (chunk_count, total_tokens, oversized_count) where
            chunk_count and total_tokens only include embeddable chunks,
            and oversized_count is chunks exceeding max sequence length.
        """
        base = (
            select(func.count(Chunk.chunk_id))
            .join(ChunkText, Chunk.chunk_id == ChunkText.chunk_id)
            .where(Chunk.embedding_status == STATUS_PENDING)
        )
        if guild_id is not None:
            base = base.where(Chunk.guild_id == guild_id)
        if channel_id is not None:
            base = base.where(Chunk.channel_id == channel_id)

        total_chunks = await session.scalar(base) or 0

        size_filter = ChunkText.token_count <= self.config.model.max_length
        embeddable = await session.scalar(base.where(size_filter)) or 0

        total_tokens = await session.scalar(
            select(func.sum(ChunkText.token_count))
            .join(Chunk, Chunk.chunk_id == ChunkText.chunk_id)
            .where(Chunk.embedding_status == STATUS_PENDING)
            .where(size_filter)
            .where(
                (Chunk.guild_id == guild_id) if guild_id is not None else True
            )
            .where(
                (Chunk.channel_id == channel_id) if channel_id is not None else True
            )
        ) or 0

        return embeddable, total_tokens, total_chunks - embeddable

    def _log_summary(self, elapsed: float) -> None:
        """Log the final summary."""
        logger.summary(
            elapsed=elapsed,
            chunks_embedded=self.chunks_embedded,
        )


async def run_embedding(
    database_url: str,
    guild_id: int | None = None,
    channel_id: int | None = None,
    config: EmbeddingConfig | None = None,
) -> None:
    """Run embedding.

    Args:
        database_url: Database connection URL.
        guild_id: If provided, only process this guild.
        channel_id: If provided, only process this channel.
        config: Embedding configuration.
    """
    orchestrator = EmbeddingOrchestrator(database_url, config)
    await orchestrator.run(guild_id=guild_id, channel_id=channel_id)
