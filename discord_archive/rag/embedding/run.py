"""Embedding orchestrator.

Coordinates the embedding of chunk texts into vectors using NV-Embed-v2
and stores them in LanceDB for vector search during retrieval.
"""

from __future__ import annotations

import time

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from sqlalchemy import distinct, func, select

from discord_archive.core import BaseOrchestrator
from discord_archive.db.models.channel import Channel
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


class EmbeddingOrchestrator(BaseOrchestrator):
    """Orchestrator for embedding.

    Loads the embedding model, connects to LanceDB, and processes
    pending chunks across guilds/channels.
    """

    def __init__(
        self,
        database_url: str,
        config: EmbeddingConfig | None = None,
    ) -> None:
        super().__init__(database_url)
        self.config = config or EmbeddingConfig.default()
        self.processor = EmbeddingProcessor(self.config)

        # Statistics
        self.guilds_processed = 0
        self.channels_processed = 0
        self.chunks_embedded = 0

    async def _run(
        self,
        guild_id: int | None = None,
        channel_id: int | None = None,
    ) -> None:
        """Execute embedding.

        Loads the model, connects to LanceDB, then processes
        channels. Model is unloaded in a finally block.
        """
        logger.model_loading()
        t0 = time.time()
        model = EmbeddingModel(self.config.model)
        model.load()
        logger.model_loaded(time.time() - t0)

        lancedb_store = LanceDBStore(self.config.lancedb_data_dir)
        lancedb_store.connect()

        try:
            async with self.async_session() as session:
                if channel_id is not None:
                    stmt = select(Channel.guild_id, Channel.name).where(
                        Channel.channel_id == channel_id
                    )
                    result = await session.execute(stmt)
                    row = result.first()
                    if row:
                        await self._process_channel(
                            session,
                            channel_id,
                            row.name or f"Channel {channel_id}",
                            model,
                            lancedb_store,
                        )
                elif guild_id is not None:
                    await self._process_guild(
                        session, guild_id, model, lancedb_store
                    )
                else:
                    guilds = await self._get_guilds_with_pending(session)
                    for gid in guilds:
                        await self._process_guild(
                            session, gid, model, lancedb_store
                        )
        finally:
            model.unload()

    async def _get_guilds_with_pending(self, session) -> list[int]:
        """Get guild IDs that have pending chunks."""
        stmt = (
            select(distinct(Chunk.guild_id))
            .where(Chunk.embedding_status == STATUS_PENDING)
        )
        result = await session.scalars(stmt)
        return list(result.all())

    async def _process_guild(
        self, session, guild_id: int, model: EmbeddingModel, lancedb_store: LanceDBStore
    ) -> None:
        """Process all channels with pending chunks in a guild."""
        from discord_archive.db.models.guild import Guild

        stmt = select(Guild.name).where(Guild.guild_id == guild_id)
        result = await session.execute(stmt)
        row = result.first()
        guild_name = row.name if row else f"Guild {guild_id}"

        logger.guild_start(guild_id, guild_name)
        self.guilds_processed += 1

        channels = await self._get_channels_with_pending(session, guild_id)
        for ch_id, ch_name in channels:
            await self._process_channel(
                session, ch_id, ch_name, model, lancedb_store
            )

    async def _get_channels_with_pending(
        self, session, guild_id: int
    ) -> list[tuple[int, str]]:
        """Get channels with pending chunks in a guild."""
        stmt = (
            select(Channel.channel_id, Channel.name)
            .where(Channel.guild_id == guild_id)
            .where(
                Channel.channel_id.in_(
                    select(distinct(Chunk.channel_id))
                    .where(Chunk.guild_id == guild_id)
                    .where(Chunk.embedding_status == STATUS_PENDING)
                )
            )
        )
        result = await session.execute(stmt)
        return [(row[0], row[1] or f"Channel {row[0]}") for row in result.all()]

    async def _get_pending_count(
        self, session, channel_id: int
    ) -> int:
        """Get count of pending chunks that have text ready for embedding."""
        stmt = (
            select(func.count(Chunk.chunk_id))
            .join(ChunkText, Chunk.chunk_id == ChunkText.chunk_id)
            .where(Chunk.channel_id == channel_id)
            .where(Chunk.embedding_status == STATUS_PENDING)
        )
        result = await session.scalar(stmt)
        return result or 0

    async def _process_channel(
        self,
        session,
        channel_id: int,
        channel_name: str,
        model: EmbeddingModel,
        lancedb_store: LanceDBStore,
    ) -> None:
        """Process a single channel."""
        pending_count = await self._get_pending_count(session, channel_id)
        logger.channel_start(channel_name, channel_id, pending_count)

        if pending_count == 0:
            logger.channel_empty(channel_name)
            return

        progress = Progress(
            SpinnerColumn(),
            TextColumn("    [progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=logger.console,
            speed_estimate_period=300,
        )

        with progress:
            task_id = progress.add_task("Embedding", total=pending_count)

            def on_progress(chunks_embedded: int) -> None:
                progress.update(task_id, completed=chunks_embedded)

            stats = await self.processor.process_channel(
                session, channel_id, model, lancedb_store, progress_callback=on_progress
            )

        self.channels_processed += 1
        self.chunks_embedded += stats.chunks_processed

        logger.channel_complete(channel_name, stats.chunks_processed)

    def _log_summary(self, elapsed: float) -> None:
        """Log the final summary."""
        logger.summary(
            elapsed=elapsed,
            guilds=self.guilds_processed,
            channels=self.channels_processed,
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
