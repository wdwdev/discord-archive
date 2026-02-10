"""Chunking orchestrator.

Coordinates the chunking of messages into semantic chunks for RAG.
"""

from __future__ import annotations

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
from sqlalchemy import distinct, func, select, text

from discord_archive.core import BaseOrchestrator
from discord_archive.db.models.channel import Channel
from discord_archive.db.models.message import Message
from discord_archive.rag.chunking.logger import logger
from discord_archive.rag.chunking.processor import ChunkingConfig, ChunkingProcessor


class SpeedColumn(ProgressColumn):
    """Custom column to display processing speed, handling None values."""

    def render(self, task: Task) -> Text:
        """Render the speed with proper None handling."""
        # Always use current speed (it persists after completion)
        speed = task.speed

        # If no speed calculated yet, show placeholder
        if speed is None:
            return Text("     -- msg/s", style="dim cyan")

        # Show speed with proper formatting
        return Text(f"{speed:>6.0f} msg/s", style="cyan")

# Partial unique indexes required by ON CONFLICT clauses.
# create_all skips indexes when the table already exists,
# so we ensure them explicitly with IF NOT EXISTS.
_CHUNK_INDEXES = [
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_chunks_reply_chain
    ON chunks (chunk_type, leaf_message_id)
    WHERE chunk_type = 'reply_chain'
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_chunks_author_group
    ON chunks (chunk_type, channel_id, start_message_id)
    WHERE chunk_type = 'author_group'
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_chunks_sliding_window
    ON chunks (chunk_type, channel_id, start_message_id)
    WHERE chunk_type = 'sliding_window'
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_chunks_open_by_channel
    ON chunks (channel_id, created_at)
    WHERE chunk_state = 'open'
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_chunks_author_group_open
    ON chunks (channel_id)
    WHERE chunk_type = 'author_group' AND chunk_state = 'open'
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_chunks_pending
    ON chunks (channel_id)
    WHERE embedding_status = 'pending'
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_chunks_updated
    ON chunks (updated_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_chunks_channel_type
    ON chunks (channel_id, chunk_type)
    """,
]


class ChunkingOrchestrator(BaseOrchestrator):
    """Orchestrator for chunking.

    Processes messages from the database and creates chunks for RAG retrieval.
    """

    def __init__(
        self,
        database_url: str,
        config: ChunkingConfig | None = None,
    ) -> None:
        """Initialize the chunking orchestrator.

        Args:
            database_url: Database connection URL.
            config: Chunking configuration. Uses defaults if not provided.
        """
        super().__init__(database_url)
        self.config = config or ChunkingConfig.default()
        self.processor = ChunkingProcessor(self.config)

    async def init_db(self) -> None:
        """Create tables and ensure required indexes exist."""
        await super().init_db()
        async with self.engine.begin() as conn:
            for ddl in _CHUNK_INDEXES:
                await conn.execute(text(ddl))

        # Statistics
        self.guilds_processed = 0
        self.channels_processed = 0
        self.messages_processed = 0
        self.chunks_created = 0
        self.chunks_closed = 0

    async def _run(
        self,
        guild_id: int | None = None,
        channel_id: int | None = None,
    ) -> None:
        """Execute chunking.

        Args:
            guild_id: If provided, only process this guild.
            channel_id: If provided, only process this channel.
        """
        async with self.async_session() as session:
            if channel_id is not None:
                # Process single channel - only fetch needed columns
                stmt = select(Channel.guild_id, Channel.name).where(
                    Channel.channel_id == channel_id
                )
                result = await session.execute(stmt)
                row = result.first()
                if row:
                    await self._process_channel(
                        session, row.guild_id, channel_id, row.name or f"Channel {channel_id}"
                    )
            elif guild_id is not None:
                # Process all channels in guild
                await self._process_guild(session, guild_id)
            else:
                # Process all guilds with messages
                guilds = await self._get_guilds_with_messages(session)
                for gid in guilds:
                    await self._process_guild(session, gid)

    async def _get_guilds_with_messages(self, session) -> list[int]:
        """Get all guild IDs that have messages."""
        stmt = select(distinct(Message.guild_id)).where(Message.guild_id.isnot(None))
        result = await session.scalars(stmt)
        return list(result.all())

    async def _process_guild(self, session, guild_id: int) -> None:
        """Process all channels in a guild."""
        # Get guild name for logging - only fetch needed column
        from discord_archive.db.models.guild import Guild

        stmt = select(Guild.name).where(Guild.guild_id == guild_id)
        result = await session.execute(stmt)
        row = result.first()
        guild_name = row.name if row else f"Guild {guild_id}"

        logger.guild_start(guild_id, guild_name)
        self.guilds_processed += 1

        # Get all text channels with messages
        channels = await self._get_channels_with_messages(session, guild_id)

        for channel_id, channel_name in channels:
            await self._process_channel(session, guild_id, channel_id, channel_name)

    async def _get_channels_with_messages(
        self, session, guild_id: int
    ) -> list[tuple[int, str]]:
        """Get all channel IDs and names that have messages in a guild."""
        stmt = (
            select(Channel.channel_id, Channel.name)
            .where(Channel.guild_id == guild_id)
            .where(
                Channel.channel_id.in_(
                    select(distinct(Message.channel_id)).where(
                        Message.guild_id == guild_id
                    )
                )
            )
        )
        result = await session.execute(stmt)
        return [(row[0], row[1] or f"Channel {row[0]}") for row in result.all()]

    async def _process_channel(
        self,
        session,
        guild_id: int,
        channel_id: int,
        channel_name: str,
    ) -> None:
        """Process a single channel."""
        from discord_archive.db.repositories.chunking_checkpoint_repository import (
            get_chunking_checkpoint,
        )

        # Get checkpoint for resuming
        checkpoint = await get_chunking_checkpoint(session, channel_id)
        last_message_id = checkpoint.last_message_id if checkpoint else 0

        # Count remaining messages to process
        remaining_messages = await self._count_channel_messages(
            session, channel_id, after_message_id=last_message_id
        )

        # Always show channel start info
        logger.channel_start(channel_name, channel_id, last_message_id)

        if remaining_messages == 0:
            logger.channel_empty(channel_name)
            return

        # Check if verbose mode (logger level is DEBUG)
        is_verbose = logger._logger.getEffectiveLevel() <= 10  # DEBUG = 10

        # Only show progress bar in non-verbose mode to avoid conflicts with debug logs
        if not is_verbose:
            progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TextColumn("•"),
                TextColumn("{task.completed}/{task.total} msgs"),
                TextColumn("•"),
                SpeedColumn(),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                console=logger.console,
            )

            with progress:
                task_id = progress.add_task(
                    f"  {channel_name}",
                    total=remaining_messages,
                )

                # Progress callback to update the bar
                def on_progress(messages: int, _created: int, _closed: int) -> None:
                    progress.update(task_id, completed=messages)

                stats = await self.processor.process_channel(
                    session, guild_id, channel_id, progress_callback=on_progress
                )
        else:
            # Verbose mode: no progress updates, just run and log at DEBUG level
            stats = await self.processor.process_channel(
                session, guild_id, channel_id, progress_callback=None
            )

        if stats.messages_processed == 0:
            logger.channel_empty(channel_name)
            return

        # Update totals
        self.channels_processed += 1
        self.messages_processed += stats.messages_processed

        sw_created = stats.sliding_window_created
        sw_closed = stats.sliding_window_closed
        ag_created = stats.author_group_created
        ag_closed = stats.author_group_closed
        rc_created = stats.reply_chain_created

        self.chunks_created += sw_created + ag_created + rc_created
        self.chunks_closed += sw_closed + ag_closed

        logger.channel_complete(
            channel_name,
            stats.messages_processed,
            (sw_created, sw_closed),
            (ag_created, ag_closed),
            rc_created,
        )

    async def _count_channel_messages(
        self,
        session,
        channel_id: int,
        after_message_id: int = 0,
    ) -> int:
        """Count messages in a channel, optionally after a message ID.

        Args:
            session: Database session
            channel_id: Channel ID
            after_message_id: Only count messages after this ID (for resume)

        Returns:
            Number of messages
        """
        stmt = select(func.count(Message.message_id)).where(
            Message.channel_id == channel_id
        )
        if after_message_id > 0:
            stmt = stmt.where(Message.message_id > after_message_id)

        result = await session.scalar(stmt)
        return result or 0

    def _log_summary(self, elapsed: float) -> None:
        """Log the final summary."""
        logger.summary(
            elapsed=elapsed,
            guilds=self.guilds_processed,
            channels=self.channels_processed,
            messages=self.messages_processed,
            chunks_created=self.chunks_created,
            chunks_closed=self.chunks_closed,
        )


async def run_chunking(
    database_url: str,
    guild_id: int | None = None,
    channel_id: int | None = None,
    config: ChunkingConfig | None = None,
) -> None:
    """Run chunking.

    Args:
        database_url: Database connection URL.
        guild_id: If provided, only process this guild.
        channel_id: If provided, only process this channel.
        config: Chunking configuration.
    """
    orchestrator = ChunkingOrchestrator(database_url, config)
    await orchestrator.run(guild_id=guild_id, channel_id=channel_id)
