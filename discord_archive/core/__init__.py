"""Base orchestrator for pipeline execution.

Provides common infrastructure for all pipeline orchestrators:
- Database engine and session management
- Timing and statistics tracking
- Common run() interface with guild/channel filtering

Usage:
    class MyOrchestrator(BaseOrchestrator):
        async def _run_pipeline(self, guild_id, channel_id):
            # Implementation
            pass

        def _log_summary(self, elapsed):
            # Log final statistics
            pass
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from discord_archive.db.engine import get_async_session, get_engine
from discord_archive.db.models import Base

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker


class BaseOrchestrator(ABC):
    """Abstract base class for pipeline orchestrators.

    Provides:
    - Database engine and session factory (cached)
    - Table initialization
    - Timing infrastructure
    - Common run() method signature

    Subclasses must implement:
    - _run_pipeline(): The actual pipeline logic
    - _log_summary(): Log final statistics
    """

    def __init__(self, database_url: str) -> None:
        """Initialize the orchestrator.

        Args:
            database_url: Database connection URL.
        """
        self.database_url = database_url
        self.engine: AsyncEngine = get_engine(database_url)
        self.async_session: async_sessionmaker[AsyncSession] = get_async_session(
            database_url
        )
        self.start_time: float = 0.0

    async def init_db(self) -> None:
        """Create tables if they don't exist.

        Override this method to add pipeline-specific indexes
        or other database initialization.
        """
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def run(
        self,
        guild_id: int | None = None,
        channel_id: int | None = None,
    ) -> None:
        """Run the pipeline.

        Args:
            guild_id: If provided, only process this guild.
            channel_id: If provided, only process this channel.
        """
        self.start_time = time.time()

        await self.init_db()
        await self._run_pipeline(guild_id=guild_id, channel_id=channel_id)

        elapsed = time.time() - self.start_time
        self._log_summary(elapsed)

    @abstractmethod
    async def _run_pipeline(
        self,
        guild_id: int | None = None,
        channel_id: int | None = None,
    ) -> None:
        """Execute the pipeline logic.

        Args:
            guild_id: If provided, only process this guild.
            channel_id: If provided, only process this channel.
        """
        ...

    @abstractmethod
    def _log_summary(self, elapsed: float) -> None:
        """Log the final summary statistics.

        Args:
            elapsed: Total time elapsed in seconds.
        """
        ...
