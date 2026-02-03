"""Ingest checkpoint state management.

Provides CRUD operations for IngestCheckpoint records,
used to track backfill and incremental sync progress.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from discord_archive.db.models import IngestCheckpoint


class IngestStateManager:
    """Manages IngestCheckpoint records for sync state tracking."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_checkpoint(self, channel_id: int) -> IngestCheckpoint | None:
        """Get checkpoint for a channel, or None if not exists."""
        result = await self.session.execute(
            select(IngestCheckpoint).where(IngestCheckpoint.channel_id == channel_id)
        )
        return result.scalar_one_or_none()

    async def create_or_get_checkpoint(
        self, channel_id: int, guild_id: int
    ) -> IngestCheckpoint:
        """Get existing checkpoint or create a new one."""
        checkpoint = await self.get_checkpoint(channel_id)
        if checkpoint:
            return checkpoint

        checkpoint = IngestCheckpoint(
            channel_id=channel_id,
            guild_id=guild_id,
            backfill_complete=False,
        )
        self.session.add(checkpoint)
        await self.session.flush()
        return checkpoint

    async def update_oldest(
        self, channel_id: int, message_id: int, guild_id: int
    ) -> None:
        """Update oldest_message_id after backfill batch.

        Only updates if the new message_id is older (smaller) than current.
        """
        checkpoint = await self.create_or_get_checkpoint(channel_id, guild_id)

        if (
            checkpoint.oldest_message_id is None
            or message_id < checkpoint.oldest_message_id
        ):
            checkpoint.oldest_message_id = message_id

        # Also update newest if not set (first batch)
        if checkpoint.newest_message_id is None:
            checkpoint.newest_message_id = message_id

        checkpoint.last_synced_at = datetime.now(timezone.utc)
        await self.session.flush()

    async def update_newest(
        self, channel_id: int, message_id: int, guild_id: int
    ) -> None:
        """Update newest_message_id after incremental batch.

        Only updates if the new message_id is newer (larger) than current.
        """
        checkpoint = await self.create_or_get_checkpoint(channel_id, guild_id)

        if (
            checkpoint.newest_message_id is None
            or message_id > checkpoint.newest_message_id
        ):
            checkpoint.newest_message_id = message_id

        checkpoint.last_synced_at = datetime.now(timezone.utc)
        await self.session.flush()

    async def update_bounds(
        self,
        channel_id: int,
        guild_id: int,
        oldest_id: int | None,
        newest_id: int | None,
    ) -> None:
        """Update both bounds after a batch (used for initial fetch)."""
        checkpoint = await self.create_or_get_checkpoint(channel_id, guild_id)

        if oldest_id is not None:
            if (
                checkpoint.oldest_message_id is None
                or oldest_id < checkpoint.oldest_message_id
            ):
                checkpoint.oldest_message_id = oldest_id

        if newest_id is not None:
            if (
                checkpoint.newest_message_id is None
                or newest_id > checkpoint.newest_message_id
            ):
                checkpoint.newest_message_id = newest_id

        checkpoint.last_synced_at = datetime.now(timezone.utc)
        await self.session.flush()

    async def mark_backfill_complete(self, channel_id: int) -> None:
        """Mark channel backfill as complete."""
        checkpoint = await self.get_checkpoint(channel_id)
        if checkpoint:
            checkpoint.backfill_complete = True
            checkpoint.last_synced_at = datetime.now(timezone.utc)
            await self.session.flush()

    async def is_backfill_complete(self, channel_id: int) -> bool:
        """Check if channel backfill is complete."""
        checkpoint = await self.get_checkpoint(channel_id)
        return checkpoint.backfill_complete if checkpoint else False

    async def get_incomplete_backfills(self, guild_id: int) -> list[int]:
        """Get channel IDs with incomplete backfill in a guild."""
        result = await self.session.execute(
            select(IngestCheckpoint.channel_id).where(
                IngestCheckpoint.guild_id == guild_id,
                IngestCheckpoint.backfill_complete == False,  # noqa: E712
            )
        )
        return list(result.scalars().all())
