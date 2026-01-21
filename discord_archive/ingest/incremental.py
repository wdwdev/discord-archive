"""Incremental sync logic for downloading new messages.

Incremental fetches messages from oldest-known to newest using the `after` parameter.
It continues until Discord returns an empty response (caught up to present).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import AsyncSession

from discord_archive.db.repositories import persist_messages_batch
from discord_archive.ingest.logger import logger
from discord_archive.ingest.state import IngestStateManager
from discord_archive.utils.snowflake import snowflake_to_datetime

if TYPE_CHECKING:
    from discord_archive.ingest.client import DiscordClient


@dataclass
class IncrementalResult:
    """Result of an incremental sync operation."""

    messages_count: int
    is_caught_up: bool


async def incremental_channel(
    client: "DiscordClient",
    session: AsyncSession,
    channel_id: int,
    guild_id: int,
    batch_size: int = 100,
) -> IncrementalResult:
    """Incrementally sync new messages for a channel.

    Fetches messages from last-known newest to present using `after` parameter.
    Updates checkpoint after each batch.

    Args:
        client: Discord API client
        session: Database session
        channel_id: Channel to sync
        guild_id: Parent guild ID
        batch_size: Messages per API call (max 100)

    Returns:
        IncrementalResult with total messages and caught-up status
    """
    state = IngestStateManager(session)
    checkpoint = await state.get_checkpoint(channel_id)

    # If no checkpoint, we need to backfill first
    if not checkpoint or checkpoint.newest_message_id is None:
        logger.warning(f"No checkpoint for channel {channel_id}, backfill first")
        return IncrementalResult(messages_count=0, is_caught_up=False)

    after_id = checkpoint.newest_message_id
    total_messages = 0

    while True:
        # Fetch batch
        messages_data = await client.get_messages(
            channel_id=channel_id,
            limit=batch_size,
            after=after_id,
        )

        # Empty response = caught up
        if not messages_data:
            break

        # Process batch
        batch_count = await persist_messages_batch(
            session=session,
            messages_data=messages_data,
            guild_id=guild_id,
        )
        total_messages += batch_count

        # Update checkpoint with newest message in batch
        # Discord returns messages oldest-first when using `after`?
        # Actually, Discord always returns newest-first, so we need max
        newest_in_batch = max(int(m["id"]) for m in messages_data)

        await state.update_newest(
            channel_id=channel_id,
            message_id=newest_in_batch,
            guild_id=guild_id,
        )

        # Commit this batch
        await session.commit()

        # Log progress
        newest_date = snowflake_to_datetime(newest_in_batch).strftime("%Y-%m-%d")
        logger.batch_progress(total_messages, None, newest_date=newest_date)

        # Prepare for next iteration
        after_id = newest_in_batch

        # If we got fewer than requested, we've caught up
        if len(messages_data) < batch_size:
            break

    return IncrementalResult(messages_count=total_messages, is_caught_up=True)
