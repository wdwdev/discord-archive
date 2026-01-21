"""Backfill logic for downloading historical messages.

Backfill fetches messages from newest to oldest using the `before` parameter.
It continues until Discord returns an empty response (channel exhausted).
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
class BackfillResult:
    """Result of a backfill operation."""

    messages_count: int
    is_complete: bool


async def backfill_channel(
    client: "DiscordClient",
    session: AsyncSession,
    channel_id: int,
    guild_id: int,
    batch_size: int = 100,
) -> BackfillResult:
    """Backfill historical messages for a channel.

    Fetches messages from newest to oldest using `before` parameter.
    Updates checkpoint after each batch.

    Args:
        client: Discord API client
        session: Database session
        channel_id: Channel to backfill
        guild_id: Parent guild ID
        batch_size: Messages per API call (max 100)

    Returns:
        BackfillResult with total messages and completion status
    """
    state = IngestStateManager(session)
    checkpoint = await state.get_checkpoint(channel_id)

    # If already complete, skip
    if checkpoint and checkpoint.backfill_complete:
        return BackfillResult(messages_count=0, is_complete=True)

    # Determine starting point
    before_id: int | None = None
    if checkpoint and checkpoint.oldest_message_id:
        before_id = checkpoint.oldest_message_id

    total_messages = 0
    is_complete = False

    while True:
        # Fetch batch
        messages_data = await client.get_messages(
            channel_id=channel_id,
            limit=batch_size,
            before=before_id,
        )

        # Empty response = backfill complete
        if not messages_data:
            is_complete = True
            await state.mark_backfill_complete(channel_id)
            break

        # Process batch using repository
        batch_count = await persist_messages_batch(
            session=session,
            messages_data=messages_data,
            guild_id=guild_id,
        )
        total_messages += batch_count

        # Update checkpoint with oldest message in batch
        # Discord returns messages newest-first, so last item is oldest
        oldest_in_batch = min(int(m["id"]) for m in messages_data)
        newest_in_batch = max(int(m["id"]) for m in messages_data)

        # Always pass newest_id; update_bounds only advances when newer.
        await state.update_bounds(
            channel_id=channel_id,
            guild_id=guild_id,
            oldest_id=oldest_in_batch,
            newest_id=newest_in_batch if not checkpoint else None,
        )

        # Commit this batch
        await session.commit()

        # Log progress
        oldest_date = snowflake_to_datetime(oldest_in_batch).strftime("%Y-%m-%d")
        logger.batch_progress(total_messages, None, oldest_date=oldest_date)

        # Prepare for next iteration
        before_id = oldest_in_batch

        # If we got fewer than requested, we've reached the end
        if len(messages_data) < batch_size:
            is_complete = True
            await state.mark_backfill_complete(channel_id)
            await session.commit()
            break

    return BackfillResult(messages_count=total_messages, is_complete=is_complete)
