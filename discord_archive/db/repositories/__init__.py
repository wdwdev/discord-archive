"""Repository layer for database operations.

Provides clean separation between data access and business logic.
All upsert operations are centralized here.
"""

from discord_archive.db.repositories.channel_repository import (
    bulk_upsert_channels,
    update_channel_parent,
    upsert_channel,
)
from discord_archive.db.repositories.chunk_repository import (
    close_chunk,
    count_chunks_by_channel,
    get_chunks_by_channel,
    get_open_author_groups,
    get_open_chunks_by_channel,
    get_open_sliding_window,
    insert_chunk_on_conflict_do_nothing,
    update_chunk_messages,
    upsert_chunk,
)
from discord_archive.db.repositories.chunking_checkpoint_repository import (
    get_all_chunking_checkpoints,
    get_chunking_checkpoint,
    upsert_chunking_checkpoint,
)
from discord_archive.db.repositories.guild_repository import upsert_guild
from discord_archive.db.repositories.message_repository import (
    bulk_insert_attachments,
    bulk_insert_messages,
    bulk_upsert_reactions,
    bulk_upsert_users,
    get_channel_message_count,
    persist_messages_batch,
)

__all__ = [
    # Guild
    "upsert_guild",
    # Channel
    "upsert_channel",
    "update_channel_parent",
    "bulk_upsert_channels",
    # Message
    "bulk_upsert_users",
    "bulk_insert_messages",
    "bulk_insert_attachments",
    "bulk_upsert_reactions",
    "get_channel_message_count",
    "persist_messages_batch",
    # Chunk
    "get_open_chunks_by_channel",
    "get_open_sliding_window",
    "get_open_author_groups",
    "upsert_chunk",
    "insert_chunk_on_conflict_do_nothing",
    "close_chunk",
    "update_chunk_messages",
    "get_chunks_by_channel",
    "count_chunks_by_channel",
    # Chunking Checkpoint
    "get_chunking_checkpoint",
    "upsert_chunking_checkpoint",
    "get_all_chunking_checkpoints",
]
