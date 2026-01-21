"""Repository layer for database operations.

Provides clean separation between data access and business logic.
All upsert operations are centralized here.
"""

from discord_archive.db.repositories.guild_repository import upsert_guild
from discord_archive.db.repositories.channel_repository import (
    bulk_upsert_channels,
    update_channel_parent,
    upsert_channel,
)
from discord_archive.db.repositories.message_repository import (
    bulk_upsert_users,
    bulk_insert_messages,
    bulk_insert_attachments,
    bulk_upsert_reactions,
    get_channel_message_count,
    persist_messages_batch,
)

__all__ = [
    "upsert_guild",
    "upsert_channel",
    "update_channel_parent",
    "bulk_upsert_channels",
    "bulk_upsert_users",
    "bulk_insert_messages",
    "bulk_insert_attachments",
    "bulk_upsert_reactions",
    "get_channel_message_count",
    "persist_messages_batch",
]
