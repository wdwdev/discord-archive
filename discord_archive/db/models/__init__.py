"""Discord Archive Database Models.

All models use SQLAlchemy 2.0 syntax with PostgreSQL dialect.
"""

from discord_archive.db.base import Base
from discord_archive.db.models.attachment import Attachment
from discord_archive.db.models.channel import Channel
from discord_archive.db.models.ingest_checkpoint import IngestCheckpoint
from discord_archive.db.models.emoji import Emoji
from discord_archive.db.models.guild import Guild
from discord_archive.db.models.guild_scheduled_event import GuildScheduledEvent
from discord_archive.db.models.message import Message
from discord_archive.db.models.reaction import Reaction
from discord_archive.db.models.role import Role
from discord_archive.db.models.sticker import Sticker
from discord_archive.db.models.user import User

__all__ = [
    "Base",
    "Attachment",
    "Channel",
    "IngestCheckpoint",
    "Emoji",
    "Guild",
    "GuildScheduledEvent",
    "Message",
    "Reaction",
    "Role",
    "Sticker",
    "User",
]
