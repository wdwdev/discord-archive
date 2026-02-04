"""Author group chunker.

Creates chunks of consecutive messages from the same author.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone

from discord_archive.db.models.chunk import Chunk
from discord_archive.db.models.message import Message
from discord_archive.rag.chunking.constants import THREAD_STARTER_MESSAGE_TYPE
from discord_archive.rag.chunking.tokenizer import estimate_tokens


@dataclass
class AuthorGroupConfig:
    """Configuration for author group chunking."""

    gap_seconds: int = 60
    max_tokens: int = 1000


@dataclass
class AuthorGroupState:
    """Mutable state for author group chunking.

    Tracks open chunks per author.
    """

    # Map of author_id -> (chunk, messages, total_tokens, last_message_time)
    open_chunks: dict[int, tuple[Chunk, list[Message], int, datetime]] = field(
        default_factory=dict
    )

    def get_author_chunk(
        self, author_id: int
    ) -> tuple[Chunk, list[Message], int, datetime] | None:
        """Get the open chunk for an author."""
        return self.open_chunks.get(author_id)

    def set_author_chunk(
        self,
        author_id: int,
        chunk: Chunk,
        messages: list[Message],
        total_tokens: int,
        last_time: datetime,
    ) -> None:
        """Set the open chunk for an author."""
        self.open_chunks[author_id] = (chunk, messages, total_tokens, last_time)

    def remove_author_chunk(self, author_id: int) -> None:
        """Remove the open chunk for an author."""
        self.open_chunks.pop(author_id, None)


class AuthorGroupChunker:
    """Author group chunker.

    Creates chunks of consecutive messages from the same author,
    closing when there's a time gap or token limit is reached.
    """

    def __init__(self, config: AuthorGroupConfig | None = None):
        self.config = config or AuthorGroupConfig()

    def load_state(
        self, chunks: dict[int, Chunk], messages_by_author: dict[int, list[Message]]
    ) -> AuthorGroupState:
        """Load state from existing open chunks."""
        state = AuthorGroupState()

        for author_id, chunk in chunks.items():
            messages = messages_by_author.get(author_id, [])
            total_tokens = sum(estimate_tokens(m.content or "") for m in messages)
            last_time = (
                messages[-1].created_at
                if messages
                else datetime.min.replace(tzinfo=timezone.utc)
            )

            state.set_author_chunk(author_id, chunk, messages, total_tokens, last_time)

        return state

    def create_empty_state(self) -> AuthorGroupState:
        """Create an empty state."""
        return AuthorGroupState()

    def process_message(
        self,
        state: AuthorGroupState,
        message: Message,
        guild_id: int,
        channel_id: int,
    ) -> tuple[AuthorGroupState, list[Chunk]]:
        """Process a single message.

        Returns:
            Updated state and list of chunks to persist.
        """
        chunks_to_persist: list[Chunk] = []

        # Skip thread starters
        if message.type == THREAD_STARTER_MESSAGE_TYPE:
            return state, chunks_to_persist

        author_id = message.author_id
        message_tokens = estimate_tokens(message.content or "")
        message_time = message.created_at

        existing = state.get_author_chunk(author_id)

        if existing is None:
            # Start a new chunk for this author
            new_chunk = self._create_new_chunk(message, guild_id, channel_id)
            state.set_author_chunk(
                author_id, new_chunk, [message], message_tokens, message_time
            )
            chunks_to_persist.append(new_chunk)
        else:
            chunk, messages, total_tokens, last_time = existing

            # Check closing conditions
            time_gap = (message_time - last_time).total_seconds()
            would_exceed_tokens = (total_tokens + message_tokens) > self.config.max_tokens

            if time_gap > self.config.gap_seconds or would_exceed_tokens:
                # Close current chunk
                chunk.chunk_state = "closed"
                chunks_to_persist.append(chunk)

                # Start a new chunk
                new_chunk = self._create_new_chunk(message, guild_id, channel_id)
                state.set_author_chunk(
                    author_id, new_chunk, [message], message_tokens, message_time
                )
                chunks_to_persist.append(new_chunk)
            else:
                # Append to current chunk
                messages.append(message)
                total_tokens += message_tokens
                chunk.message_ids = [m.message_id for m in messages]
                # author_ids stays the same (single author)
                state.set_author_chunk(
                    author_id, chunk, messages, total_tokens, message_time
                )
                chunks_to_persist.append(chunk)

        return state, chunks_to_persist

    def _create_new_chunk(
        self,
        message: Message,
        guild_id: int,
        channel_id: int,
    ) -> Chunk:
        """Create a new author group chunk."""
        return Chunk(
            chunk_type="author_group",
            guild_id=guild_id,
            channel_id=channel_id,
            message_ids=[message.message_id],
            author_ids=[message.author_id],
            chunk_state="open",
            start_message_id=message.message_id,
            leaf_message_id=None,
            cross_channel_ref=None,
            embedding_status="pending",
        )
