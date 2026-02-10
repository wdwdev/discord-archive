"""Author group chunker.

Creates chunks of consecutive messages from the same author.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from discord_archive.db.models.chunk import Chunk
from discord_archive.db.models.message import Message
from discord_archive.rag.chunking.constants import MAX_CHUNK_TOKENS, THREAD_STARTER_MESSAGE_TYPE
from discord_archive.rag.chunking.tokenizer import estimate_message_context_tokens

if TYPE_CHECKING:
    from discord_archive.db.models.attachment import Attachment


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

    # Map of author_id -> (chunk, messages, message_contexts, total_tokens, last_message_time)
    open_chunks: dict[
        int, tuple[Chunk, list[Message], dict[int, tuple[str | None, list["Attachment"]]], int, datetime]
    ] = field(default_factory=dict)

    def get_author_chunk(
        self, author_id: int
    ) -> tuple[Chunk, list[Message], dict[int, tuple[str | None, list["Attachment"]]], int, datetime] | None:
        """Get the open chunk for an author."""
        return self.open_chunks.get(author_id)

    def set_author_chunk(
        self,
        author_id: int,
        chunk: Chunk,
        messages: list[Message],
        message_contexts: dict[int, tuple[str | None, list["Attachment"]]],
        total_tokens: int,
        last_time: datetime,
    ) -> None:
        """Set the open chunk for an author."""
        self.open_chunks[author_id] = (
            chunk,
            messages,
            message_contexts,
            total_tokens,
            last_time,
        )

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
        self,
        chunks: dict[int, Chunk],
        messages_by_author: dict[int, list[Message]],
        usernames: dict[int, str],
        attachments_by_msg: dict[int, list["Attachment"]],
    ) -> AuthorGroupState:
        """Load state from existing open chunks.

        Args:
            chunks: author_id → Chunk mapping
            messages_by_author: author_id → messages mapping
            usernames: author_id → username mapping
            attachments_by_msg: message_id → attachments list mapping
        """
        state = AuthorGroupState()

        for author_id, chunk in chunks.items():
            messages = messages_by_author.get(author_id, [])
            message_contexts = {}
            total_tokens = 0

            username = usernames.get(author_id)
            for m in messages:
                attachments = attachments_by_msg.get(m.message_id, [])
                message_contexts[m.message_id] = (username, attachments)
                total_tokens += estimate_message_context_tokens(m, username, attachments)

            last_time = (
                messages[-1].created_at if messages else datetime.min.replace(tzinfo=timezone.utc)
            )

            state.set_author_chunk(
                author_id, chunk, messages, message_contexts, total_tokens, last_time
            )

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
        username: str | None = None,
        attachments: list["Attachment"] | None = None,
    ) -> tuple[AuthorGroupState, list[Chunk]]:
        """Process a single message.

        Args:
            state: Current chunker state
            message: Message to process
            guild_id: Guild ID
            channel_id: Channel ID
            username: Author username (for token estimation)
            attachments: List of attachments (for token estimation)

        Returns:
            Updated state and list of chunks to persist.
        """
        if attachments is None:
            attachments = []

        chunks_to_persist: list[Chunk] = []

        # Skip thread starters
        if message.type == THREAD_STARTER_MESSAGE_TYPE:
            return state, chunks_to_persist

        author_id = message.author_id
        message_tokens = estimate_message_context_tokens(message, username, attachments)
        message_time = message.created_at

        # Discard single messages that exceed the absolute maximum
        if message_tokens > MAX_CHUNK_TOKENS:
            import logging
            logging.warning(
                f"Discarding message {message.message_id} with {message_tokens} tokens "
                f"(exceeds MAX_CHUNK_TOKENS={MAX_CHUNK_TOKENS})"
            )
            return state, chunks_to_persist

        existing = state.get_author_chunk(author_id)

        if existing is None:
            # Start a new chunk for this author
            new_chunk = self._create_new_chunk(message, guild_id, channel_id)
            message_contexts = {message.message_id: (username, attachments)}
            state.set_author_chunk(
                author_id, new_chunk, [message], message_contexts, message_tokens, message_time
            )
            chunks_to_persist.append(new_chunk)
        else:
            chunk, messages, message_contexts, total_tokens, last_time = existing

            # Check if adding would exceed the absolute maximum
            would_exceed_max = (total_tokens + message_tokens) > MAX_CHUNK_TOKENS
            if would_exceed_max:
                # Close current chunk and discard this message
                import logging
                logging.warning(
                    f"Closing author chunk at {total_tokens} tokens and discarding message "
                    f"{message.message_id} to avoid exceeding MAX_CHUNK_TOKENS={MAX_CHUNK_TOKENS}"
                )
                chunk.chunk_state = "closed"
                chunks_to_persist.append(chunk)
                # Remove this author's open chunk
                state.remove_author_chunk(author_id)
                return state, chunks_to_persist

            # Check closing conditions
            time_gap = (message_time - last_time).total_seconds()
            would_exceed_tokens = (total_tokens + message_tokens) > self.config.max_tokens

            if time_gap > self.config.gap_seconds or would_exceed_tokens:
                # Close current chunk
                chunk.chunk_state = "closed"
                chunks_to_persist.append(chunk)

                # Start a new chunk
                new_chunk = self._create_new_chunk(message, guild_id, channel_id)
                new_contexts = {message.message_id: (username, attachments)}
                state.set_author_chunk(
                    author_id, new_chunk, [message], new_contexts, message_tokens, message_time
                )
                chunks_to_persist.append(new_chunk)
            else:
                # Append to current chunk
                messages.append(message)
                message_contexts[message.message_id] = (username, attachments)
                total_tokens += message_tokens
                chunk.message_ids = [m.message_id for m in messages]
                chunk.mentioned_user_ids = sorted(
                    set(uid for m in messages for uid in (m.mentions or []))
                )
                chunk.mentioned_role_ids = sorted(
                    set(rid for m in messages for rid in (m.mention_roles or []))
                )
                chunk.last_message_at = message.created_at
                # author_ids stays the same (single author)
                state.set_author_chunk(
                    author_id, chunk, messages, message_contexts, total_tokens, message_time
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
            mentioned_user_ids=sorted(set(message.mentions or [])),
            mentioned_role_ids=sorted(set(message.mention_roles or [])),
            has_attachments=False,
            chunk_state="open",
            start_message_id=message.message_id,
            leaf_message_id=None,
            cross_channel_ref=None,
            embedding_status="pending",
            first_message_at=message.created_at,
            last_message_at=message.created_at,
        )
