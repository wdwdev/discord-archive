"""Sliding window chunker.

Creates overlapping windows of consecutive messages for general context continuity.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from discord_archive.db.models.chunk import Chunk
from discord_archive.db.models.message import Message
from discord_archive.rag.chunking.constants import MAX_CHUNK_TOKENS, THREAD_STARTER_MESSAGE_TYPE
from discord_archive.rag.chunking.tokenizer import estimate_message_context_tokens

if TYPE_CHECKING:
    from discord_archive.db.models.attachment import Attachment


@dataclass
class SlidingWindowConfig:
    """Configuration for sliding window chunking."""

    max_tokens: int = 500
    overlap_ratio: float = 0.25
    max_overlap_messages: int = 10


@dataclass
class SlidingWindowState:
    """Mutable state for sliding window chunking.

    Tracks the current open window and its contents.
    """

    # The open chunk (if any)
    chunk: Chunk | None = None

    # Cached content for token estimation
    messages: list[Message] = field(default_factory=list)
    # Context for each message (username, attachments)
    message_contexts: dict[int, tuple[str | None, list["Attachment"]]] = field(default_factory=dict)
    total_tokens: int = 0

    def is_empty(self) -> bool:
        return self.chunk is None


class SlidingWindowChunker:
    """Sliding window chunker.

    Creates overlapping windows of consecutive messages.
    At most one window can be open per channel at a time.
    """

    def __init__(self, config: SlidingWindowConfig | None = None):
        self.config = config or SlidingWindowConfig()

    def load_state(
        self,
        chunk: Chunk,
        messages: list[Message],
        usernames: dict[int, str],
        attachments_by_msg: dict[int, list["Attachment"]],
    ) -> SlidingWindowState:
        """Load state from an existing open chunk.

        Args:
            chunk: Existing open chunk
            messages: Messages in the chunk
            usernames: author_id → username mapping
            attachments_by_msg: message_id → attachments list mapping
        """
        message_contexts = {}
        total_tokens = 0

        for m in messages:
            username = usernames.get(m.author_id)
            attachments = attachments_by_msg.get(m.message_id, [])
            message_contexts[m.message_id] = (username, attachments)
            total_tokens += estimate_message_context_tokens(m, username, attachments)

        return SlidingWindowState(
            chunk=chunk,
            messages=messages,
            message_contexts=message_contexts,
            total_tokens=total_tokens,
        )

    def create_empty_state(self) -> SlidingWindowState:
        """Create an empty state (no open window)."""
        return SlidingWindowState()

    def process_message(
        self,
        state: SlidingWindowState,
        message: Message,
        guild_id: int,
        channel_id: int,
        username: str | None = None,
        attachments: list["Attachment"] | None = None,
    ) -> tuple[SlidingWindowState, list[Chunk]]:
        """Process a single message.

        Args:
            state: Current chunker state
            message: Message to process
            guild_id: Guild ID
            channel_id: Channel ID
            username: Author username (for token estimation)
            attachments: List of attachments (for token estimation)

        Returns:
            Updated state and list of chunks to persist (closed chunks + current open).
        """
        if attachments is None:
            attachments = []

        chunks_to_persist: list[Chunk] = []
        message_tokens = estimate_message_context_tokens(message, username, attachments)

        # Skip messages with no content (but not attachments-only, handle later)
        if message.type == THREAD_STARTER_MESSAGE_TYPE:
            return state, chunks_to_persist

        # Discard single messages that exceed the absolute maximum
        if message_tokens > MAX_CHUNK_TOKENS:
            # Log warning and skip this message
            import logging
            logging.warning(
                f"Discarding message {message.message_id} with {message_tokens} tokens "
                f"(exceeds MAX_CHUNK_TOKENS={MAX_CHUNK_TOKENS})"
            )
            return state, chunks_to_persist

        if state.is_empty():
            # Start a new window
            state = self._create_new_window(
                message, message_tokens, guild_id, channel_id, username, attachments
            )
            chunks_to_persist.append(state.chunk)
        else:
            # Check if adding this message would exceed the absolute maximum
            would_exceed_max = (state.total_tokens + message_tokens) > MAX_CHUNK_TOKENS
            if would_exceed_max:
                # Close current window and discard this message
                import logging
                logging.warning(
                    f"Closing chunk at {state.total_tokens} tokens and discarding message "
                    f"{message.message_id} to avoid exceeding MAX_CHUNK_TOKENS={MAX_CHUNK_TOKENS}"
                )
                state.chunk.chunk_state = "closed"
                chunks_to_persist.append(state.chunk)
                # Reset state without adding the oversized message
                state = SlidingWindowState()
                return state, chunks_to_persist

            # Check if adding this message would exceed max_tokens
            would_exceed = (state.total_tokens + message_tokens) > self.config.max_tokens

            if would_exceed and len(state.messages) > 0:
                # Close current window
                state.chunk.chunk_state = "closed"
                chunks_to_persist.append(state.chunk)

                # Create new window with overlap
                overlap_messages = self._compute_overlap(state.messages)
                state = self._create_new_window_with_overlap(
                    overlap_messages,
                    message,
                    message_tokens,
                    guild_id,
                    channel_id,
                    username,
                    attachments,
                    state.message_contexts,
                )
                chunks_to_persist.append(state.chunk)
            else:
                # Append to current window
                state.messages.append(message)
                state.message_contexts[message.message_id] = (username, attachments)
                state.total_tokens += message_tokens
                state.chunk.message_ids = [m.message_id for m in state.messages]
                state.chunk.author_ids = sorted(set(m.author_id for m in state.messages))
                state.chunk.mentioned_user_ids = sorted(
                    set(uid for m in state.messages for uid in (m.mentions or []))
                )
                state.chunk.mentioned_role_ids = sorted(
                    set(rid for m in state.messages for rid in (m.mention_roles or []))
                )
                state.chunk.last_message_at = message.created_at
                chunks_to_persist.append(state.chunk)

        return state, chunks_to_persist

    def _create_new_window(
        self,
        message: Message,
        message_tokens: int,
        guild_id: int,
        channel_id: int,
        username: str | None,
        attachments: list["Attachment"],
    ) -> SlidingWindowState:
        """Create a new window starting with the given message."""
        chunk = Chunk(
            chunk_type="sliding_window",
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
        return SlidingWindowState(
            chunk=chunk,
            messages=[message],
            message_contexts={message.message_id: (username, attachments)},
            total_tokens=message_tokens,
        )

    def _create_new_window_with_overlap(
        self,
        overlap_messages: list[Message],
        new_message: Message,
        new_message_tokens: int,
        guild_id: int,
        channel_id: int,
        new_username: str | None,
        new_attachments: list["Attachment"],
        prev_contexts: dict[int, tuple[str | None, list["Attachment"]]],
    ) -> SlidingWindowState:
        """Create a new window with overlap from previous window."""
        messages = overlap_messages + [new_message]

        # Rebuild contexts and estimate tokens
        message_contexts = {}
        overlap_tokens = 0

        for m in overlap_messages:
            ctx = prev_contexts.get(m.message_id, (None, []))
            message_contexts[m.message_id] = ctx
            overlap_tokens += estimate_message_context_tokens(m, ctx[0], ctx[1])

        message_contexts[new_message.message_id] = (new_username, new_attachments)

        # The start_message_id is the first message in this window
        start_message_id = messages[0].message_id

        chunk = Chunk(
            chunk_type="sliding_window",
            guild_id=guild_id,
            channel_id=channel_id,
            message_ids=[m.message_id for m in messages],
            author_ids=sorted(set(m.author_id for m in messages)),
            mentioned_user_ids=sorted(set(uid for m in messages for uid in (m.mentions or []))),
            mentioned_role_ids=sorted(set(rid for m in messages for rid in (m.mention_roles or []))),
            has_attachments=False,
            chunk_state="open",
            start_message_id=start_message_id,
            leaf_message_id=None,
            cross_channel_ref=None,
            embedding_status="pending",
            first_message_at=messages[0].created_at,
            last_message_at=messages[-1].created_at,
        )
        return SlidingWindowState(
            chunk=chunk,
            messages=messages,
            message_contexts=message_contexts,
            total_tokens=overlap_tokens + new_message_tokens,
        )

    def _compute_overlap(self, messages: list[Message]) -> list[Message]:
        """Compute overlap messages for the next window.

        K = max(1, floor(window_message_count * overlap_ratio))
        K <= max_overlap_messages
        """
        if not messages:
            return []

        k = max(1, int(len(messages) * self.config.overlap_ratio))
        k = min(k, self.config.max_overlap_messages)
        k = min(k, len(messages))

        return messages[-k:]
