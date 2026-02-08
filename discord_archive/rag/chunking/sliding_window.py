"""Sliding window chunker.

Creates overlapping windows of consecutive messages for general context continuity.
"""

from dataclasses import dataclass, field

from discord_archive.db.models.chunk import Chunk
from discord_archive.db.models.message import Message
from discord_archive.rag.chunking.constants import THREAD_STARTER_MESSAGE_TYPE
from discord_archive.rag.chunking.tokenizer import estimate_tokens


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

    def load_state(self, chunk: Chunk, messages: list[Message]) -> SlidingWindowState:
        """Load state from an existing open chunk."""
        total_tokens = sum(estimate_tokens(m.content or "") for m in messages)
        return SlidingWindowState(
            chunk=chunk,
            messages=messages,
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
    ) -> tuple[SlidingWindowState, list[Chunk]]:
        """Process a single message.

        Returns:
            Updated state and list of chunks to persist (closed chunks + current open).
        """
        chunks_to_persist: list[Chunk] = []
        message_tokens = estimate_tokens(message.content or "")

        # Skip messages with no content (but not attachments-only, handle later)
        if message.type == THREAD_STARTER_MESSAGE_TYPE:
            return state, chunks_to_persist

        if state.is_empty():
            # Start a new window
            state = self._create_new_window(message, message_tokens, guild_id, channel_id)
            chunks_to_persist.append(state.chunk)
        else:
            # Check if adding this message would exceed max_tokens
            would_exceed = (state.total_tokens + message_tokens) > self.config.max_tokens

            if would_exceed and len(state.messages) > 0:
                # Close current window
                state.chunk.chunk_state = "closed"
                chunks_to_persist.append(state.chunk)

                # Create new window with overlap
                overlap_messages = self._compute_overlap(state.messages)
                state = self._create_new_window_with_overlap(
                    overlap_messages, message, message_tokens, guild_id, channel_id
                )
                chunks_to_persist.append(state.chunk)
            else:
                # Append to current window
                state.messages.append(message)
                state.total_tokens += message_tokens
                state.chunk.message_ids = [m.message_id for m in state.messages]
                state.chunk.author_ids = sorted(set(m.author_id for m in state.messages))
                state.chunk.mentioned_user_ids = sorted(set(
                    uid for m in state.messages for uid in (m.mentions or [])
                ))
                state.chunk.mentioned_role_ids = sorted(set(
                    rid for m in state.messages for rid in (m.mention_roles or [])
                ))
                state.chunk.last_message_at = message.created_at
                chunks_to_persist.append(state.chunk)

        return state, chunks_to_persist

    def _create_new_window(
        self,
        message: Message,
        message_tokens: int,
        guild_id: int,
        channel_id: int,
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
            total_tokens=message_tokens,
        )

    def _create_new_window_with_overlap(
        self,
        overlap_messages: list[Message],
        new_message: Message,
        new_message_tokens: int,
        guild_id: int,
        channel_id: int,
    ) -> SlidingWindowState:
        """Create a new window with overlap from previous window."""
        messages = overlap_messages + [new_message]
        overlap_tokens = sum(estimate_tokens(m.content or "") for m in overlap_messages)

        # The start_message_id is the first message in this window
        start_message_id = messages[0].message_id

        chunk = Chunk(
            chunk_type="sliding_window",
            guild_id=guild_id,
            channel_id=channel_id,
            message_ids=[m.message_id for m in messages],
            author_ids=sorted(set(m.author_id for m in messages)),
            mentioned_user_ids=sorted(set(
                uid for m in messages for uid in (m.mentions or [])
            )),
            mentioned_role_ids=sorted(set(
                rid for m in messages for rid in (m.mention_roles or [])
            )),
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
