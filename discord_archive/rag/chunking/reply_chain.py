"""Reply chain chunker.

Creates chunks for reply chains, tracing from leaf message back to root.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from discord_archive.db.models.chunk import Chunk
from discord_archive.db.models.message import Message
from discord_archive.rag.chunking.constants import MAX_CHUNK_TOKENS, THREAD_STARTER_MESSAGE_TYPE
from discord_archive.rag.chunking.tokenizer import estimate_message_context_tokens

if TYPE_CHECKING:
    from discord_archive.db.models.attachment import Attachment


@dataclass
class ReplyChainConfig:
    """Configuration for reply chain chunking."""

    max_tokens: int = 5000
    max_depth: int = 20


class ReplyChainChunker:
    """Reply chain chunker.

    Creates immutable chunks for reply chains. Each reply message gets
    its own chunk containing the full chain from root to leaf.

    Reply chain chunks are always created as 'closed' since they are
    determined entirely by the message graph.
    """

    def __init__(self, config: ReplyChainConfig | None = None):
        self.config = config or ReplyChainConfig()

    def process_message(
        self,
        message: Message,
        message_lookup: dict[int, Message],
        guild_id: int,
        channel_id: int,
        usernames: dict[int, str] | None = None,
        attachments_by_msg: dict[int, list["Attachment"]] | None = None,
    ) -> Chunk | None:
        """Process a single message and create a reply chain chunk if applicable.

        Args:
            message: The message to process (potential leaf of a reply chain)
            message_lookup: Dict of message_id -> Message for chain traversal
            guild_id: Guild ID for the chunk
            channel_id: Channel ID for the chunk
            usernames: author_id -> username mapping (for token estimation)
            attachments_by_msg: message_id -> attachments list mapping (for token estimation)

        Returns:
            A Chunk if this message is a reply, None otherwise.
        """
        if usernames is None:
            usernames = {}
        if attachments_by_msg is None:
            attachments_by_msg = {}

        # Skip if not a reply
        if not message.referenced_message_id:
            return None

        # Skip thread starters
        if message.type == THREAD_STARTER_MESSAGE_TYPE:
            return None

        # Build the chain
        chain, cross_channel_ref = self._build_chain(
            message, message_lookup, channel_id, usernames, attachments_by_msg
        )

        # Filter out thread starters from chain
        chain = [m for m in chain if m.type != THREAD_STARTER_MESSAGE_TYPE]

        # Must have at least one message with content
        if not chain or not any(m.content for m in chain):
            return None

        # Create the chunk
        return Chunk(
            chunk_type="reply_chain",
            guild_id=guild_id,
            channel_id=channel_id,
            message_ids=[m.message_id for m in chain],
            author_ids=sorted(set(m.author_id for m in chain)),
            mentioned_user_ids=sorted(set(
                uid for m in chain for uid in (m.mentions or [])
            )),
            mentioned_role_ids=sorted(set(
                rid for m in chain for rid in (m.mention_roles or [])
            )),
            has_attachments=False,
            chunk_state="closed",  # Always closed
            start_message_id=chain[0].message_id,
            leaf_message_id=chain[-1].message_id,
            cross_channel_ref=cross_channel_ref,
            embedding_status="pending",
            first_message_at=chain[0].created_at,
            last_message_at=chain[-1].created_at,
        )

    def _build_chain(
        self,
        leaf_message: Message,
        message_lookup: dict[int, Message],
        channel_id: int,
        usernames: dict[int, str],
        attachments_by_msg: dict[int, list["Attachment"]],
    ) -> tuple[list[Message], int | None]:
        """Build the reply chain from root to leaf.

        Traverses up the reply chain until:
        - No more references
        - Message not found (missing parent)
        - Cross-channel reference
        - Max depth reached
        - Token limit reached

        Returns:
            Tuple of (chain messages in root->leaf order, cross_channel_ref if any)
        """
        chain: list[Message] = []
        cross_channel_ref: int | None = None
        visited: set[int] = set()
        total_tokens = 0

        current = leaf_message

        while True:
            # Cycle detection
            if current.message_id in visited:
                break
            visited.add(current.message_id)

            # Add to chain (will reverse later)
            chain.append(current)

            # Token estimation
            username = usernames.get(current.author_id)
            attachments = attachments_by_msg.get(current.message_id, [])
            message_tokens = estimate_message_context_tokens(current, username, attachments)
            total_tokens += message_tokens

            # Check absolute maximum first
            if total_tokens > MAX_CHUNK_TOKENS:
                import logging
                logging.warning(
                    f"Reply chain exceeds MAX_CHUNK_TOKENS={MAX_CHUNK_TOKENS} at {total_tokens} tokens, "
                    f"discarding entire chain starting from message {leaf_message.message_id}"
                )
                return [], None

            # Check limits
            if len(chain) >= self.config.max_depth:
                break
            if total_tokens >= self.config.max_tokens:
                break

            # Check for parent
            ref_id = current.referenced_message_id
            if not ref_id:
                break

            parent = message_lookup.get(ref_id)
            if parent is None:
                # Missing parent - treat current as root
                break

            if parent.channel_id != channel_id:
                # Cross-channel reference - record and stop
                cross_channel_ref = parent.message_id
                break

            current = parent

        # Reverse to get root -> leaf order
        chain.reverse()

        return chain, cross_channel_ref
