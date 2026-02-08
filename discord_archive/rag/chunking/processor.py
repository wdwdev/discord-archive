"""Main chunking processor.

Coordinates the three chunkers and persists results to database.
"""

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

if TYPE_CHECKING:
    from collections.abc import Callable

from discord_archive.db.models.attachment import Attachment
from discord_archive.db.models.chunk import Chunk
from discord_archive.db.models.message import Message
from discord_archive.db.models.user import User
from discord_archive.db.repositories.chunk_repository import (
    bulk_insert_reply_chains,
    bulk_upsert_chunks,
    get_open_author_groups,
    get_open_sliding_window,
)
from discord_archive.db.repositories.chunk_text_repository import (
    bulk_insert_chunk_texts,
)
from discord_archive.db.repositories.chunking_checkpoint_repository import (
    get_chunking_checkpoint,
    upsert_chunking_checkpoint,
)
from discord_archive.rag.chunking.author_group import (
    AuthorGroupChunker,
    AuthorGroupConfig,
    AuthorGroupState,
)
from discord_archive.rag.chunking.reply_chain import (
    ReplyChainChunker,
    ReplyChainConfig,
)
from discord_archive.rag.chunking.sliding_window import (
    SlidingWindowChunker,
    SlidingWindowConfig,
    SlidingWindowState,
)
from discord_archive.rag.chunking.text_builder import (
    MessageContext,
    TextBuilder,
)

logger = logging.getLogger(__name__)


@dataclass
class ChunkingConfig:
    """Configuration for the chunking processor."""

    sliding_window: SlidingWindowConfig
    author_group: AuthorGroupConfig
    reply_chain: ReplyChainConfig
    batch_size: int = 3000

    @classmethod
    def default(cls) -> "ChunkingConfig":
        return cls(
            sliding_window=SlidingWindowConfig(),
            author_group=AuthorGroupConfig(),
            reply_chain=ReplyChainConfig(),
            batch_size=3000,
        )


@dataclass
class ChunkingStats:
    """Statistics from a chunking run."""

    messages_processed: int = 0
    sliding_window_created: int = 0
    sliding_window_closed: int = 0
    author_group_created: int = 0
    author_group_closed: int = 0
    reply_chain_created: int = 0


class ChunkingProcessor:
    """Main chunking processor.

    Coordinates sliding window, author group, and reply chain chunkers.
    Handles state loading, message processing, and persistence.
    """

    def __init__(self, config: ChunkingConfig | None = None):
        self.config = config or ChunkingConfig.default()
        self.sliding_window = SlidingWindowChunker(self.config.sliding_window)
        self.author_group = AuthorGroupChunker(self.config.author_group)
        self.reply_chain = ReplyChainChunker(self.config.reply_chain)
        self.text_builder = TextBuilder()

    async def process_channel(
        self,
        session: AsyncSession,
        guild_id: int,
        channel_id: int,
        progress_callback: "Callable[[int, int, int], None] | None" = None,
    ) -> ChunkingStats:
        """Process all new messages in a channel.

        Loads checkpoint, processes messages in batches, updates checkpoint.

        Args:
            session: Database session
            guild_id: Guild ID
            channel_id: Channel ID
            progress_callback: Optional callback(messages_processed, chunks_created, chunks_closed)
        """
        stats = ChunkingStats()

        # Get checkpoint
        checkpoint = await get_chunking_checkpoint(session, channel_id)
        last_message_id = checkpoint.last_message_id if checkpoint else 0

        # Load existing open chunks
        sw_state, ag_state = await self._load_states(session, channel_id)

        # Process messages in batches
        while True:
            messages = await self._fetch_messages(
                session, channel_id, last_message_id, self.config.batch_size
            )

            if not messages:
                break

            # Build message lookup for reply chain traversal
            message_lookup = await self._build_message_lookup(session, messages)

            # Process batch and collect chunks to persist
            batch_stats, chunks_to_persist = await self._process_batch(
                messages, message_lookup, guild_id, channel_id, sw_state, ag_state
            )

            # Batch persist all chunks and build texts
            await self._persist_chunks_batch(session, chunks_to_persist, message_lookup)

            # Update stats
            stats.messages_processed += batch_stats.messages_processed
            stats.sliding_window_created += batch_stats.sliding_window_created
            stats.sliding_window_closed += batch_stats.sliding_window_closed
            stats.author_group_created += batch_stats.author_group_created
            stats.author_group_closed += batch_stats.author_group_closed
            stats.reply_chain_created += batch_stats.reply_chain_created

            # Update checkpoint
            last_message_id = messages[-1].message_id
            await upsert_chunking_checkpoint(session, channel_id, last_message_id)

            # Commit batch
            await session.commit()

            # Report progress
            if progress_callback:
                chunks_created = (
                    stats.sliding_window_created
                    + stats.author_group_created
                    + stats.reply_chain_created
                )
                chunks_closed = stats.sliding_window_closed + stats.author_group_closed
                progress_callback(stats.messages_processed, chunks_created, chunks_closed)

        return stats

    async def _process_batch(
        self,
        messages: list[Message],
        message_lookup: dict[int, Message],
        guild_id: int,
        channel_id: int,
        sw_state: SlidingWindowState,
        ag_state: AuthorGroupState,
    ) -> tuple[ChunkingStats, list[Chunk]]:
        """Process a batch of messages without persisting.

        Returns stats and list of unique chunks to persist.
        Only includes each chunk once (the final state).
        """
        stats = ChunkingStats()

        # Track unique chunks by their identity key
        # For sliding_window/author_group: (chunk_type, channel_id, start_message_id)
        # For reply_chain: (chunk_type, leaf_message_id)
        sw_ag_chunks: dict[tuple, Chunk] = {}
        reply_chains: dict[int, Chunk] = {}

        for message in messages:
            stats.messages_processed += 1

            # Sliding window
            new_sw_state, sw_chunks = self.sliding_window.process_message(
                sw_state, message, guild_id, channel_id
            )
            for chunk in sw_chunks:
                key = (chunk.chunk_type, chunk.channel_id, chunk.start_message_id)
                was_new = chunk.chunk_id is None and key not in sw_ag_chunks
                if was_new:
                    stats.sliding_window_created += 1
                if chunk.chunk_state == "closed":
                    stats.sliding_window_closed += 1
                sw_ag_chunks[key] = chunk

            sw_state.chunk = new_sw_state.chunk
            sw_state.messages = new_sw_state.messages
            sw_state.total_tokens = new_sw_state.total_tokens

            # Author group
            new_ag_state, ag_chunks = self.author_group.process_message(
                ag_state, message, guild_id, channel_id
            )
            for chunk in ag_chunks:
                key = (chunk.chunk_type, chunk.channel_id, chunk.start_message_id)
                was_new = chunk.chunk_id is None and key not in sw_ag_chunks
                if was_new:
                    stats.author_group_created += 1
                if chunk.chunk_state == "closed":
                    stats.author_group_closed += 1
                sw_ag_chunks[key] = chunk

            ag_state.open_chunks = new_ag_state.open_chunks

            # Reply chain (stateless, always closed)
            rc_chunk = self.reply_chain.process_message(
                message, message_lookup, guild_id, channel_id
            )
            if rc_chunk and rc_chunk.leaf_message_id:
                if rc_chunk.leaf_message_id not in reply_chains:
                    stats.reply_chain_created += 1
                reply_chains[rc_chunk.leaf_message_id] = rc_chunk

        # Combine all unique chunks
        chunks_to_persist = list(sw_ag_chunks.values()) + list(reply_chains.values())
        return stats, chunks_to_persist

    async def _persist_chunks_batch(
        self,
        session: AsyncSession,
        chunks: list[Chunk],
        message_lookup: dict[int, Message],
    ) -> None:
        """Persist multiple chunks efficiently using bulk operations.

        Chunks are already deduplicated by _process_batch.
        Also builds and persists chunk texts after chunk IDs are available.
        """
        if not chunks:
            return

        # Set has_attachments before persisting
        msg_ids_with_attachments = await self._get_message_ids_with_attachments(
            session, chunks
        )
        for chunk in chunks:
            if not chunk.has_attachments:
                chunk.has_attachments = any(
                    mid in msg_ids_with_attachments for mid in chunk.message_ids
                )

        # Separate by type for optimal bulk handling
        reply_chains = [c for c in chunks if c.chunk_type == "reply_chain"]
        other_chunks = [c for c in chunks if c.chunk_type != "reply_chain"]

        # Bulk insert reply chains (immutable, ON CONFLICT DO NOTHING)
        if reply_chains:
            await bulk_insert_reply_chains(session, reply_chains)

        # Bulk upsert sliding_window and author_group chunks
        if other_chunks:
            id_map = await bulk_upsert_chunks(session, other_chunks)
            # Update chunk IDs for state tracking
            for chunk in other_chunks:
                key = (chunk.chunk_type, chunk.channel_id, chunk.start_message_id)
                if key in id_map and chunk.chunk_id is None:
                    chunk.chunk_id = id_map[key]

        # Build and persist texts for chunks with IDs
        await self._build_and_persist_texts(session, chunks, message_lookup)

    async def _load_states(
        self,
        session: AsyncSession,
        channel_id: int,
    ) -> tuple[SlidingWindowState, AuthorGroupState]:
        """Load chunker states from existing open chunks."""
        # Load sliding window state
        sw_chunk = await get_open_sliding_window(session, channel_id)
        if sw_chunk:
            sw_messages = await self._fetch_messages_by_ids(
                session, sw_chunk.message_ids
            )
            sw_state = self.sliding_window.load_state(sw_chunk, sw_messages)
        else:
            sw_state = self.sliding_window.create_empty_state()

        # Load author group state
        ag_chunks = await get_open_author_groups(session, channel_id)
        if ag_chunks:
            # Collect all message IDs from all chunks in one query
            all_message_ids: list[int] = []
            for chunk in ag_chunks.values():
                all_message_ids.extend(chunk.message_ids)

            # Fetch all messages at once
            all_messages_dict: dict[int, Message] = {}
            if all_message_ids:
                stmt = (
                    select(
                        Message.message_id,
                        Message.channel_id,
                        Message.author_id,
                        Message.content,
                        Message.created_at,
                        Message.type,
                        Message.referenced_message_id,
                        Message.embeds,
                        Message.mentions,
                        Message.mention_roles,
                    )
                    .where(Message.message_id.in_(all_message_ids))
                )
                result = await session.execute(stmt)
                for row in result.all():
                    all_messages_dict[row.message_id] = Message(
                        message_id=row.message_id,
                        channel_id=row.channel_id,
                        author_id=row.author_id,
                        content=row.content or "",
                        created_at=row.created_at,
                        type=row.type,
                        referenced_message_id=row.referenced_message_id,
                        embeds=row.embeds or [],
                        mentions=row.mentions or [],
                        mention_roles=row.mention_roles or [],
                    )

            # Build messages_by_author from the fetched messages
            messages_by_author: dict[int, list[Message]] = {}
            for author_id, chunk in ag_chunks.items():
                found_messages = []
                missing_ids = []
                for mid in chunk.message_ids:
                    if mid in all_messages_dict:
                        found_messages.append(all_messages_dict[mid])
                    else:
                        missing_ids.append(mid)

                if missing_ids:
                    logger.warning(
                        "Author group chunk (author=%d) missing %d messages: %s",
                        author_id,
                        len(missing_ids),
                        missing_ids[:5],
                    )

                messages_by_author[author_id] = found_messages

            ag_state = self.author_group.load_state(ag_chunks, messages_by_author)
        else:
            ag_state = self.author_group.create_empty_state()

        return sw_state, ag_state

    async def _fetch_messages(
        self,
        session: AsyncSession,
        channel_id: int,
        after_message_id: int,
        limit: int,
    ) -> list[Message]:
        """Fetch messages after a given message ID.

        Only loads columns needed for chunking (excludes heavy JSONB like raw).
        """
        stmt = (
            select(
                Message.message_id,
                Message.channel_id,
                Message.author_id,
                Message.content,
                Message.created_at,
                Message.type,
                Message.referenced_message_id,
                Message.embeds,
                Message.mentions,
                Message.mention_roles,
            )
            .where(Message.channel_id == channel_id)
            .where(Message.message_id > after_message_id)
            .order_by(Message.message_id)
            .limit(limit)
        )
        result = await session.execute(stmt)
        rows = result.all()

        # Convert to Message objects (lightweight, no ORM tracking)
        return [
            Message(
                message_id=row.message_id,
                channel_id=row.channel_id,
                author_id=row.author_id,
                content=row.content or "",
                created_at=row.created_at,
                type=row.type,
                referenced_message_id=row.referenced_message_id,
                embeds=row.embeds or [],
                mentions=row.mentions or [],
                mention_roles=row.mention_roles or [],
            )
            for row in rows
        ]

    async def _fetch_messages_by_ids(
        self,
        session: AsyncSession,
        message_ids: list[int],
    ) -> list[Message]:
        """Fetch messages by IDs, preserving order.

        Only loads columns needed for chunking (excludes heavy JSONB like raw).
        """
        if not message_ids:
            return []

        stmt = (
            select(
                Message.message_id,
                Message.channel_id,
                Message.author_id,
                Message.content,
                Message.created_at,
                Message.type,
                Message.referenced_message_id,
                Message.embeds,
                Message.mentions,
                Message.mention_roles,
            )
            .where(Message.message_id.in_(message_ids))
        )
        result = await session.execute(stmt)
        rows = result.all()

        # Convert to dict for ordering
        messages = {
            row.message_id: Message(
                message_id=row.message_id,
                channel_id=row.channel_id,
                author_id=row.author_id,
                content=row.content or "",
                created_at=row.created_at,
                type=row.type,
                referenced_message_id=row.referenced_message_id,
                embeds=row.embeds or [],
                mentions=row.mentions or [],
                mention_roles=row.mention_roles or [],
            )
            for row in rows
        }

        # Preserve order, warn about missing messages
        ordered: list[Message] = []
        missing = []
        for mid in message_ids:
            if mid in messages:
                ordered.append(messages[mid])
            else:
                missing.append(mid)

        if missing:
            logger.warning(
                "Missing %d messages from database (first 5: %s). "
                "These may have been deleted.",
                len(missing),
                missing[:5],
            )

        return ordered

    async def _build_message_lookup(
        self,
        session: AsyncSession,
        messages: list[Message],
    ) -> dict[int, Message]:
        """Build a lookup dict for reply chain traversal.

        Includes the current batch plus any referenced messages.
        Uses recursive fetching to handle deep reply chains.
        Only loads columns needed for chunking (excludes heavy JSONB like raw).
        """
        lookup = {m.message_id: m for m in messages}

        # Recursively fetch referenced messages up to max_depth
        max_depth = self.reply_chain.config.max_depth
        pending_ids: set[int] = set()

        # Initial pass: find references not in lookup
        for m in messages:
            if m.referenced_message_id and m.referenced_message_id not in lookup:
                pending_ids.add(m.referenced_message_id)

        depth = 0
        while pending_ids and depth < max_depth:
            depth += 1

            stmt = (
                select(
                    Message.message_id,
                    Message.channel_id,
                    Message.author_id,
                    Message.content,
                    Message.created_at,
                    Message.type,
                    Message.referenced_message_id,
                    Message.embeds,
                    Message.mentions,
                    Message.mention_roles,
                )
                .where(Message.message_id.in_(pending_ids))
            )
            result = await session.execute(stmt)
            rows = result.all()

            # Track which IDs we actually found
            found_ids = set()
            next_pending: set[int] = set()

            for row in rows:
                found_ids.add(row.message_id)
                lookup[row.message_id] = Message(
                    message_id=row.message_id,
                    channel_id=row.channel_id,
                    author_id=row.author_id,
                    content=row.content or "",
                    created_at=row.created_at,
                    type=row.type,
                    referenced_message_id=row.referenced_message_id,
                    embeds=row.embeds or [],
                    mentions=row.mentions or [],
                    mention_roles=row.mention_roles or [],
                )
                # Queue parent for next iteration
                if row.referenced_message_id and row.referenced_message_id not in lookup:
                    next_pending.add(row.referenced_message_id)

            # Log if some messages weren't found (deleted or cross-channel)
            missing = pending_ids - found_ids
            if missing:
                logger.debug(
                    "Reply chain depth %d: %d messages not found (deleted or cross-channel)",
                    depth,
                    len(missing),
                )

            pending_ids = next_pending

        if pending_ids:
            logger.debug(
                "Reply chain traversal reached max depth %d with %d unresolved references",
                max_depth,
                len(pending_ids),
            )

        return lookup

    async def _fetch_attachments_for_messages(
        self,
        session: AsyncSession,
        message_ids: list[int],
    ) -> dict[int, list[Attachment]]:
        """Fetch attachments grouped by message_id.

        Returns a dict mapping message_id to list of Attachment objects.
        Messages without attachments are not included in the result.
        """
        if not message_ids:
            return {}

        stmt = select(Attachment).where(Attachment.message_id.in_(message_ids))
        result = await session.scalars(stmt)
        attachments = result.all()

        # Group by message_id
        grouped: dict[int, list[Attachment]] = {}
        for att in attachments:
            if att.message_id not in grouped:
                grouped[att.message_id] = []
            grouped[att.message_id].append(att)

        return grouped

    async def _get_message_ids_with_attachments(
        self,
        session: AsyncSession,
        chunks: list[Chunk],
    ) -> set[int]:
        """Get message IDs that have attachments.

        Collects all message IDs from chunks and queries the attachments table
        to find which ones have attachments. Returns a set of message IDs.
        """
        all_msg_ids: set[int] = set()
        for chunk in chunks:
            if not chunk.has_attachments:
                all_msg_ids.update(chunk.message_ids)

        if not all_msg_ids:
            return set()

        from sqlalchemy import distinct

        stmt = (
            select(distinct(Attachment.message_id))
            .where(Attachment.message_id.in_(list(all_msg_ids)))
        )
        result = await session.scalars(stmt)
        return set(result.all())

    async def _fetch_users_for_authors(
        self,
        session: AsyncSession,
        author_ids: list[int],
    ) -> dict[int, str]:
        """Fetch usernames by user_id.

        Returns a dict mapping user_id to username.
        Missing users are not included in the result (caller should handle None).
        """
        if not author_ids:
            return {}

        # Deduplicate author_ids
        unique_ids = list(set(author_ids))

        stmt = select(User.user_id, User.username).where(User.user_id.in_(unique_ids))
        result = await session.execute(stmt)

        # Build mapping, only including users with usernames
        return {
            row.user_id: row.username
            for row in result.all()
            if row.username is not None
        }

    async def _build_and_persist_texts(
        self,
        session: AsyncSession,
        chunks: list[Chunk],
        message_lookup: dict[int, Message],
    ) -> None:
        """Build and persist chunk texts.

        Fetches attachments and usernames, builds MessageContext for each message,
        then builds and persists chunk texts.
        """
        # Filter to chunks with IDs (successfully persisted)
        chunks_with_ids = [c for c in chunks if c.chunk_id is not None]
        if not chunks_with_ids:
            return

        # Collect all message_ids and author_ids from chunks
        all_message_ids: set[int] = set()
        all_author_ids: set[int] = set()
        for chunk in chunks_with_ids:
            for mid in chunk.message_ids:
                all_message_ids.add(mid)
            for aid in chunk.author_ids:
                all_author_ids.add(aid)

        # Find messages missing from lookup (from previous batches)
        missing_ids = [mid for mid in all_message_ids if mid not in message_lookup]
        if missing_ids:
            # Fetch missing messages and add to lookup
            missing_messages = await self._fetch_messages_by_ids(session, missing_ids)
            for msg in missing_messages:
                message_lookup[msg.message_id] = msg

        # Batch fetch attachments and usernames
        attachments_by_msg = await self._fetch_attachments_for_messages(
            session, list(all_message_ids)
        )
        usernames_by_author = await self._fetch_users_for_authors(
            session, list(all_author_ids)
        )

        # Build texts for each chunk
        chunk_texts: list[tuple[int, str, int]] = []
        for chunk in chunks_with_ids:
            # Build MessageContext for each message in the chunk
            contexts: list[MessageContext] = []
            for mid in chunk.message_ids:
                msg = message_lookup.get(mid)
                if msg is None:
                    logger.warning(
                        "Chunk %d references message %d not found in database",
                        chunk.chunk_id,
                        mid,
                    )
                    continue

                ctx = MessageContext(
                    message=msg,
                    author_username=usernames_by_author.get(msg.author_id),
                    attachments=attachments_by_msg.get(mid, []),
                )
                contexts.append(ctx)

            if not contexts:
                continue

            # Build text
            text, token_count = self.text_builder.build_chunk_text(chunk, contexts)
            chunk_texts.append((chunk.chunk_id, text, token_count))

        # Bulk insert chunk texts
        if chunk_texts:
            await bulk_insert_chunk_texts(session, chunk_texts)
