"""Tests for discord_archive.rag.chunking.reply_chain."""

from __future__ import annotations

from datetime import datetime, timezone

from discord_archive.db.models.message import Message
from discord_archive.rag.chunking.reply_chain import (
    ReplyChainChunker,
    ReplyChainConfig,
)


def make_message(
    message_id: int,
    author_id: int = 1,
    content: str = "test",
    channel_id: int = 100,
    referenced_message_id: int | None = None,
    msg_type: int = 0,
    mentions: list[int] | None = None,
    mention_roles: list[int] | None = None,
) -> Message:
    """Create a test message."""
    return Message(
        message_id=message_id,
        channel_id=channel_id,
        author_id=author_id,
        content=content,
        created_at=datetime.now(timezone.utc),
        type=msg_type,
        referenced_message_id=referenced_message_id,
        mentions=mentions or [],
        mention_roles=mention_roles or [],
    )


class TestReplyChainConfig:
    """Tests for ReplyChainConfig."""

    def test_default_values(self) -> None:
        config = ReplyChainConfig()
        assert config.max_tokens == 5000
        assert config.max_depth == 20

    def test_custom_values(self) -> None:
        config = ReplyChainConfig(max_tokens=1000, max_depth=10)
        assert config.max_tokens == 1000
        assert config.max_depth == 10


class TestReplyChainChunker:
    """Tests for ReplyChainChunker."""

    def test_non_reply_returns_none(self) -> None:
        chunker = ReplyChainChunker()
        msg = make_message(1, content="hello")
        lookup = {1: msg}

        result = chunker.process_message(msg, lookup, guild_id=1, channel_id=100)

        assert result is None

    def test_reply_creates_closed_chunk(self) -> None:
        chunker = ReplyChainChunker()

        msg1 = make_message(1, author_id=100, content="original")
        msg2 = make_message(
            2, author_id=200, content="reply", referenced_message_id=1
        )
        lookup = {1: msg1, 2: msg2}

        result = chunker.process_message(msg2, lookup, guild_id=1, channel_id=100)

        assert result is not None
        assert result.chunk_type == "reply_chain"
        assert result.chunk_state == "closed"  # Always closed
        assert result.message_ids == [1, 2]  # Root to leaf order
        assert set(result.author_ids) == {100, 200}
        assert result.start_message_id == 1
        assert result.leaf_message_id == 2
        assert result.first_message_at == msg1.created_at
        assert result.last_message_at == msg2.created_at

    def test_deep_chain_traversal(self) -> None:
        chunker = ReplyChainChunker()

        # Create a chain: 1 <- 2 <- 3 <- 4
        msg1 = make_message(1, content="original")
        msg2 = make_message(2, content="reply1", referenced_message_id=1)
        msg3 = make_message(3, content="reply2", referenced_message_id=2)
        msg4 = make_message(4, content="reply3", referenced_message_id=3)
        lookup = {1: msg1, 2: msg2, 3: msg3, 4: msg4}

        result = chunker.process_message(msg4, lookup, guild_id=1, channel_id=100)

        assert result is not None
        assert result.message_ids == [1, 2, 3, 4]
        assert result.start_message_id == 1
        assert result.leaf_message_id == 4

    def test_max_depth_limits_chain(self) -> None:
        config = ReplyChainConfig(max_depth=2)
        chunker = ReplyChainChunker(config)

        # Create a chain: 1 <- 2 <- 3
        msg1 = make_message(1, content="original")
        msg2 = make_message(2, content="reply1", referenced_message_id=1)
        msg3 = make_message(3, content="reply2", referenced_message_id=2)
        lookup = {1: msg1, 2: msg2, 3: msg3}

        result = chunker.process_message(msg3, lookup, guild_id=1, channel_id=100)

        assert result is not None
        # Should stop at max_depth (2 messages)
        assert len(result.message_ids) == 2

    def test_missing_parent_stops_traversal(self) -> None:
        chunker = ReplyChainChunker()

        # Message 2 references message 1, but 1 is not in lookup
        msg2 = make_message(2, content="reply", referenced_message_id=1)
        lookup = {2: msg2}

        result = chunker.process_message(msg2, lookup, guild_id=1, channel_id=100)

        assert result is not None
        assert result.message_ids == [2]  # Only the leaf

    def test_cross_channel_ref_stops_traversal(self) -> None:
        chunker = ReplyChainChunker()

        # msg1 is in channel 200, msg2 references it but is in channel 100
        msg1 = make_message(1, content="original", channel_id=200)
        msg2 = make_message(
            2, content="reply", channel_id=100, referenced_message_id=1
        )
        lookup = {1: msg1, 2: msg2}

        result = chunker.process_message(msg2, lookup, guild_id=1, channel_id=100)

        assert result is not None
        assert result.message_ids == [2]  # Only the leaf
        assert result.cross_channel_ref == 1  # Records the cross-channel reference

    def test_thread_starter_message_skipped(self) -> None:
        chunker = ReplyChainChunker()

        # Thread starter (type 21) should be skipped entirely
        msg = make_message(
            1, content="thread start", msg_type=21, referenced_message_id=100
        )
        lookup = {1: msg}

        result = chunker.process_message(msg, lookup, guild_id=1, channel_id=100)

        assert result is None

    def test_thread_starter_filtered_from_chain(self) -> None:
        chunker = ReplyChainChunker()

        # Chain: 1 (normal) <- 2 (thread starter) <- 3 (normal reply)
        msg1 = make_message(1, content="original")
        msg2 = make_message(
            2, content="thread start", msg_type=21, referenced_message_id=1
        )
        msg3 = make_message(3, content="reply", referenced_message_id=2)
        lookup = {1: msg1, 2: msg2, 3: msg3}

        result = chunker.process_message(msg3, lookup, guild_id=1, channel_id=100)

        assert result is not None
        # Thread starter should be filtered out
        assert 2 not in result.message_ids
        assert set(result.message_ids) == {1, 3}

    def test_cycle_detection(self) -> None:
        chunker = ReplyChainChunker()

        # Create a cycle: 1 <- 2 <- 1 (msg1 references msg2)
        msg1 = make_message(1, content="original", referenced_message_id=2)
        msg2 = make_message(2, content="reply", referenced_message_id=1)
        lookup = {1: msg1, 2: msg2}

        # Should not infinite loop
        result = chunker.process_message(msg2, lookup, guild_id=1, channel_id=100)

        assert result is not None
        # Should stop when cycle detected
        assert len(result.message_ids) <= 2

    def test_empty_content_chain_returns_none(self) -> None:
        chunker = ReplyChainChunker()

        # Both messages have no content
        msg1 = make_message(1, content="")
        msg2 = make_message(2, content="", referenced_message_id=1)
        lookup = {1: msg1, 2: msg2}

        result = chunker.process_message(msg2, lookup, guild_id=1, channel_id=100)

        # Should return None when no content
        assert result is None

    def test_mentions_collected_from_chain(self) -> None:
        chunker = ReplyChainChunker()

        msg1 = make_message(1, content="original", mentions=[300])
        msg2 = make_message(
            2, content="reply", referenced_message_id=1, mention_roles=[10]
        )
        lookup = {1: msg1, 2: msg2}

        result = chunker.process_message(msg2, lookup, guild_id=1, channel_id=100)

        assert result is not None
        assert result.mentioned_user_ids == [300]
        assert result.mentioned_role_ids == [10]
        assert result.has_attachments is False

    def test_embedding_status_is_pending(self) -> None:
        chunker = ReplyChainChunker()

        msg1 = make_message(1, content="original")
        msg2 = make_message(2, content="reply", referenced_message_id=1)
        lookup = {1: msg1, 2: msg2}

        result = chunker.process_message(msg2, lookup, guild_id=1, channel_id=100)

        assert result is not None
        assert result.embedding_status == "pending"
