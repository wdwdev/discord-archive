"""Tests for discord_archive.rag.chunking.author_group."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from discord_archive.db.models.message import Message
from discord_archive.rag.chunking.author_group import (
    AuthorGroupChunker,
    AuthorGroupConfig,
    AuthorGroupState,
)


def make_message(
    message_id: int,
    author_id: int = 1,
    content: str = "test",
    channel_id: int = 100,
    created_at: datetime | None = None,
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
        created_at=created_at or datetime.now(timezone.utc),
        type=msg_type,
        mentions=mentions or [],
        mention_roles=mention_roles or [],
    )


class TestAuthorGroupConfig:
    """Tests for AuthorGroupConfig."""

    def test_default_values(self) -> None:
        config = AuthorGroupConfig()
        assert config.gap_seconds == 60
        assert config.max_tokens == 1000

    def test_custom_values(self) -> None:
        config = AuthorGroupConfig(gap_seconds=120, max_tokens=500)
        assert config.gap_seconds == 120
        assert config.max_tokens == 500


class TestAuthorGroupState:
    """Tests for AuthorGroupState."""

    def test_initial_state_has_no_chunks(self) -> None:
        state = AuthorGroupState()
        assert len(state.open_chunks) == 0

    def test_get_author_chunk_returns_none_when_empty(self) -> None:
        state = AuthorGroupState()
        assert state.get_author_chunk(123) is None

    def test_set_and_get_author_chunk(self) -> None:
        from discord_archive.db.models.chunk import Chunk

        state = AuthorGroupState()
        now = datetime.now(timezone.utc)
        chunk = Chunk(
            chunk_type="author_group",
            guild_id=1,
            channel_id=100,
            message_ids=[1],
            author_ids=[123],
            chunk_state="open",
            start_message_id=1,
            embedding_status="pending",
            first_message_at=now,
            last_message_at=now,
        )
        state.set_author_chunk(123, chunk, [], 0, now)

        result = state.get_author_chunk(123)
        assert result is not None
        assert result[0] is chunk

    def test_remove_author_chunk(self) -> None:
        from discord_archive.db.models.chunk import Chunk

        state = AuthorGroupState()
        now = datetime.now(timezone.utc)
        chunk = Chunk(
            chunk_type="author_group",
            guild_id=1,
            channel_id=100,
            message_ids=[1],
            author_ids=[123],
            chunk_state="open",
            start_message_id=1,
            embedding_status="pending",
            first_message_at=now,
            last_message_at=now,
        )
        state.set_author_chunk(123, chunk, [], 0, now)

        state.remove_author_chunk(123)
        assert state.get_author_chunk(123) is None

    def test_remove_nonexistent_chunk_is_safe(self) -> None:
        state = AuthorGroupState()
        state.remove_author_chunk(999)  # Should not raise


class TestAuthorGroupChunker:
    """Tests for AuthorGroupChunker."""

    def test_create_empty_state(self) -> None:
        chunker = AuthorGroupChunker()
        state = chunker.create_empty_state()
        assert len(state.open_chunks) == 0

    def test_first_message_creates_open_chunk(self) -> None:
        chunker = AuthorGroupChunker()
        state = chunker.create_empty_state()
        msg = make_message(1, author_id=100, content="hello")

        state, chunks = chunker.process_message(
            state, msg, guild_id=1, channel_id=100
        )

        assert len(chunks) == 1
        assert chunks[0].chunk_type == "author_group"
        assert chunks[0].chunk_state == "open"
        assert chunks[0].message_ids == [1]
        assert chunks[0].author_ids == [100]
        assert chunks[0].first_message_at == msg.created_at
        assert chunks[0].last_message_at == msg.created_at

    def test_same_author_appends_to_chunk(self) -> None:
        chunker = AuthorGroupChunker()
        state = chunker.create_empty_state()
        now = datetime.now(timezone.utc)

        msg1 = make_message(1, author_id=100, content="hello", created_at=now)
        msg2 = make_message(
            2,
            author_id=100,
            content="world",
            created_at=now + timedelta(seconds=30),
        )

        state, _ = chunker.process_message(
            state, msg1, guild_id=1, channel_id=100
        )
        state, chunks = chunker.process_message(
            state, msg2, guild_id=1, channel_id=100
        )

        assert len(chunks) == 1
        assert chunks[0].message_ids == [1, 2]
        assert chunks[0].chunk_state == "open"
        assert chunks[0].first_message_at == msg1.created_at
        assert chunks[0].last_message_at == msg2.created_at

    def test_different_authors_have_separate_chunks(self) -> None:
        chunker = AuthorGroupChunker()
        state = chunker.create_empty_state()

        msg1 = make_message(1, author_id=100, content="hello")
        msg2 = make_message(2, author_id=200, content="world")

        state, chunks1 = chunker.process_message(
            state, msg1, guild_id=1, channel_id=100
        )
        state, chunks2 = chunker.process_message(
            state, msg2, guild_id=1, channel_id=100
        )

        # Each author has their own open chunk
        assert len(chunks1) == 1
        assert chunks1[0].author_ids == [100]
        assert len(chunks2) == 1
        assert chunks2[0].author_ids == [200]

        # Both chunks should be tracked
        assert state.get_author_chunk(100) is not None
        assert state.get_author_chunk(200) is not None

    def test_time_gap_closes_chunk(self) -> None:
        config = AuthorGroupConfig(gap_seconds=60)
        chunker = AuthorGroupChunker(config)
        state = chunker.create_empty_state()
        now = datetime.now(timezone.utc)

        msg1 = make_message(1, author_id=100, content="hello", created_at=now)
        msg2 = make_message(
            2,
            author_id=100,
            content="world",
            created_at=now + timedelta(seconds=120),  # > gap_seconds
        )

        state, _ = chunker.process_message(
            state, msg1, guild_id=1, channel_id=100
        )
        state, chunks = chunker.process_message(
            state, msg2, guild_id=1, channel_id=100
        )

        # Should have closed chunk and new open chunk
        assert len(chunks) == 2
        assert chunks[0].chunk_state == "closed"
        assert chunks[0].message_ids == [1]
        assert chunks[1].chunk_state == "open"
        assert chunks[1].message_ids == [2]

    def test_token_limit_closes_chunk(self) -> None:
        config = AuthorGroupConfig(max_tokens=5)
        chunker = AuthorGroupChunker(config)
        state = chunker.create_empty_state()
        now = datetime.now(timezone.utc)

        msg1 = make_message(
            1,
            author_id=100,
            content="hello world test message",  # Many tokens
            created_at=now,
        )
        msg2 = make_message(
            2,
            author_id=100,
            content="another long message here",
            created_at=now + timedelta(seconds=10),
        )

        state, _ = chunker.process_message(
            state, msg1, guild_id=1, channel_id=100
        )
        state, chunks = chunker.process_message(
            state, msg2, guild_id=1, channel_id=100
        )

        # Should have closed first chunk due to token limit
        assert len(chunks) == 2
        assert chunks[0].chunk_state == "closed"
        assert chunks[1].chunk_state == "open"

    def test_mentions_aggregated_on_append(self) -> None:
        chunker = AuthorGroupChunker()
        state = chunker.create_empty_state()
        now = datetime.now(timezone.utc)

        msg1 = make_message(
            1, author_id=100, content="hi", created_at=now, mentions=[500]
        )
        msg2 = make_message(
            2,
            author_id=100,
            content="bye",
            created_at=now + timedelta(seconds=10),
            mentions=[600],
            mention_roles=[10],
        )

        state, _ = chunker.process_message(state, msg1, guild_id=1, channel_id=100)
        state, chunks = chunker.process_message(state, msg2, guild_id=1, channel_id=100)

        assert chunks[0].mentioned_user_ids == sorted({500, 600})
        assert chunks[0].mentioned_role_ids == [10]

    def test_skips_thread_starter_messages(self) -> None:
        chunker = AuthorGroupChunker()
        state = chunker.create_empty_state()

        msg = make_message(1, author_id=100, content="thread start", msg_type=21)

        state, chunks = chunker.process_message(
            state, msg, guild_id=1, channel_id=100
        )

        assert len(chunks) == 0
        assert state.get_author_chunk(100) is None

    def test_load_state_from_existing_chunks(self) -> None:
        from discord_archive.db.models.chunk import Chunk

        chunker = AuthorGroupChunker()

        messages = [
            make_message(1, author_id=100, content="hello"),
            make_message(2, author_id=100, content="world"),
        ]
        chunk = Chunk(
            chunk_type="author_group",
            guild_id=1,
            channel_id=100,
            message_ids=[1, 2],
            author_ids=[100],
            chunk_state="open",
            start_message_id=1,
            embedding_status="pending",
            first_message_at=messages[0].created_at,
            last_message_at=messages[-1].created_at,
        )

        state = chunker.load_state(
            chunks={100: chunk},
            messages_by_author={100: messages},
        )

        result = state.get_author_chunk(100)
        assert result is not None
        assert result[0] is chunk
        assert len(result[1]) == 2
