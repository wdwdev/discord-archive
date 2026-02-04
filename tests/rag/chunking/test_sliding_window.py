"""Tests for discord_archive.rag.chunking.sliding_window."""

from __future__ import annotations

from datetime import datetime, timezone

from discord_archive.db.models.message import Message
from discord_archive.rag.chunking.sliding_window import (
    SlidingWindowChunker,
    SlidingWindowConfig,
    SlidingWindowState,
)


def make_message(
    message_id: int,
    author_id: int = 1,
    content: str = "test",
    channel_id: int = 100,
    msg_type: int = 0,
) -> Message:
    """Create a test message."""
    msg = Message(
        message_id=message_id,
        channel_id=channel_id,
        author_id=author_id,
        content=content,
        created_at=datetime.now(timezone.utc),
        type=msg_type,
    )
    return msg


class TestSlidingWindowConfig:
    """Tests for SlidingWindowConfig."""

    def test_default_values(self) -> None:
        config = SlidingWindowConfig()
        assert config.max_tokens == 500
        assert config.overlap_ratio == 0.25
        assert config.max_overlap_messages == 10

    def test_custom_values(self) -> None:
        config = SlidingWindowConfig(max_tokens=1000, overlap_ratio=0.5)
        assert config.max_tokens == 1000
        assert config.overlap_ratio == 0.5


class TestSlidingWindowState:
    """Tests for SlidingWindowState."""

    def test_is_empty_when_no_chunk(self) -> None:
        state = SlidingWindowState()
        assert state.is_empty() is True

    def test_is_not_empty_when_chunk_exists(self) -> None:
        chunker = SlidingWindowChunker()
        msg = make_message(1)
        state, _ = chunker.process_message(
            chunker.create_empty_state(), msg, guild_id=1, channel_id=100
        )
        assert state.is_empty() is False


class TestSlidingWindowChunker:
    """Tests for SlidingWindowChunker."""

    def test_create_empty_state(self) -> None:
        chunker = SlidingWindowChunker()
        state = chunker.create_empty_state()
        assert state.is_empty() is True
        assert state.messages == []
        assert state.total_tokens == 0

    def test_first_message_creates_open_chunk(self) -> None:
        chunker = SlidingWindowChunker()
        state = chunker.create_empty_state()
        msg = make_message(1, content="hello world")

        state, chunks = chunker.process_message(
            state, msg, guild_id=1, channel_id=100
        )

        assert len(chunks) == 1
        assert chunks[0].chunk_type == "sliding_window"
        assert chunks[0].chunk_state == "open"
        assert chunks[0].message_ids == [1]
        assert chunks[0].author_ids == [1]
        assert chunks[0].start_message_id == 1
        assert chunks[0].embedding_status == "pending"

    def test_appends_to_existing_window(self) -> None:
        chunker = SlidingWindowChunker()
        state = chunker.create_empty_state()

        msg1 = make_message(1, content="hello")
        msg2 = make_message(2, content="world")

        state, _ = chunker.process_message(
            state, msg1, guild_id=1, channel_id=100
        )
        state, chunks = chunker.process_message(
            state, msg2, guild_id=1, channel_id=100
        )

        assert len(chunks) == 1
        assert chunks[0].message_ids == [1, 2]
        assert chunks[0].chunk_state == "open"

    def test_closes_window_and_creates_new_when_exceeds_tokens(self) -> None:
        # Very small max_tokens to trigger closure
        config = SlidingWindowConfig(max_tokens=5)
        chunker = SlidingWindowChunker(config)
        state = chunker.create_empty_state()

        msg1 = make_message(1, content="hello world test")  # ~4 tokens
        msg2 = make_message(2, content="another long message")  # ~5 tokens

        state, chunks1 = chunker.process_message(
            state, msg1, guild_id=1, channel_id=100
        )
        state, chunks2 = chunker.process_message(
            state, msg2, guild_id=1, channel_id=100
        )

        # First message creates open chunk
        assert len(chunks1) == 1

        # Second message should close first chunk and create new one
        assert len(chunks2) == 2
        assert chunks2[0].chunk_state == "closed"  # Old chunk closed
        assert chunks2[1].chunk_state == "open"  # New chunk open

    def test_skips_thread_starter_messages(self) -> None:
        chunker = SlidingWindowChunker()
        state = chunker.create_empty_state()

        # Type 21 is thread starter
        msg = make_message(1, content="thread start", msg_type=21)

        state, chunks = chunker.process_message(
            state, msg, guild_id=1, channel_id=100
        )

        assert len(chunks) == 0
        assert state.is_empty() is True

    def test_compute_overlap(self) -> None:
        chunker = SlidingWindowChunker(SlidingWindowConfig(overlap_ratio=0.25))

        messages = [make_message(i) for i in range(10)]
        overlap = chunker._compute_overlap(messages)

        # 10 * 0.25 = 2.5, floor = 2
        assert len(overlap) == 2
        assert overlap[0].message_id == 8
        assert overlap[1].message_id == 9

    def test_compute_overlap_respects_max(self) -> None:
        chunker = SlidingWindowChunker(
            SlidingWindowConfig(overlap_ratio=0.5, max_overlap_messages=2)
        )

        messages = [make_message(i) for i in range(10)]
        overlap = chunker._compute_overlap(messages)

        # Would be 5, but capped at 2
        assert len(overlap) == 2

    def test_compute_overlap_minimum_one(self) -> None:
        chunker = SlidingWindowChunker(SlidingWindowConfig(overlap_ratio=0.01))

        messages = [make_message(i) for i in range(10)]
        overlap = chunker._compute_overlap(messages)

        # Even with tiny ratio, at least 1 message
        assert len(overlap) == 1

    def test_compute_overlap_empty_list(self) -> None:
        chunker = SlidingWindowChunker()
        overlap = chunker._compute_overlap([])
        assert overlap == []

    def test_multiple_authors_tracked(self) -> None:
        chunker = SlidingWindowChunker()
        state = chunker.create_empty_state()

        msg1 = make_message(1, author_id=100, content="hello")
        msg2 = make_message(2, author_id=200, content="world")

        state, _ = chunker.process_message(
            state, msg1, guild_id=1, channel_id=100
        )
        state, chunks = chunker.process_message(
            state, msg2, guild_id=1, channel_id=100
        )

        # Should have both authors
        assert set(chunks[0].author_ids) == {100, 200}

    def test_load_state_from_existing_chunk(self) -> None:
        from discord_archive.db.models.chunk import Chunk

        chunker = SlidingWindowChunker()

        chunk = Chunk(
            chunk_type="sliding_window",
            guild_id=1,
            channel_id=100,
            message_ids=[1, 2],
            author_ids=[100],
            chunk_state="open",
            start_message_id=1,
            embedding_status="pending",
        )
        messages = [
            make_message(1, content="hello"),
            make_message(2, content="world"),
        ]

        state = chunker.load_state(chunk, messages)

        assert state.chunk is chunk
        assert len(state.messages) == 2
        assert state.total_tokens > 0
