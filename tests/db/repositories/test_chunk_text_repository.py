"""Tests for discord_archive.db.repositories.chunk_text_repository."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from discord_archive.db.repositories.chunk_text_repository import (
    BATCH_SIZE,
    bulk_insert_chunk_texts,
)


def _make_session() -> AsyncMock:
    """Create a mock AsyncSession."""
    session = AsyncMock()
    return session


class TestBulkInsertChunkTexts:
    """Tests for bulk_insert_chunk_texts."""

    @pytest.mark.asyncio
    async def test_empty_input(self) -> None:
        """Empty input should not execute any queries."""
        session = _make_session()

        await bulk_insert_chunk_texts(session, [])

        session.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_single_chunk_text(self) -> None:
        """Single chunk text should execute one query."""
        session = _make_session()
        chunk_texts = [(1, "Hello world", 2)]

        await bulk_insert_chunk_texts(session, chunk_texts)

        session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_multiple_chunk_texts(self) -> None:
        """Multiple chunk texts within batch size should execute one query."""
        session = _make_session()
        chunk_texts = [
            (1, "Text 1", 2),
            (2, "Text 2", 3),
            (3, "Text 3", 4),
        ]

        await bulk_insert_chunk_texts(session, chunk_texts)

        session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_batching(self) -> None:
        """Inputs exceeding batch size should be split into multiple queries."""
        session = _make_session()
        # Create more than BATCH_SIZE entries
        chunk_texts = [(i, f"Text {i}", i % 100) for i in range(BATCH_SIZE + 10)]

        await bulk_insert_chunk_texts(session, chunk_texts)

        # Should have called execute twice (one full batch + one partial)
        assert session.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_exact_batch_size(self) -> None:
        """Exactly batch size entries should execute one query."""
        session = _make_session()
        chunk_texts = [(i, f"Text {i}", i % 100) for i in range(BATCH_SIZE)]

        await bulk_insert_chunk_texts(session, chunk_texts)

        session.execute.assert_called_once()


class TestBatchSize:
    """Tests for BATCH_SIZE constant."""

    def test_batch_size_reasonable(self) -> None:
        """Batch size should be reasonable for PostgreSQL parameter limits."""
        # 3 params per row, limit is ~32767
        # 500 * 3 = 1500, well under limit
        assert BATCH_SIZE == 500
        assert BATCH_SIZE * 3 < 32767
