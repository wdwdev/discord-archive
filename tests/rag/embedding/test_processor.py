"""Tests for discord_archive.rag.embedding.processor."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest
import torch

from discord_archive.rag.embedding.processor import (
    EmbeddingConfig,
    EmbeddingProcessor,
    EmbeddingStats,
    _PendingChunkRow,
)


def _make_row(
    chunk_id: int = 1,
    token_count: int = 100,
    text: str = "test text",
) -> _PendingChunkRow:
    """Create a test pending chunk row."""
    return _PendingChunkRow(
        chunk_id=chunk_id,
        text=text,
        token_count=token_count,
        guild_id=100,
        channel_id=200,
        author_ids=[1],
        mentioned_user_ids=[],
        mentioned_role_ids=[],
        has_attachments=False,
        first_message_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        last_message_at=datetime(2024, 1, 2, tzinfo=timezone.utc),
    )


class TestEmbeddingConfig:
    """Tests for EmbeddingConfig."""

    def test_default_values(self) -> None:
        config = EmbeddingConfig.default()
        assert config.db_batch_size == 1000
        assert config.token_budget == 8_000
        assert config.max_batch_size == 32
        assert config.lancedb_data_dir == "data/lancedb"
        assert config.model.model_name == "nvidia/NV-Embed-v2"


class TestEmbeddingStats:
    """Tests for EmbeddingStats."""

    def test_default_values(self) -> None:
        stats = EmbeddingStats()
        assert stats.chunks_processed == 0
        assert stats.chunks_skipped == 0


class TestTakeBatch:
    """Tests for EmbeddingProcessor._take_batch."""

    def test_takes_first_batch(self) -> None:
        config = EmbeddingConfig.default()
        config.token_budget = 500
        config.max_batch_size = 100
        processor = EmbeddingProcessor(config)

        rows = [_make_row(chunk_id=i, token_count=100) for i in range(6)]
        batch, remaining = processor._take_batch(rows)
        assert len(batch) == 5  # 5*100=500 <= 500
        assert len(remaining) == 1

    def test_respects_max_batch_size(self) -> None:
        config = EmbeddingConfig.default()
        config.token_budget = 100_000
        config.max_batch_size = 4
        processor = EmbeddingProcessor(config)

        rows = [_make_row(chunk_id=i, token_count=10) for i in range(10)]
        batch, remaining = processor._take_batch(rows)
        assert len(batch) == 4
        assert len(remaining) == 6

    def test_override_max_batch_size(self) -> None:
        config = EmbeddingConfig.default()
        config.token_budget = 100_000
        config.max_batch_size = 100
        processor = EmbeddingProcessor(config)

        rows = [_make_row(chunk_id=i, token_count=10) for i in range(10)]
        batch, remaining = processor._take_batch(rows, max_batch_size=3)
        assert len(batch) == 3
        assert len(remaining) == 7

    def test_override_token_budget(self) -> None:
        config = EmbeddingConfig.default()
        config.token_budget = 100_000
        config.max_batch_size = 100
        processor = EmbeddingProcessor(config)

        # 10 rows of 100 tokens, budget overridden to 300
        rows = [_make_row(chunk_id=i, token_count=100) for i in range(10)]
        batch, remaining = processor._take_batch(rows, token_budget=300)
        assert len(batch) == 3  # 3*100=300 <= 300
        assert len(remaining) == 7

    def test_single_large_chunk(self) -> None:
        config = EmbeddingConfig.default()
        config.token_budget = 500
        processor = EmbeddingProcessor(config)

        rows = [_make_row(chunk_id=1, token_count=1000)]
        batch, remaining = processor._take_batch(rows)
        assert len(batch) == 1
        assert remaining == []

    def test_empty_input(self) -> None:
        config = EmbeddingConfig.default()
        processor = EmbeddingProcessor(config)
        batch, remaining = processor._take_batch([])
        assert batch == []
        assert remaining == []


class TestTakeBatchPacking:
    """Tests for _take_batch packing behavior across multiple calls."""

    def test_respects_token_budget(self) -> None:
        config = EmbeddingConfig.default()
        config.token_budget = 500
        config.max_batch_size = 100
        processor = EmbeddingProcessor(config)

        # 5 rows of 100 tokens each: 5 * 100 = 500, fits in one batch
        rows = [_make_row(chunk_id=i, token_count=100) for i in range(5)]
        batch, remaining = processor._take_batch(rows)
        assert len(batch) == 5
        assert remaining == []

    def test_splits_when_exceeding_budget(self) -> None:
        config = EmbeddingConfig.default()
        config.token_budget = 500
        config.max_batch_size = 100
        processor = EmbeddingProcessor(config)

        # 6 rows of 100 tokens: first batch 5, then 1 remaining
        rows = [_make_row(chunk_id=i, token_count=100) for i in range(6)]
        batch, remaining = processor._take_batch(rows)
        assert len(batch) == 5
        assert len(remaining) == 1

    def test_single_large_chunk(self) -> None:
        config = EmbeddingConfig.default()
        config.token_budget = 500
        processor = EmbeddingProcessor(config)

        rows = [_make_row(chunk_id=1, token_count=1000)]
        batch, remaining = processor._take_batch(rows)
        assert len(batch) == 1
        assert remaining == []

    def test_sorted_input_packs_efficiently(self) -> None:
        config = EmbeddingConfig.default()
        config.token_budget = 1000
        processor = EmbeddingProcessor(config)

        # Pre-sorted ascending: [50, 100, 200, 500]
        rows = [
            _make_row(chunk_id=1, token_count=50),
            _make_row(chunk_id=2, token_count=100),
            _make_row(chunk_id=3, token_count=200),
            _make_row(chunk_id=4, token_count=500),
        ]
        # Adding row1: 1*50=50 <= 1000 ok
        # Adding row2: 2*100=200 <= 1000 ok
        # Adding row3: 3*200=600 <= 1000 ok
        # Adding row4: 4*500=2000 > 1000, split
        batch, remaining = processor._take_batch(rows)
        assert len(batch) == 3
        assert len(remaining) == 1


class TestBuildArrowTable:
    """Tests for EmbeddingProcessor._build_arrow_table."""

    def test_build_arrow_table(self) -> None:
        rows = [
            _make_row(chunk_id=1),
            _make_row(chunk_id=2),
        ]
        vectors = np.array([[0.1] * 4096, [0.2] * 4096], dtype=np.float32)

        table = EmbeddingProcessor._build_arrow_table(rows, vectors)

        assert table.num_rows == 2
        assert table.column("chunk_id").to_pylist() == [1, 2]
        assert len(table.column("vector")[0].as_py()) == 4096
        assert table.column("guild_id").to_pylist() == [100, 100]
        assert table.column("channel_id").to_pylist() == [200, 200]
        assert table.column("has_attachments").to_pylist() == [False, False]

    def test_build_arrow_table_preserves_metadata(self) -> None:
        row = _PendingChunkRow(
            chunk_id=42,
            text="test",
            token_count=10,
            guild_id=111,
            channel_id=222,
            author_ids=[1, 2, 3],
            mentioned_user_ids=[4, 5],
            mentioned_role_ids=[6],
            has_attachments=True,
            first_message_at=datetime(2024, 6, 1, tzinfo=timezone.utc),
            last_message_at=datetime(2024, 6, 15, tzinfo=timezone.utc),
        )
        vectors = np.array([[0.5] * 4096], dtype=np.float32)

        table = EmbeddingProcessor._build_arrow_table([row], vectors)

        assert table.column("chunk_id")[0].as_py() == 42
        assert table.column("guild_id")[0].as_py() == 111
        assert table.column("channel_id")[0].as_py() == 222
        assert table.column("author_ids")[0].as_py() == [1, 2, 3]
        assert table.column("mentioned_user_ids")[0].as_py() == [4, 5]
        assert table.column("mentioned_role_ids")[0].as_py() == [6]
        assert table.column("has_attachments")[0].as_py() is True


class TestFetchPendingChunks:
    """Tests for EmbeddingProcessor._fetch_pending_chunks using mock session."""

    @pytest.mark.asyncio
    async def test_fetch_pending_chunks_returns_rows(self) -> None:
        config = EmbeddingConfig.default()
        processor = EmbeddingProcessor(config)

        mock_row = MagicMock()
        mock_row.chunk_id = 1
        mock_row.text = "hello world"
        mock_row.token_count = 5
        mock_row.guild_id = 100
        mock_row.channel_id = 200
        mock_row.author_ids = [1]
        mock_row.mentioned_user_ids = []
        mock_row.mentioned_role_ids = []
        mock_row.has_attachments = False
        mock_row.first_message_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
        mock_row.last_message_at = datetime(2024, 1, 2, tzinfo=timezone.utc)

        mock_result = MagicMock()
        mock_result.all.return_value = [mock_row]

        session = AsyncMock()
        session.execute.return_value = mock_result

        rows = await processor._fetch_pending_chunks(session, 0, 0)

        assert len(rows) == 1
        assert rows[0].chunk_id == 1
        assert rows[0].text == "hello world"
        assert rows[0].token_count == 5

    @pytest.mark.asyncio
    async def test_fetch_pending_chunks_empty(self) -> None:
        config = EmbeddingConfig.default()
        processor = EmbeddingProcessor(config)

        mock_result = MagicMock()
        mock_result.all.return_value = []

        session = AsyncMock()
        session.execute.return_value = mock_result

        rows = await processor._fetch_pending_chunks(session, 0, 0)
        assert rows == []


class TestAdaptiveBatching:
    """Tests for adaptive OOM handling in process."""

    @pytest.mark.asyncio
    async def test_oom_reduces_budget_for_remaining(self) -> None:
        """After OOM, token_budget is reduced so remaining batches are smaller."""
        config = EmbeddingConfig.default()
        config.token_budget = 400
        config.max_batch_size = 100
        processor = EmbeddingProcessor(config)

        # 8 rows of 100 tokens. Budget=400 → batch of 4.
        # OOM on batch of 4 → new budget = (4//2)*100 = 200 → batch of 2.
        rows = [_make_row(chunk_id=i, token_count=100) for i in range(8)]
        vec2 = np.array([[0.1] * 4096, [0.2] * 4096], dtype=np.float32)

        model = MagicMock()
        model.encode_documents.side_effect = [
            torch.cuda.OutOfMemoryError("CUDA OOM"),  # batch of 4
            vec2,  # batch of 2
            vec2,  # batch of 2
            vec2,  # batch of 2
            vec2,  # batch of 2
        ]
        lancedb_store = MagicMock()
        session = AsyncMock()

        processor._fetch_pending_chunks = AsyncMock(
            side_effect=[rows, []]
        )

        with patch("torch.cuda.empty_cache"):
            result = await processor.process(
                session, model, lancedb_store,
            )

        assert result.chunks_processed == 8
        assert result.chunks_skipped == 0
        # 1 failed + 4 successful encode calls
        assert model.encode_documents.call_count == 5
        lancedb_store.add.assert_called_once()

    @pytest.mark.asyncio
    async def test_oom_budget_allows_more_small_chunks(self) -> None:
        """Reduced budget still allows large batches for small tokens."""
        config = EmbeddingConfig.default()
        config.token_budget = 1000
        config.max_batch_size = 100
        processor = EmbeddingProcessor(config)

        # Mixed tokens: 4 small (10 tok) + 4 large (200 tok), sorted ascending.
        # Budget=1000, max_batch=100.
        # First batch: all 4 small + first large → [10,10,10,10,200]
        #   5*200=1000 <= 1000, fits.
        #   Adding next 200: 6*200=1200 > 1000, split.
        # So batch 1 = 5 items, OOM.
        # New budget = (5//2)*200 = 400.
        # Re-batch: [10,10,10,10,200,200,200] with budget=400
        #   Batch: [10,10,10,10] → 4*10=40 <= 400, try next
        #   Adding 200: 5*200=1000 > 400, split.
        # batch 2 = 4 small items, succeeds.
        # Remaining: [200,200,200], budget=400
        #   Batch: [200] → 1*200=200 <= 400, [200,200] → 2*200=400 <= 400,
        #   [200,200,200] → 3*200=600 > 400, split.
        # batch 3 = 2 items (200 tok), succeeds.
        # batch 4 = 1 item (200 tok), succeeds.
        rows = (
            [_make_row(chunk_id=i, token_count=10) for i in range(4)]
            + [_make_row(chunk_id=i + 4, token_count=200) for i in range(3)]
        )
        vec4 = np.array([[0.1] * 4096] * 4, dtype=np.float32)
        vec2 = np.array([[0.1] * 4096] * 2, dtype=np.float32)
        vec1 = np.array([[0.1] * 4096], dtype=np.float32)

        model = MagicMock()
        model.encode_documents.side_effect = [
            torch.cuda.OutOfMemoryError("OOM"),  # batch of 5
            vec4,  # batch of 4 (small tokens)
            vec2,  # batch of 2 (large tokens)
            vec1,  # batch of 1
        ]
        lancedb_store = MagicMock()
        session = AsyncMock()

        processor._fetch_pending_chunks = AsyncMock(
            side_effect=[rows, []]
        )

        with patch("torch.cuda.empty_cache"):
            result = await processor.process(
                session, model, lancedb_store,
            )

        assert result.chunks_processed == 7
        assert result.chunks_skipped == 0
        # Small tokens got batch of 4 (budget=400, 4*10=40 fits)
        # Large tokens got batch of 2 (budget=400, 2*200=400 fits)
        assert model.encode_documents.call_count == 4

    @pytest.mark.asyncio
    async def test_oom_single_chunk_skipped(self) -> None:
        """A single chunk that OOMs is skipped."""
        config = EmbeddingConfig.default()
        config.token_budget = 100
        config.max_batch_size = 1
        processor = EmbeddingProcessor(config)

        rows = [_make_row(chunk_id=99, token_count=50000)]

        model = MagicMock()
        model.encode_documents.side_effect = torch.cuda.OutOfMemoryError(
            "CUDA OOM"
        )
        lancedb_store = MagicMock()
        session = AsyncMock()

        processor._fetch_pending_chunks = AsyncMock(
            side_effect=[rows, []]
        )

        with patch("torch.cuda.empty_cache"):
            result = await processor.process(
                session, model, lancedb_store,
            )

        assert result.chunks_processed == 0
        assert result.chunks_skipped == 1
        lancedb_store.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_oom_processes_normally(self) -> None:
        """Without OOM, batches process normally."""
        config = EmbeddingConfig.default()
        config.token_budget = 200
        config.max_batch_size = 2
        processor = EmbeddingProcessor(config)

        rows = [_make_row(chunk_id=i, token_count=100) for i in range(4)]
        vec2 = np.array([[0.1] * 4096, [0.2] * 4096], dtype=np.float32)

        model = MagicMock()
        model.encode_documents.return_value = vec2
        lancedb_store = MagicMock()
        session = AsyncMock()
        callback = MagicMock()

        processor._fetch_pending_chunks = AsyncMock(
            side_effect=[rows, []]
        )

        result = await processor.process(
            session, model, lancedb_store, progress_callback=callback,
        )

        assert result.chunks_processed == 4
        assert result.tokens_processed == 400
        assert model.encode_documents.call_count == 2
        lancedb_store.add.assert_called_once()
        assert callback.call_count == 2
        # Callback reports cumulative tokens (100 tok/chunk × 2 chunks/batch)
        callback.assert_any_call(200)
        callback.assert_any_call(400)

    @pytest.mark.asyncio
    async def test_oom_cascading_reduction(self) -> None:
        """OOM at reduced budget halves again."""
        config = EmbeddingConfig.default()
        config.token_budget = 800
        config.max_batch_size = 100
        processor = EmbeddingProcessor(config)

        # 8 rows of 100 tokens. Budget=800 → batch of 8.
        # OOM → new budget = (8//2)*100 = 400 → batch of 4.
        # OOM → new budget = (4//2)*100 = 200 → batch of 2.
        rows = [_make_row(chunk_id=i, token_count=100) for i in range(8)]
        vec2 = np.array([[0.1] * 4096, [0.2] * 4096], dtype=np.float32)

        model = MagicMock()
        model.encode_documents.side_effect = [
            torch.cuda.OutOfMemoryError("OOM"),  # batch of 8
            torch.cuda.OutOfMemoryError("OOM"),  # batch of 4
            vec2,  # batch of 2
            vec2,  # batch of 2
            vec2,  # batch of 2
            vec2,  # batch of 2
        ]
        lancedb_store = MagicMock()
        session = AsyncMock()

        processor._fetch_pending_chunks = AsyncMock(
            side_effect=[rows, []]
        )

        with patch("torch.cuda.empty_cache"):
            result = await processor.process(
                session, model, lancedb_store,
            )

        assert result.chunks_processed == 8
        assert result.chunks_skipped == 0
        # 2 failed + 4 successful
        assert model.encode_documents.call_count == 6
        lancedb_store.add.assert_called_once()


class TestGracefulShutdown:
    """Tests for Ctrl+C graceful shutdown in process."""

    @pytest.mark.asyncio
    async def test_interrupt_saves_in_flight_flush(self) -> None:
        """KeyboardInterrupt during encode finalizes the previous LanceDB write."""
        config = EmbeddingConfig.default()
        config.token_budget = 200
        config.max_batch_size = 2
        config.db_batch_size = 2
        processor = EmbeddingProcessor(config)

        batch1 = [_make_row(chunk_id=i, token_count=100) for i in range(2)]
        batch2 = [_make_row(chunk_id=i, token_count=100) for i in range(2, 4)]
        vec2 = np.array([[0.1] * 4096, [0.2] * 4096], dtype=np.float32)

        model = MagicMock()
        model.encode_documents.side_effect = [
            vec2,                          # batch1 succeeds
            KeyboardInterrupt,             # batch2 interrupted during encode
        ]
        lancedb_store = MagicMock()
        session = AsyncMock()

        processor._fetch_pending_chunks = AsyncMock(
            side_effect=[batch1, batch2, []]
        )

        with pytest.raises(KeyboardInterrupt):
            await processor.process(
                session, model, lancedb_store,
            )

        # batch1 was submitted to LanceDB and should be marked embedded
        lancedb_store.add.assert_called_once()
        session.execute.assert_called()
        session.commit.assert_called()

    @pytest.mark.asyncio
    async def test_interrupt_saves_accumulated_rows(self) -> None:
        """KeyboardInterrupt flushes rows encoded in the current DB batch."""
        config = EmbeddingConfig.default()
        config.token_budget = 100
        config.max_batch_size = 1
        processor = EmbeddingProcessor(config)

        # 3 rows, budget allows 1 at a time.
        # encode row 0 → OK, encode row 1 → OK, encode row 2 → interrupt
        rows = [_make_row(chunk_id=i, token_count=100) for i in range(3)]
        vec1 = np.array([[0.1] * 4096], dtype=np.float32)

        model = MagicMock()
        model.encode_documents.side_effect = [
            vec1,                # row 0 succeeds
            vec1,                # row 1 succeeds
            KeyboardInterrupt,   # row 2 interrupted
        ]
        lancedb_store = MagicMock()
        session = AsyncMock()

        processor._fetch_pending_chunks = AsyncMock(
            side_effect=[rows, []]
        )

        with pytest.raises(KeyboardInterrupt):
            await processor.process(
                session, model, lancedb_store,
            )

        # The 2 successfully encoded rows should be flushed
        lancedb_store.add.assert_called_once()
        table = lancedb_store.add.call_args[0][0]
        assert table.num_rows == 2
        assert table.column("chunk_id").to_pylist() == [0, 1]

    @pytest.mark.asyncio
    async def test_interrupt_propagates(self) -> None:
        """KeyboardInterrupt is re-raised after saving."""
        config = EmbeddingConfig.default()
        config.token_budget = 200
        config.max_batch_size = 2
        processor = EmbeddingProcessor(config)

        rows = [_make_row(chunk_id=i, token_count=100) for i in range(2)]

        model = MagicMock()
        model.encode_documents.side_effect = KeyboardInterrupt
        lancedb_store = MagicMock()
        session = AsyncMock()

        processor._fetch_pending_chunks = AsyncMock(
            side_effect=[rows, []]
        )

        with pytest.raises(KeyboardInterrupt):
            await processor.process(
                session, model, lancedb_store,
            )
