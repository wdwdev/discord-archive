"""Unit tests for discord_archive.ingest.backfill."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from discord_archive.ingest.backfill import BackfillResult, backfill_channel

PATCH_BASE = "discord_archive.ingest.backfill"


def _make_msg(msg_id: int) -> dict:
    """Build a minimal message dict."""
    return {"id": str(msg_id)}


def _make_checkpoint(
    oldest_message_id: int | None = None,
    newest_message_id: int | None = None,
    backfill_complete: bool = False,
) -> MagicMock:
    cp = MagicMock()
    cp.oldest_message_id = oldest_message_id
    cp.newest_message_id = newest_message_id
    cp.backfill_complete = backfill_complete
    return cp


@pytest.fixture
def mock_client():
    client = AsyncMock()
    return client


@pytest.fixture
def mock_session():
    return AsyncMock()


@pytest.fixture
def mock_state():
    with patch(f"{PATCH_BASE}.IngestStateManager") as cls:
        state = AsyncMock()
        cls.return_value = state
        yield state


@pytest.fixture
def mock_persist():
    with patch(f"{PATCH_BASE}.persist_messages_batch", new_callable=AsyncMock) as m:
        yield m


@pytest.fixture
def mock_snowflake():
    with patch(f"{PATCH_BASE}.snowflake_to_datetime") as m:
        m.return_value = datetime(2023, 1, 15, tzinfo=timezone.utc)
        yield m


@pytest.fixture
def mock_logger():
    with patch(f"{PATCH_BASE}.logger") as m:
        yield m


# ---------------------------------------------------------------------------
# TestBackfillChannel
# ---------------------------------------------------------------------------


class TestBackfillChannel:
    """Tests for backfill_channel."""

    @pytest.mark.asyncio
    async def test_already_complete_skips(
        self, mock_client, mock_session, mock_state, mock_persist, mock_snowflake, mock_logger
    ):
        cp = _make_checkpoint(backfill_complete=True)
        mock_state.get_checkpoint.return_value = cp

        result = await backfill_channel(mock_client, mock_session, 100, 1)

        assert result.messages_count == 0
        assert result.is_complete is True
        mock_client.get_messages.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_empty_channel_marks_complete(
        self, mock_client, mock_session, mock_state, mock_persist, mock_snowflake, mock_logger
    ):
        mock_state.get_checkpoint.return_value = None
        mock_client.get_messages.return_value = []

        result = await backfill_channel(mock_client, mock_session, 100, 1)

        assert result.messages_count == 0
        assert result.is_complete is True
        mock_state.mark_backfill_complete.assert_awaited_once_with(100)

    @pytest.mark.asyncio
    async def test_partial_batch_marks_complete(
        self, mock_client, mock_session, mock_state, mock_persist, mock_snowflake, mock_logger
    ):
        mock_state.get_checkpoint.return_value = None
        # Return fewer than batch_size (100)
        mock_client.get_messages.return_value = [_make_msg(50), _make_msg(40)]
        mock_persist.return_value = 2

        result = await backfill_channel(mock_client, mock_session, 100, 1)

        assert result.messages_count == 2
        assert result.is_complete is True
        mock_state.mark_backfill_complete.assert_awaited()

    @pytest.mark.asyncio
    async def test_full_batch_then_empty_marks_complete(
        self, mock_client, mock_session, mock_state, mock_persist, mock_snowflake, mock_logger
    ):
        mock_state.get_checkpoint.return_value = None
        # First call returns 100 messages, second returns empty
        batch = [_make_msg(i) for i in range(200, 100, -1)]  # IDs 200..101
        mock_client.get_messages.side_effect = [batch, []]
        mock_persist.return_value = 100

        result = await backfill_channel(mock_client, mock_session, 100, 1)

        assert result.messages_count == 100
        assert result.is_complete is True

    @pytest.mark.asyncio
    async def test_correct_before_progression(
        self, mock_client, mock_session, mock_state, mock_persist, mock_snowflake, mock_logger
    ):
        mock_state.get_checkpoint.return_value = None
        batch1 = [_make_msg(i) for i in range(200, 100, -1)]  # oldest=101
        batch2 = [_make_msg(i) for i in range(100, 50, -1)]   # oldest=51, partial
        mock_client.get_messages.side_effect = [batch1, batch2]
        mock_persist.return_value = 50

        await backfill_channel(mock_client, mock_session, 100, 1)

        calls = mock_client.get_messages.call_args_list
        # First call: before=None (no checkpoint)
        assert calls[0].kwargs["before"] is None
        # Second call: before=101 (oldest from first batch)
        assert calls[1].kwargs["before"] == 101

    @pytest.mark.asyncio
    async def test_no_checkpoint_passes_newest_id_every_batch(
        self, mock_client, mock_session, mock_state, mock_persist, mock_snowflake, mock_logger
    ):
        """When checkpoint is None, newest_id is passed to update_bounds on every batch."""
        mock_state.get_checkpoint.return_value = None
        batch1 = [_make_msg(i) for i in range(200, 100, -1)]
        batch2 = [_make_msg(i) for i in range(100, 80, -1)]  # partial
        mock_client.get_messages.side_effect = [batch1, batch2]
        mock_persist.return_value = 20

        await backfill_channel(mock_client, mock_session, 100, 1)

        bounds_calls = mock_state.update_bounds.call_args_list
        # checkpoint=None → `not checkpoint` is True → newest_id is always passed
        assert bounds_calls[0].kwargs["newest_id"] == 200
        assert bounds_calls[1].kwargs["newest_id"] == 100

    @pytest.mark.asyncio
    async def test_existing_checkpoint_passes_none_newest_id(
        self, mock_client, mock_session, mock_state, mock_persist, mock_snowflake, mock_logger
    ):
        """When checkpoint exists, newest_id is None in update_bounds."""
        cp = _make_checkpoint(oldest_message_id=500, backfill_complete=False)
        mock_state.get_checkpoint.return_value = cp
        batch = [_make_msg(i) for i in range(499, 450, -1)]  # partial
        mock_client.get_messages.return_value = batch
        mock_persist.return_value = 49

        await backfill_channel(mock_client, mock_session, 100, 1)

        bounds_call = mock_state.update_bounds.call_args
        assert bounds_call.kwargs["newest_id"] is None

    @pytest.mark.asyncio
    async def test_existing_checkpoint_uses_oldest_as_before(
        self, mock_client, mock_session, mock_state, mock_persist, mock_snowflake, mock_logger
    ):
        cp = _make_checkpoint(oldest_message_id=500, backfill_complete=False)
        mock_state.get_checkpoint.return_value = cp
        mock_client.get_messages.return_value = []

        await backfill_channel(mock_client, mock_session, 100, 1)

        call_kwargs = mock_client.get_messages.call_args.kwargs
        assert call_kwargs["before"] == 500

    @pytest.mark.asyncio
    async def test_commits_after_each_batch(
        self, mock_client, mock_session, mock_state, mock_persist, mock_snowflake, mock_logger
    ):
        mock_state.get_checkpoint.return_value = None
        batch1 = [_make_msg(i) for i in range(200, 100, -1)]
        batch2 = [_make_msg(i) for i in range(100, 80, -1)]  # partial
        mock_client.get_messages.side_effect = [batch1, batch2]
        mock_persist.return_value = 20

        await backfill_channel(mock_client, mock_session, 100, 1)

        # Commit called after each batch: once after batch1 + once after batch2 (partial marks complete + commit)
        assert mock_session.commit.await_count >= 2

    @pytest.mark.asyncio
    async def test_accumulates_total_count(
        self, mock_client, mock_session, mock_state, mock_persist, mock_snowflake, mock_logger
    ):
        mock_state.get_checkpoint.return_value = None
        batch1 = [_make_msg(i) for i in range(200, 100, -1)]
        batch2 = [_make_msg(i) for i in range(100, 80, -1)]  # partial
        mock_client.get_messages.side_effect = [batch1, batch2]
        mock_persist.side_effect = [100, 20]

        result = await backfill_channel(mock_client, mock_session, 100, 1)

        assert result.messages_count == 120


# ---------------------------------------------------------------------------
# TestBackfillResult
# ---------------------------------------------------------------------------


class TestBackfillResult:
    """Tests for BackfillResult dataclass."""

    def test_fields(self):
        r = BackfillResult(messages_count=42, is_complete=True)
        assert r.messages_count == 42
        assert r.is_complete is True
