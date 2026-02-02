"""Unit tests for discord_archive.ingest.incremental."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from discord_archive.ingest.incremental import IncrementalResult, incremental_channel

PATCH_BASE = "discord_archive.ingest.incremental"


def _make_msg(msg_id: int) -> dict:
    """Build a minimal message dict."""
    return {"id": str(msg_id)}


def _make_checkpoint(
    oldest_message_id: int | None = None,
    newest_message_id: int | None = None,
    backfill_complete: bool = True,
) -> MagicMock:
    cp = MagicMock()
    cp.oldest_message_id = oldest_message_id
    cp.newest_message_id = newest_message_id
    cp.backfill_complete = backfill_complete
    return cp


@pytest.fixture
def mock_client():
    return AsyncMock()


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
        m.return_value = datetime(2024, 6, 1, tzinfo=timezone.utc)
        yield m


@pytest.fixture
def mock_logger():
    with patch(f"{PATCH_BASE}.logger") as m:
        yield m


# ---------------------------------------------------------------------------
# TestIncrementalChannel
# ---------------------------------------------------------------------------


class TestIncrementalChannel:
    """Tests for incremental_channel."""

    @pytest.mark.asyncio
    async def test_no_checkpoint_returns_zero(
        self, mock_client, mock_session, mock_state, mock_persist, mock_snowflake, mock_logger
    ):
        mock_state.get_checkpoint.return_value = None

        result = await incremental_channel(mock_client, mock_session, 100, 1)

        assert result.messages_count == 0
        assert result.is_caught_up is False
        mock_client.get_messages.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_checkpoint_with_newest_none_returns_zero(
        self, mock_client, mock_session, mock_state, mock_persist, mock_snowflake, mock_logger
    ):
        cp = _make_checkpoint(newest_message_id=None)
        mock_state.get_checkpoint.return_value = cp

        result = await incremental_channel(mock_client, mock_session, 100, 1)

        assert result.messages_count == 0
        assert result.is_caught_up is False

    @pytest.mark.asyncio
    async def test_empty_fetch_returns_caught_up(
        self, mock_client, mock_session, mock_state, mock_persist, mock_snowflake, mock_logger
    ):
        cp = _make_checkpoint(newest_message_id=500)
        mock_state.get_checkpoint.return_value = cp
        mock_client.get_messages.return_value = []

        result = await incremental_channel(mock_client, mock_session, 100, 1)

        assert result.messages_count == 0
        assert result.is_caught_up is True

    @pytest.mark.asyncio
    async def test_partial_batch(
        self, mock_client, mock_session, mock_state, mock_persist, mock_snowflake, mock_logger
    ):
        cp = _make_checkpoint(newest_message_id=500)
        mock_state.get_checkpoint.return_value = cp
        mock_client.get_messages.return_value = [_make_msg(501), _make_msg(502)]
        mock_persist.return_value = 2

        result = await incremental_channel(mock_client, mock_session, 100, 1)

        assert result.messages_count == 2
        assert result.is_caught_up is True

    @pytest.mark.asyncio
    async def test_correct_after_progression(
        self, mock_client, mock_session, mock_state, mock_persist, mock_snowflake, mock_logger
    ):
        cp = _make_checkpoint(newest_message_id=500)
        mock_state.get_checkpoint.return_value = cp
        batch1 = [_make_msg(i) for i in range(501, 601)]  # 100 messages, max=600
        batch2 = [_make_msg(i) for i in range(601, 621)]  # 20 messages, partial
        mock_client.get_messages.side_effect = [batch1, batch2]
        mock_persist.return_value = 20

        await incremental_channel(mock_client, mock_session, 100, 1)

        calls = mock_client.get_messages.call_args_list
        # First call: after=500 (from checkpoint)
        assert calls[0].kwargs["after"] == 500
        # Second call: after=600 (newest from first batch)
        assert calls[1].kwargs["after"] == 600

    @pytest.mark.asyncio
    async def test_updates_newest_each_batch(
        self, mock_client, mock_session, mock_state, mock_persist, mock_snowflake, mock_logger
    ):
        cp = _make_checkpoint(newest_message_id=500)
        mock_state.get_checkpoint.return_value = cp
        batch1 = [_make_msg(i) for i in range(501, 601)]
        batch2 = [_make_msg(i) for i in range(601, 621)]  # partial
        mock_client.get_messages.side_effect = [batch1, batch2]
        mock_persist.return_value = 20

        await incremental_channel(mock_client, mock_session, 100, 1)

        newest_calls = mock_state.update_newest.call_args_list
        assert len(newest_calls) == 2
        assert newest_calls[0].kwargs["message_id"] == 600
        assert newest_calls[1].kwargs["message_id"] == 620

    @pytest.mark.asyncio
    async def test_commits_each_batch(
        self, mock_client, mock_session, mock_state, mock_persist, mock_snowflake, mock_logger
    ):
        cp = _make_checkpoint(newest_message_id=500)
        mock_state.get_checkpoint.return_value = cp
        batch1 = [_make_msg(i) for i in range(501, 601)]
        batch2 = [_make_msg(i) for i in range(601, 621)]  # partial
        mock_client.get_messages.side_effect = [batch1, batch2]
        mock_persist.return_value = 20

        await incremental_channel(mock_client, mock_session, 100, 1)

        assert mock_session.commit.await_count == 2

    @pytest.mark.asyncio
    async def test_accumulates_total_count(
        self, mock_client, mock_session, mock_state, mock_persist, mock_snowflake, mock_logger
    ):
        cp = _make_checkpoint(newest_message_id=500)
        mock_state.get_checkpoint.return_value = cp
        batch1 = [_make_msg(i) for i in range(501, 601)]
        batch2 = [_make_msg(i) for i in range(601, 621)]  # partial
        mock_client.get_messages.side_effect = [batch1, batch2]
        mock_persist.side_effect = [100, 20]

        result = await incremental_channel(mock_client, mock_session, 100, 1)

        assert result.messages_count == 120


# ---------------------------------------------------------------------------
# TestIncrementalResult
# ---------------------------------------------------------------------------


class TestIncrementalResult:
    """Tests for IncrementalResult dataclass."""

    def test_fields(self):
        r = IncrementalResult(messages_count=15, is_caught_up=True)
        assert r.messages_count == 15
        assert r.is_caught_up is True
