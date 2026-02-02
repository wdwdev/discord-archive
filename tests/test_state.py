"""Unit tests for discord_archive.ingest.state."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from discord_archive.db.models import IngestCheckpoint
from discord_archive.ingest.state import IngestStateManager


def _make_session() -> AsyncMock:
    """Create a mock AsyncSession."""
    session = AsyncMock()
    # session.add() is synchronous in SQLAlchemy
    session.add = MagicMock()
    return session


def _make_checkpoint(
    channel_id: int = 100,
    guild_id: int = 1,
    oldest_message_id: int | None = None,
    newest_message_id: int | None = None,
    backfill_complete: bool = False,
) -> MagicMock:
    """Build a MagicMock checkpoint with the given attributes."""
    cp = MagicMock(spec=IngestCheckpoint)
    cp.channel_id = channel_id
    cp.guild_id = guild_id
    cp.oldest_message_id = oldest_message_id
    cp.newest_message_id = newest_message_id
    cp.backfill_complete = backfill_complete
    cp.last_synced_at = datetime.now(timezone.utc)
    cp.created_at = datetime.now(timezone.utc)
    return cp


def _mock_scalar_result(value):
    """Configure session.execute to return a scalar_one_or_none result."""
    result = MagicMock()
    result.scalar_one_or_none.return_value = value
    return result


def _mock_scalars_result(values: list):
    """Configure session.execute to return a scalars().all() result."""
    result = MagicMock()
    scalars = MagicMock()
    scalars.all.return_value = values
    result.scalars.return_value = scalars
    return result


# ---------------------------------------------------------------------------
# TestGetCheckpoint
# ---------------------------------------------------------------------------


class TestGetCheckpoint:
    """Tests for IngestStateManager.get_checkpoint."""

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self):
        session = _make_session()
        session.execute.return_value = _mock_scalar_result(None)
        mgr = IngestStateManager(session)

        result = await mgr.get_checkpoint(100)

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_checkpoint_when_found(self):
        cp = _make_checkpoint(channel_id=100)
        session = _make_session()
        session.execute.return_value = _mock_scalar_result(cp)
        mgr = IngestStateManager(session)

        result = await mgr.get_checkpoint(100)

        assert result is cp


# ---------------------------------------------------------------------------
# TestCreateOrGetCheckpoint
# ---------------------------------------------------------------------------


class TestCreateOrGetCheckpoint:
    """Tests for IngestStateManager.create_or_get_checkpoint."""

    @pytest.mark.asyncio
    async def test_returns_existing(self):
        cp = _make_checkpoint(channel_id=100)
        session = _make_session()
        session.execute.return_value = _mock_scalar_result(cp)
        mgr = IngestStateManager(session)

        result = await mgr.create_or_get_checkpoint(100, 1)

        assert result is cp
        session.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_creates_new(self):
        session = _make_session()
        session.execute.return_value = _mock_scalar_result(None)
        mgr = IngestStateManager(session)

        result = await mgr.create_or_get_checkpoint(100, 1)

        assert result.channel_id == 100
        assert result.guild_id == 1
        assert result.backfill_complete is False
        session.add.assert_called_once()
        session.flush.assert_awaited_once()


# ---------------------------------------------------------------------------
# TestUpdateOldest
# ---------------------------------------------------------------------------


class TestUpdateOldest:
    """Tests for IngestStateManager.update_oldest."""

    @pytest.mark.asyncio
    async def test_updates_when_older(self):
        cp = _make_checkpoint(oldest_message_id=500)
        session = _make_session()
        session.execute.return_value = _mock_scalar_result(cp)
        mgr = IngestStateManager(session)

        await mgr.update_oldest(100, 300, 1)

        assert cp.oldest_message_id == 300

    @pytest.mark.asyncio
    async def test_no_op_when_newer(self):
        cp = _make_checkpoint(oldest_message_id=200)
        session = _make_session()
        session.execute.return_value = _mock_scalar_result(cp)
        mgr = IngestStateManager(session)

        await mgr.update_oldest(100, 500, 1)

        assert cp.oldest_message_id == 200

    @pytest.mark.asyncio
    async def test_sets_newest_on_first_batch(self):
        cp = _make_checkpoint(oldest_message_id=None, newest_message_id=None)
        session = _make_session()
        session.execute.return_value = _mock_scalar_result(cp)
        mgr = IngestStateManager(session)

        await mgr.update_oldest(100, 400, 1)

        assert cp.oldest_message_id == 400
        assert cp.newest_message_id == 400

    @pytest.mark.asyncio
    async def test_does_not_overwrite_existing_newest(self):
        cp = _make_checkpoint(oldest_message_id=None, newest_message_id=800)
        session = _make_session()
        session.execute.return_value = _mock_scalar_result(cp)
        mgr = IngestStateManager(session)

        await mgr.update_oldest(100, 400, 1)

        assert cp.newest_message_id == 800


# ---------------------------------------------------------------------------
# TestUpdateNewest
# ---------------------------------------------------------------------------


class TestUpdateNewest:
    """Tests for IngestStateManager.update_newest."""

    @pytest.mark.asyncio
    async def test_updates_when_newer(self):
        cp = _make_checkpoint(newest_message_id=500)
        session = _make_session()
        session.execute.return_value = _mock_scalar_result(cp)
        mgr = IngestStateManager(session)

        await mgr.update_newest(100, 800, 1)

        assert cp.newest_message_id == 800

    @pytest.mark.asyncio
    async def test_no_op_when_older(self):
        cp = _make_checkpoint(newest_message_id=800)
        session = _make_session()
        session.execute.return_value = _mock_scalar_result(cp)
        mgr = IngestStateManager(session)

        await mgr.update_newest(100, 500, 1)

        assert cp.newest_message_id == 800

    @pytest.mark.asyncio
    async def test_sets_when_none(self):
        cp = _make_checkpoint(newest_message_id=None)
        session = _make_session()
        session.execute.return_value = _mock_scalar_result(cp)
        mgr = IngestStateManager(session)

        await mgr.update_newest(100, 600, 1)

        assert cp.newest_message_id == 600


# ---------------------------------------------------------------------------
# TestUpdateBounds
# ---------------------------------------------------------------------------


class TestUpdateBounds:
    """Tests for IngestStateManager.update_bounds."""

    @pytest.mark.asyncio
    async def test_updates_both_bounds(self):
        cp = _make_checkpoint(oldest_message_id=None, newest_message_id=None)
        session = _make_session()
        session.execute.return_value = _mock_scalar_result(cp)
        mgr = IngestStateManager(session)

        await mgr.update_bounds(100, 1, oldest_id=200, newest_id=900)

        assert cp.oldest_message_id == 200
        assert cp.newest_message_id == 900

    @pytest.mark.asyncio
    async def test_only_improves(self):
        cp = _make_checkpoint(oldest_message_id=300, newest_message_id=700)
        session = _make_session()
        session.execute.return_value = _mock_scalar_result(cp)
        mgr = IngestStateManager(session)

        # oldest_id=400 is not older, newest_id=600 is not newer
        await mgr.update_bounds(100, 1, oldest_id=400, newest_id=600)

        assert cp.oldest_message_id == 300
        assert cp.newest_message_id == 700

    @pytest.mark.asyncio
    async def test_none_values_are_no_ops(self):
        cp = _make_checkpoint(oldest_message_id=300, newest_message_id=700)
        session = _make_session()
        session.execute.return_value = _mock_scalar_result(cp)
        mgr = IngestStateManager(session)

        await mgr.update_bounds(100, 1, oldest_id=None, newest_id=None)

        assert cp.oldest_message_id == 300
        assert cp.newest_message_id == 700

    @pytest.mark.asyncio
    async def test_improves_individual_bounds(self):
        cp = _make_checkpoint(oldest_message_id=300, newest_message_id=700)
        session = _make_session()
        session.execute.return_value = _mock_scalar_result(cp)
        mgr = IngestStateManager(session)

        await mgr.update_bounds(100, 1, oldest_id=100, newest_id=900)

        assert cp.oldest_message_id == 100
        assert cp.newest_message_id == 900


# ---------------------------------------------------------------------------
# TestMarkBackfillComplete
# ---------------------------------------------------------------------------


class TestMarkBackfillComplete:
    """Tests for IngestStateManager.mark_backfill_complete."""

    @pytest.mark.asyncio
    async def test_sets_flag(self):
        cp = _make_checkpoint(backfill_complete=False)
        session = _make_session()
        session.execute.return_value = _mock_scalar_result(cp)
        mgr = IngestStateManager(session)

        await mgr.mark_backfill_complete(100)

        assert cp.backfill_complete is True

    @pytest.mark.asyncio
    async def test_no_op_when_no_checkpoint(self):
        session = _make_session()
        session.execute.return_value = _mock_scalar_result(None)
        mgr = IngestStateManager(session)

        # Should not raise
        await mgr.mark_backfill_complete(100)


# ---------------------------------------------------------------------------
# TestIsBackfillComplete
# ---------------------------------------------------------------------------


class TestIsBackfillComplete:
    """Tests for IngestStateManager.is_backfill_complete."""

    @pytest.mark.asyncio
    async def test_true(self):
        cp = _make_checkpoint(backfill_complete=True)
        session = _make_session()
        session.execute.return_value = _mock_scalar_result(cp)
        mgr = IngestStateManager(session)

        assert await mgr.is_backfill_complete(100) is True

    @pytest.mark.asyncio
    async def test_false(self):
        cp = _make_checkpoint(backfill_complete=False)
        session = _make_session()
        session.execute.return_value = _mock_scalar_result(cp)
        mgr = IngestStateManager(session)

        assert await mgr.is_backfill_complete(100) is False

    @pytest.mark.asyncio
    async def test_no_checkpoint_returns_false(self):
        session = _make_session()
        session.execute.return_value = _mock_scalar_result(None)
        mgr = IngestStateManager(session)

        assert await mgr.is_backfill_complete(100) is False


# ---------------------------------------------------------------------------
# TestGetIncompleteBackfills
# ---------------------------------------------------------------------------


class TestGetIncompleteBackfills:
    """Tests for IngestStateManager.get_incomplete_backfills."""

    @pytest.mark.asyncio
    async def test_returns_ids(self):
        session = _make_session()
        session.execute.return_value = _mock_scalars_result([100, 200, 300])
        mgr = IngestStateManager(session)

        result = await mgr.get_incomplete_backfills(1)

        assert result == [100, 200, 300]

    @pytest.mark.asyncio
    async def test_returns_empty(self):
        session = _make_session()
        session.execute.return_value = _mock_scalars_result([])
        mgr = IngestStateManager(session)

        result = await mgr.get_incomplete_backfills(1)

        assert result == []
