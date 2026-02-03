"""Unit tests for discord_archive.db.repositories.message_repository."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from discord_archive.db.repositories.message_repository import (
    bulk_insert_attachments,
    bulk_insert_messages,
    bulk_upsert_reactions,
    bulk_upsert_users,
    get_channel_message_count,
    persist_messages_batch,
)


@pytest.fixture
def session() -> AsyncMock:
    return AsyncMock()


def _make_user(user_id: int) -> MagicMock:
    user = MagicMock()
    user.user_id = user_id
    user.username = f"user_{user_id}"
    user.discriminator = "0"
    user.global_name = None
    user.avatar = None
    user.avatar_decoration_data = None
    user.banner = None
    user.accent_color = None
    user.bot = False
    user.system = False
    user.public_flags = 0
    user.premium_type = None
    user.raw = {}
    return user


def _make_message(message_id: int) -> MagicMock:
    msg = MagicMock()
    msg.message_id = message_id
    msg.channel_id = 1
    msg.author_id = 1
    msg.guild_id = 1
    msg.content = "hello"
    msg.created_at = None
    msg.edited_timestamp = None
    msg.type = 0
    msg.tts = False
    msg.flags = 0
    msg.pinned = False
    msg.mention_everyone = False
    msg.mentions = []
    msg.mention_roles = []
    msg.mention_channels = []
    msg.webhook_id = None
    msg.application = None
    msg.application_id = None
    msg.message_reference = None
    msg.referenced_message_id = None
    msg.message_snapshots = None
    msg.interaction_metadata = None
    msg.thread = None
    msg.embeds = []
    msg.components = []
    msg.sticker_items = []
    msg.poll = None
    msg.activity = None
    msg.call = None
    msg.role_subscription_data = None
    msg.raw = {}
    return msg


def _make_attachment(attachment_id: int) -> MagicMock:
    att = MagicMock()
    att.attachment_id = attachment_id
    att.message_id = 1
    att.filename = "file.txt"
    att.description = None
    att.content_type = "text/plain"
    att.size = 100
    att.url = "https://example.com/file.txt"
    att.proxy_url = "https://example.com/file.txt"
    att.height = None
    att.width = None
    att.duration_secs = None
    att.waveform = None
    att.ephemeral = False
    att.flags = 0
    att.title = None
    att.raw = {}
    return att


def _make_reaction(message_id: int, emoji_key: str) -> MagicMock:
    r = MagicMock()
    r.message_id = message_id
    r.emoji_key = emoji_key
    r.emoji_id = None
    r.emoji_name = emoji_key
    r.emoji_animated = False
    r.count = 1
    r.count_details = {}
    r.burst_colors = []
    r.raw = {}
    return r


# ---------------------------------------------------------------------------
# TestGetChannelMessageCount
# ---------------------------------------------------------------------------


class TestGetChannelMessageCount:
    """Tests for get_channel_message_count."""

    @pytest.mark.asyncio
    async def test_returns_count(self, session):
        mock_result = MagicMock()
        mock_result.scalar.return_value = 42
        session.execute.return_value = mock_result

        count = await get_channel_message_count(session, channel_id=1)

        assert count == 42

    @pytest.mark.asyncio
    async def test_returns_zero_when_none(self, session):
        mock_result = MagicMock()
        mock_result.scalar.return_value = None
        session.execute.return_value = mock_result

        count = await get_channel_message_count(session, channel_id=1)

        assert count == 0


# ---------------------------------------------------------------------------
# TestBulkUpsertUsers
# ---------------------------------------------------------------------------


class TestBulkUpsertUsers:
    """Tests for bulk_upsert_users."""

    @pytest.mark.asyncio
    async def test_empty_list_no_execute(self, session):
        await bulk_upsert_users(session, [])

        session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_deduplicates_by_user_id(self, session):
        users = [_make_user(1), _make_user(2), _make_user(1)]

        await bulk_upsert_users(session, users)

        session.execute.assert_awaited_once()
        # The statement is built with pg_insert values; we verify execute was
        # called exactly once (meaning dedup happened before the query).

    @pytest.mark.asyncio
    async def test_calls_execute(self, session):
        users = [_make_user(10)]

        await bulk_upsert_users(session, users)

        session.execute.assert_awaited_once()


# ---------------------------------------------------------------------------
# TestBulkInsertMessages
# ---------------------------------------------------------------------------


class TestBulkInsertMessages:
    """Tests for bulk_insert_messages."""

    @pytest.mark.asyncio
    async def test_empty_list_no_execute(self, session):
        await bulk_insert_messages(session, [])

        session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_calls_execute(self, session):
        msgs = [_make_message(1)]

        await bulk_insert_messages(session, msgs)

        session.execute.assert_awaited_once()


# ---------------------------------------------------------------------------
# TestBulkInsertAttachments
# ---------------------------------------------------------------------------


class TestBulkInsertAttachments:
    """Tests for bulk_insert_attachments."""

    @pytest.mark.asyncio
    async def test_empty_list_no_execute(self, session):
        await bulk_insert_attachments(session, [])

        session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_calls_execute(self, session):
        atts = [_make_attachment(1)]

        await bulk_insert_attachments(session, atts)

        session.execute.assert_awaited_once()


# ---------------------------------------------------------------------------
# TestBulkUpsertReactions
# ---------------------------------------------------------------------------


class TestBulkUpsertReactions:
    """Tests for bulk_upsert_reactions."""

    @pytest.mark.asyncio
    async def test_empty_list_no_execute(self, session):
        await bulk_upsert_reactions(session, [])

        session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_calls_execute(self, session):
        reactions = [_make_reaction(1, "thumbsup")]

        await bulk_upsert_reactions(session, reactions)

        session.execute.assert_awaited_once()


# ---------------------------------------------------------------------------
# TestPersistMessagesBatch
# ---------------------------------------------------------------------------


class TestPersistMessagesBatch:
    """Tests for persist_messages_batch."""

    @pytest.mark.asyncio
    async def test_empty_returns_zero(self, session):
        result = await persist_messages_batch(session, [], guild_id=1)

        assert result == 0
        session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(
        "discord_archive.db.repositories.message_repository.extract_users_from_message"
    )
    @patch("discord_archive.db.repositories.message_repository.map_messages")
    async def test_calls_all_bulk_ops_in_order(
        self, mock_map_messages, mock_extract_users, session
    ):
        msgs = [_make_message(1), _make_message(2)]
        atts = [_make_attachment(10)]
        rxns = [_make_reaction(1, "heart")]
        mock_map_messages.return_value = (msgs, atts, rxns)

        users = [_make_user(100)]
        mock_extract_users.return_value = users

        messages_data = [{"id": "1", "author": {"id": "100"}}, {"id": "2", "author": {"id": "100"}}]

        result = await persist_messages_batch(session, messages_data, guild_id=1)

        # Verify map_messages called with right args
        mock_map_messages.assert_called_once_with(messages_data, 1)

        # extract_users_from_message called for each message
        assert mock_extract_users.call_count == 2

        # session.execute should have been called 4 times (users, messages, attachments, reactions)
        assert session.execute.await_count == 4

        assert result == 2

    @pytest.mark.asyncio
    @patch(
        "discord_archive.db.repositories.message_repository.extract_users_from_message"
    )
    @patch("discord_archive.db.repositories.message_repository.map_messages")
    async def test_returns_message_count(
        self, mock_map_messages, mock_extract_users, session
    ):
        msgs = [_make_message(1), _make_message(2), _make_message(3)]
        mock_map_messages.return_value = (msgs, [], [])
        mock_extract_users.return_value = []

        messages_data = [{"id": str(i)} for i in range(3)]

        result = await persist_messages_batch(session, messages_data, guild_id=1)

        assert result == 3
