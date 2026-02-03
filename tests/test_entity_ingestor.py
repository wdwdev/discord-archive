"""Unit tests for discord_archive.ingest.entity_ingestor."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from discord_archive.ingest.client import DiscordAPIError
from discord_archive.ingest.entity_ingestor import (
    ingest_emojis,
    ingest_roles,
    ingest_scheduled_events,
    ingest_stickers,
)

GUILD_ID = 111222333


@pytest.fixture
def session() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def client() -> AsyncMock:
    return AsyncMock()


# ---------------------------------------------------------------------------
# TestIngestRoles
# ---------------------------------------------------------------------------


@patch("discord_archive.ingest.entity_ingestor.logger")
class TestIngestRoles:
    """Tests for ingest_roles."""

    @pytest.mark.asyncio
    async def test_returns_zero_when_no_roles_key(self, _logger, session):
        result = await ingest_roles(session, {}, GUILD_ID)

        assert result == 0

    @pytest.mark.asyncio
    async def test_returns_zero_when_roles_empty(self, _logger, session):
        result = await ingest_roles(session, {"roles": []}, GUILD_ID)

        assert result == 0

    @pytest.mark.asyncio
    @patch("discord_archive.ingest.entity_ingestor.map_role")
    async def test_upserts_each_role(self, mock_map_role, _logger, session):
        mock_map_role.return_value = MagicMock()
        guild_data = {"roles": [{"id": "1"}, {"id": "2"}, {"id": "3"}]}

        result = await ingest_roles(session, guild_data, GUILD_ID)

        assert result == 3
        assert session.execute.await_count == 3

    @pytest.mark.asyncio
    @patch("discord_archive.ingest.entity_ingestor.map_role")
    async def test_calls_map_role_with_guild_id(self, mock_map_role, _logger, session):
        role_data = {"id": "1", "name": "Admin"}
        mock_map_role.return_value = MagicMock()

        await ingest_roles(session, {"roles": [role_data]}, GUILD_ID)

        mock_map_role.assert_called_once_with(role_data, GUILD_ID)


# ---------------------------------------------------------------------------
# TestIngestEmojis
# ---------------------------------------------------------------------------


@patch("discord_archive.ingest.entity_ingestor.logger")
class TestIngestEmojis:
    """Tests for ingest_emojis."""

    @pytest.mark.asyncio
    @patch("discord_archive.ingest.entity_ingestor.map_emoji")
    async def test_returns_count_of_emojis(self, mock_map_emoji, _logger, session, client):
        client.get_guild_emojis.return_value = [{"id": "1"}, {"id": "2"}]
        mock_map_emoji.return_value = MagicMock()

        result = await ingest_emojis(client, session, GUILD_ID)

        assert result == 2

    @pytest.mark.asyncio
    async def test_returns_zero_when_empty(self, _logger, session, client):
        client.get_guild_emojis.return_value = []

        result = await ingest_emojis(client, session, GUILD_ID)

        assert result == 0

    @pytest.mark.asyncio
    async def test_skips_on_403(self, _logger, session, client):
        client.get_guild_emojis.side_effect = DiscordAPIError(403, "Forbidden")

        result = await ingest_emojis(client, session, GUILD_ID)

        assert result == 0

    @pytest.mark.asyncio
    async def test_reraises_non_403(self, _logger, session, client):
        client.get_guild_emojis.side_effect = DiscordAPIError(500, "Server Error")

        with pytest.raises(DiscordAPIError) as exc_info:
            await ingest_emojis(client, session, GUILD_ID)

        assert exc_info.value.status_code == 500


# ---------------------------------------------------------------------------
# TestIngestStickers
# ---------------------------------------------------------------------------


@patch("discord_archive.ingest.entity_ingestor.logger")
class TestIngestStickers:
    """Tests for ingest_stickers."""

    @pytest.mark.asyncio
    @patch("discord_archive.ingest.entity_ingestor.map_sticker")
    async def test_returns_count_of_stickers(self, mock_map_sticker, _logger, session, client):
        client.get_guild_stickers.return_value = [{"id": "1"}, {"id": "2"}, {"id": "3"}]
        mock_map_sticker.return_value = MagicMock()

        result = await ingest_stickers(client, session, GUILD_ID)

        assert result == 3

    @pytest.mark.asyncio
    async def test_returns_zero_when_empty(self, _logger, session, client):
        client.get_guild_stickers.return_value = []

        result = await ingest_stickers(client, session, GUILD_ID)

        assert result == 0

    @pytest.mark.asyncio
    async def test_skips_on_403(self, _logger, session, client):
        client.get_guild_stickers.side_effect = DiscordAPIError(403, "Forbidden")

        result = await ingest_stickers(client, session, GUILD_ID)

        assert result == 0

    @pytest.mark.asyncio
    async def test_reraises_non_403(self, _logger, session, client):
        client.get_guild_stickers.side_effect = DiscordAPIError(500, "Server Error")

        with pytest.raises(DiscordAPIError) as exc_info:
            await ingest_stickers(client, session, GUILD_ID)

        assert exc_info.value.status_code == 500


# ---------------------------------------------------------------------------
# TestIngestScheduledEvents
# ---------------------------------------------------------------------------


@patch("discord_archive.ingest.entity_ingestor.logger")
class TestIngestScheduledEvents:
    """Tests for ingest_scheduled_events."""

    @pytest.mark.asyncio
    @patch("discord_archive.ingest.entity_ingestor.map_scheduled_event")
    async def test_returns_count_of_events(
        self, mock_map_event, _logger, session, client
    ):
        client.get_guild_scheduled_events.return_value = [{"id": "1"}, {"id": "2"}]
        mock_map_event.return_value = MagicMock()

        result = await ingest_scheduled_events(client, session, GUILD_ID)

        assert result == 2

    @pytest.mark.asyncio
    async def test_returns_zero_when_empty(self, _logger, session, client):
        client.get_guild_scheduled_events.return_value = []

        result = await ingest_scheduled_events(client, session, GUILD_ID)

        assert result == 0

    @pytest.mark.asyncio
    async def test_skips_on_403(self, _logger, session, client):
        client.get_guild_scheduled_events.side_effect = DiscordAPIError(
            403, "Forbidden"
        )

        result = await ingest_scheduled_events(client, session, GUILD_ID)

        assert result == 0

    @pytest.mark.asyncio
    async def test_reraises_non_403(self, _logger, session, client):
        client.get_guild_scheduled_events.side_effect = DiscordAPIError(
            500, "Server Error"
        )

        with pytest.raises(DiscordAPIError) as exc_info:
            await ingest_scheduled_events(client, session, GUILD_ID)

        assert exc_info.value.status_code == 500
