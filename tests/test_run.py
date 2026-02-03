"""Unit tests for discord_archive.ingest.run."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from discord_archive.config.settings import AccountConfig, AppSettings
from discord_archive.ingest.client import DiscordAPIError
from discord_archive.ingest.guild_processor import ChannelProcessResult, GuildProcessResult
from discord_archive.ingest.run import IngestOrchestrator, run_ingest


def _make_account(name: str = "test", guilds: list[str] | None = None) -> AccountConfig:
    return AccountConfig(
        name=name,
        token=f"token-{name}",
        user_agent=f"agent-{name}",
        guilds=guilds or ["100", "200"],
    )


def _make_settings(accounts: list[AccountConfig] | None = None) -> AppSettings:
    return AppSettings(
        database_url="postgresql+asyncpg://test/db",
        accounts=accounts or [_make_account()],
    )


@pytest.fixture
def settings() -> AppSettings:
    return _make_settings()


# ---------------------------------------------------------------------------
# TestRunPipeline
# ---------------------------------------------------------------------------


class TestRunPipeline:
    """Tests for IngestOrchestrator._run_pipeline."""

    @pytest.mark.asyncio
    @patch.object(IngestOrchestrator, "_process_single_channel", new_callable=AsyncMock)
    @patch.object(IngestOrchestrator, "_process_account", new_callable=AsyncMock)
    @patch.object(IngestOrchestrator, "__init__", return_value=None)
    async def test_channel_id_routes_to_single_channel(
        self, _init, mock_process_account, mock_process_single
    ):
        orch = IngestOrchestrator.__new__(IngestOrchestrator)
        orch.settings = _make_settings()

        await orch._run_pipeline(channel_id=999)

        mock_process_single.assert_awaited_once_with(999)
        mock_process_account.assert_not_awaited()

    @pytest.mark.asyncio
    @patch.object(IngestOrchestrator, "_process_account", new_callable=AsyncMock)
    @patch.object(IngestOrchestrator, "__init__", return_value=None)
    async def test_no_channel_id_processes_all_accounts(self, _init, mock_process_account):
        acct1 = _make_account("a1")
        acct2 = _make_account("a2")
        orch = IngestOrchestrator.__new__(IngestOrchestrator)
        orch.settings = _make_settings([acct1, acct2])

        await orch._run_pipeline()

        assert mock_process_account.await_count == 2
        mock_process_account.assert_any_await(acct1, None)
        mock_process_account.assert_any_await(acct2, None)


# ---------------------------------------------------------------------------
# TestProcessAccount
# ---------------------------------------------------------------------------


class TestProcessAccount:
    """Tests for IngestOrchestrator._process_account."""

    @pytest.mark.asyncio
    @patch("discord_archive.ingest.run.process_guild", new_callable=AsyncMock)
    @patch("discord_archive.ingest.run.DiscordClient")
    @patch("discord_archive.ingest.run.logger")
    async def test_processes_all_guilds(self, _logger, mock_client_cls, mock_process_guild):
        mock_client = AsyncMock()
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        mock_process_guild.return_value = GuildProcessResult(
            channels_processed=2, messages_ingested=10
        )

        mock_session = AsyncMock()
        mock_session_factory = MagicMock()
        mock_session_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_factory.return_value.__aexit__ = AsyncMock(return_value=False)

        orch = IngestOrchestrator.__new__(IngestOrchestrator)
        orch.settings = _make_settings()
        orch.guilds_processed = 0
        orch.channels_processed = 0
        orch.messages_ingested = 0
        orch.async_session = mock_session_factory

        account = _make_account(guilds=["100", "200"])
        await orch._process_account(account)

        assert mock_process_guild.await_count == 2
        assert orch.guilds_processed == 2
        assert orch.channels_processed == 4
        assert orch.messages_ingested == 20

    @pytest.mark.asyncio
    @patch("discord_archive.ingest.run.process_guild", new_callable=AsyncMock)
    @patch("discord_archive.ingest.run.DiscordClient")
    @patch("discord_archive.ingest.run.logger")
    async def test_filter_guild_id_skips_non_matching(
        self, _logger, mock_client_cls, mock_process_guild
    ):
        mock_client = AsyncMock()
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        mock_process_guild.return_value = GuildProcessResult(
            channels_processed=1, messages_ingested=5
        )

        mock_session = AsyncMock()
        mock_session_factory = MagicMock()
        mock_session_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_factory.return_value.__aexit__ = AsyncMock(return_value=False)

        orch = IngestOrchestrator.__new__(IngestOrchestrator)
        orch.settings = _make_settings()
        orch.guilds_processed = 0
        orch.channels_processed = 0
        orch.messages_ingested = 0
        orch.async_session = mock_session_factory

        account = _make_account(guilds=["100", "200", "300"])
        await orch._process_account(account, filter_guild_id=200)

        mock_process_guild.assert_awaited_once()
        call_args = mock_process_guild.call_args
        assert call_args[0][2] == 200  # guild_id positional arg

    @pytest.mark.asyncio
    @patch("discord_archive.ingest.run.process_guild", new_callable=AsyncMock)
    @patch("discord_archive.ingest.run.DiscordClient")
    @patch("discord_archive.ingest.run.logger")
    async def test_error_propagates(self, _logger, mock_client_cls, mock_process_guild):
        mock_client = AsyncMock()
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        mock_process_guild.side_effect = RuntimeError("boom")

        mock_session = AsyncMock()
        mock_session_factory = MagicMock()
        mock_session_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_factory.return_value.__aexit__ = AsyncMock(return_value=False)

        orch = IngestOrchestrator.__new__(IngestOrchestrator)
        orch.settings = _make_settings()
        orch.guilds_processed = 0
        orch.channels_processed = 0
        orch.messages_ingested = 0
        orch.async_session = mock_session_factory

        account = _make_account(guilds=["100"])

        with pytest.raises(RuntimeError, match="boom"):
            await orch._process_account(account)


# ---------------------------------------------------------------------------
# TestProcessSingleChannel
# ---------------------------------------------------------------------------


class TestProcessSingleChannel:
    """Tests for IngestOrchestrator._process_single_channel."""

    def _make_orch(self) -> IngestOrchestrator:
        orch = IngestOrchestrator.__new__(IngestOrchestrator)
        orch.settings = _make_settings()
        orch.guilds_processed = 0
        orch.channels_processed = 0
        orch.messages_ingested = 0
        return orch

    @pytest.mark.asyncio
    @patch("discord_archive.ingest.guild_processor.process_channel", new_callable=AsyncMock)
    @patch("discord_archive.ingest.run.DiscordClient")
    @patch("discord_archive.ingest.run.logger")
    async def test_finds_channel_in_first_account(
        self, _logger, mock_client_cls, mock_process_channel
    ):
        mock_client = AsyncMock()
        mock_client.get_channel.return_value = {"guild_id": "555", "id": "777"}
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        mock_process_channel.return_value = ChannelProcessResult(messages_ingested=10)

        mock_session = AsyncMock()
        mock_session_factory = MagicMock()
        mock_session_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_factory.return_value.__aexit__ = AsyncMock(return_value=False)

        orch = self._make_orch()
        orch.async_session = mock_session_factory

        await orch._process_single_channel(777)

        mock_process_channel.assert_awaited_once()
        assert orch.channels_processed == 1
        assert orch.messages_ingested == 10

    @pytest.mark.asyncio
    @patch("discord_archive.ingest.guild_processor.process_channel", new_callable=AsyncMock)
    @patch("discord_archive.ingest.run.DiscordClient")
    @patch("discord_archive.ingest.run.logger")
    async def test_skips_401_403_404_tries_next(
        self, _logger, mock_client_cls, mock_process_channel
    ):
        acct1 = _make_account("fail")
        acct2 = _make_account("ok")

        mock_client_fail = AsyncMock()
        mock_client_fail.get_channel.side_effect = DiscordAPIError(403, "Forbidden")

        mock_client_ok = AsyncMock()
        mock_client_ok.get_channel.return_value = {"guild_id": "555", "id": "777"}

        # Return different clients for each account
        clients = iter([mock_client_fail, mock_client_ok])

        async def aenter_side_effect(self_inner):
            return next(clients)

        mock_ctx1 = MagicMock()
        mock_ctx1.__aenter__ = AsyncMock(return_value=mock_client_fail)
        mock_ctx1.__aexit__ = AsyncMock(return_value=False)

        mock_ctx2 = MagicMock()
        mock_ctx2.__aenter__ = AsyncMock(return_value=mock_client_ok)
        mock_ctx2.__aexit__ = AsyncMock(return_value=False)

        mock_client_cls.side_effect = [mock_ctx1, mock_ctx2]

        mock_process_channel.return_value = ChannelProcessResult(messages_ingested=5)

        mock_session = AsyncMock()
        mock_session_factory = MagicMock()
        mock_session_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_factory.return_value.__aexit__ = AsyncMock(return_value=False)

        orch = self._make_orch()
        orch.settings = _make_settings([acct1, acct2])
        orch.async_session = mock_session_factory

        await orch._process_single_channel(777)

        mock_process_channel.assert_awaited_once()
        assert orch.channels_processed == 1

    @pytest.mark.asyncio
    @patch("discord_archive.ingest.run.DiscordClient")
    @patch("discord_archive.ingest.run.logger")
    async def test_skips_channel_without_guild_id(self, _logger, mock_client_cls):
        acct1 = _make_account("no_guild")
        acct2 = _make_account("no_guild2")

        mock_client = AsyncMock()
        mock_client.get_channel.return_value = {"guild_id": 0, "id": "777"}
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        orch = self._make_orch()
        orch.settings = _make_settings([acct1, acct2])

        await orch._process_single_channel(777)

        # Should have tried both accounts and warned
        _logger.warning.assert_called_once()
        assert orch.channels_processed == 0

    @pytest.mark.asyncio
    @patch("discord_archive.ingest.run.DiscordClient")
    @patch("discord_archive.ingest.run.logger")
    async def test_warns_when_not_found(self, mock_logger, mock_client_cls):
        mock_client = AsyncMock()
        mock_client.get_channel.side_effect = DiscordAPIError(404, "Not Found")
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        orch = self._make_orch()
        orch.settings = _make_settings([_make_account()])

        await orch._process_single_channel(999)

        mock_logger.warning.assert_called_once()
        assert "999" in str(mock_logger.warning.call_args)


# ---------------------------------------------------------------------------
# TestRunIngest
# ---------------------------------------------------------------------------


class TestRunIngest:
    """Tests for run_ingest entry point."""

    @pytest.mark.asyncio
    @patch("discord_archive.ingest.run.IngestOrchestrator")
    @patch("discord_archive.ingest.run.load_config")
    async def test_loads_config_and_runs(self, mock_load_config, mock_orch_cls):
        mock_settings = _make_settings()
        mock_load_config.return_value = mock_settings

        mock_orch = AsyncMock()
        mock_orch_cls.return_value = mock_orch

        await run_ingest(config_path="test.json", guild_id=42, channel_id=99)

        mock_load_config.assert_called_once_with("test.json")
        mock_orch_cls.assert_called_once_with(mock_settings)
        mock_orch.run.assert_awaited_once_with(guild_id=42, channel_id=99)
