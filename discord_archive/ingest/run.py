"""Main orchestration for Discord ingest pipeline.

Coordinates guild, channel, and message ingestion across all accounts
defined in config.json.
"""

from __future__ import annotations

from discord_archive.config.settings import AccountConfig, AppSettings, load_config
from discord_archive.core import BaseOrchestrator
from discord_archive.ingest.client import DiscordClient
from discord_archive.ingest.guild_processor import process_guild
from discord_archive.ingest.logger import logger


class IngestOrchestrator(BaseOrchestrator):
    """Orchestrates the full ingest pipeline."""

    def __init__(self, settings: AppSettings) -> None:
        super().__init__(settings.database_url)
        self.settings = settings
        # Stats
        self.guilds_processed = 0
        self.channels_processed = 0
        self.messages_ingested = 0

    async def _run_pipeline(
        self,
        guild_id: int | None = None,
        channel_id: int | None = None,
    ) -> None:
        """Execute the ingest pipeline."""
        if channel_id:
            await self._process_single_channel(channel_id)
        else:
            for account in self.settings.accounts:
                await self._process_account(account, guild_id)

    def _log_summary(self, elapsed: float) -> None:
        """Log the final ingest summary."""
        logger.summary(
            guilds=self.guilds_processed,
            channels=self.channels_processed,
            messages=self.messages_ingested,
            elapsed=elapsed,
        )

    async def _process_account(
        self, account: AccountConfig, filter_guild_id: int | None = None
    ) -> None:
        """Process all guilds for an account."""
        logger.info(f"Processing account: {account.name}")

        async with DiscordClient(
            token=account.token,
            user_agent=account.user_agent,
        ) as client:
            for guild_id_str in account.guilds:
                guild_id = int(guild_id_str)

                if filter_guild_id and guild_id != filter_guild_id:
                    continue

                async with self.async_session() as session:
                    try:
                        result = await process_guild(client, session, guild_id)
                        self.guilds_processed += 1
                        self.channels_processed += result.channels_processed
                        self.messages_ingested += result.messages_ingested
                    except Exception as e:
                        logger.error(f"Error processing guild {guild_id}: {e}")
                        raise

    async def _process_single_channel(self, channel_id: int) -> None:
        """Process a single channel by ID (needs to find the right account)."""
        from discord_archive.ingest.guild_processor import process_channel

        for account in self.settings.accounts:
            async with DiscordClient(
                token=account.token,
                user_agent=account.user_agent,
            ) as client:
                try:
                    channel_data = await client.get_channel(channel_id)
                    guild_id = int(channel_data.get("guild_id", 0))

                    if not guild_id:
                        continue

                    async with self.async_session() as session:
                        result = await process_channel(
                            client=client,
                            session=session,
                            channel_data=channel_data,
                            guild_id=guild_id,
                        )
                        self.channels_processed += 1
                        self.messages_ingested += result.messages_ingested
                        return  # Found and processed
                except Exception:
                    continue  # Try next account

        logger.warning(f"Could not find channel {channel_id} in any account")


async def run_ingest(
    config_path: str = "config.json",
    guild_id: int | None = None,
    channel_id: int | None = None,
) -> None:
    """Entry point for running the ingest pipeline."""
    settings = load_config(config_path)
    orchestrator = IngestOrchestrator(settings)
    await orchestrator.run(guild_id=guild_id, channel_id=channel_id)
