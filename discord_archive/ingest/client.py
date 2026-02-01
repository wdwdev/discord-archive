"""Discord REST API client with rate limit handling.

This module provides an async HTTP client for Discord's REST API with:
- Automatic rate limit handling (429 responses)
- Exponential backoff for server errors (5xx)
- Proper request headers for user/bot tokens
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import httpx

from discord_archive.ingest.logger import logger


# Discord API base URL
BASE_URL = "https://discord.com/api/v10"

# Retry configuration
MAX_RETRIES = 5
MAX_RATE_LIMIT_RETRIES = 30  # Cap on consecutive 429 retries
INITIAL_BACKOFF = 1.0  # seconds
MAX_BACKOFF = 64.0  # seconds


class DiscordAPIError(Exception):
    """Raised when Discord API returns an error."""

    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        self.message = message
        super().__init__(f"Discord API error {status_code}: {message}")


class DiscordRateLimitError(Exception):
    """Raised when rate limited (for internal use)."""

    def __init__(self, retry_after: float) -> None:
        self.retry_after = retry_after
        super().__init__(f"Rate limited, retry after {retry_after}s")


@dataclass
class DiscordClient:
    """Async Discord REST API client.

    Handles rate limits and retries automatically.
    """

    token: str
    user_agent: str

    def __post_init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    @property
    def headers(self) -> dict[str, str]:
        """Build request headers."""
        return {
            "Authorization": self.token,
            "User-Agent": self.user_agent,
            "Content-Type": "application/json",
        }

    async def __aenter__(self) -> "DiscordClient":
        self._client = httpx.AsyncClient(
            base_url=BASE_URL,
            headers=self.headers,
            timeout=30.0,
        )
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """Make a request with rate limit and retry handling."""
        if not self._client:
            raise RuntimeError("Client not initialized. Use async with.")

        backoff = INITIAL_BACKOFF
        rate_limit_retries = 0

        for attempt in range(MAX_RETRIES + 1):
            try:
                response = await self._client.request(method, path, params=params)

                # Success
                if response.status_code == 200:
                    return response.json()

                # No content (e.g., DELETE success)
                if response.status_code == 204:
                    return None

                # Rate limited - wait and retry (doesn't count as attempt)
                if response.status_code == 429:
                    rate_limit_retries += 1
                    if rate_limit_retries > MAX_RATE_LIMIT_RETRIES:
                        raise DiscordAPIError(429, "Max rate limit retries exceeded")
                    retry_after = float(response.headers.get("Retry-After", 1.0))
                    logger.rate_limit(retry_after)
                    await asyncio.sleep(retry_after)
                    continue  # Don't increment attempt counter

                # Client errors - fail immediately
                if response.status_code in (401, 403, 404):
                    error_msg = response.text
                    try:
                        error_msg = response.json().get("message", response.text)
                    except Exception:
                        pass
                    raise DiscordAPIError(response.status_code, error_msg)

                # Server errors - retry with backoff
                if response.status_code >= 500:
                    if attempt < MAX_RETRIES:
                        logger.retry(
                            attempt + 1,
                            MAX_RETRIES,
                            backoff,
                            f"HTTP {response.status_code}",
                        )
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, MAX_BACKOFF)
                        continue
                    raise DiscordAPIError(response.status_code, response.text)

                # Other errors
                raise DiscordAPIError(response.status_code, response.text)

            except httpx.TimeoutException:
                if attempt < MAX_RETRIES:
                    logger.retry(attempt + 1, MAX_RETRIES, backoff, "timeout")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF)
                    continue
                raise

            except httpx.TransportError as e:
                if attempt < MAX_RETRIES:
                    logger.retry(attempt + 1, MAX_RETRIES, backoff, str(e))
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF)
                    continue
                raise

        # Should not reach here
        raise DiscordAPIError(500, "Max retries exceeded")

    # -------------------------------------------------------------------------
    # Guild endpoints
    # -------------------------------------------------------------------------

    async def get_guild(self, guild_id: int) -> dict[str, Any]:
        """Fetch guild information."""
        return await self._request("GET", f"/guilds/{guild_id}")

    async def get_guild_channels(self, guild_id: int) -> list[dict[str, Any]]:
        """Fetch all channels in a guild (excludes threads)."""
        return await self._request("GET", f"/guilds/{guild_id}/channels")

    async def get_guild_members(
        self, guild_id: int, limit: int = 1000, after: int | None = None
    ) -> list[dict[str, Any]]:
        """Fetch guild members (paginated)."""
        params: dict[str, Any] = {"limit": limit}
        if after:
            params["after"] = after
        return await self._request("GET", f"/guilds/{guild_id}/members", params=params)

    # -------------------------------------------------------------------------
    # Thread endpoints
    # -------------------------------------------------------------------------

    async def get_active_threads(self, guild_id: int) -> dict[str, Any]:
        """Fetch all active threads in a guild."""
        return await self._request("GET", f"/guilds/{guild_id}/threads/active")

    async def get_public_archived_threads(
        self, channel_id: int, before: str | None = None, limit: int = 100
    ) -> dict[str, Any]:
        """Fetch public archived threads in a channel."""
        params: dict[str, Any] = {"limit": limit}
        if before:
            params["before"] = before
        return await self._request(
            "GET", f"/channels/{channel_id}/threads/archived/public", params=params
        )

    async def get_private_archived_threads(
        self, channel_id: int, before: str | None = None, limit: int = 100
    ) -> dict[str, Any]:
        """Fetch private archived threads in a channel (requires permissions)."""
        params: dict[str, Any] = {"limit": limit}
        if before:
            params["before"] = before
        return await self._request(
            "GET", f"/channels/{channel_id}/threads/archived/private", params=params
        )

    # -------------------------------------------------------------------------
    # Channel endpoints
    # -------------------------------------------------------------------------

    async def get_channel(self, channel_id: int) -> dict[str, Any]:
        """Fetch channel information."""
        return await self._request("GET", f"/channels/{channel_id}")

    # -------------------------------------------------------------------------
    # Message endpoints
    # -------------------------------------------------------------------------

    async def get_messages(
        self,
        channel_id: int,
        limit: int = 100,
        before: int | None = None,
        after: int | None = None,
        around: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch messages from a channel.

        Args:
            channel_id: The channel to fetch from
            limit: Max messages to return (1-100)
            before: Get messages before this message ID
            after: Get messages after this message ID
            around: Get messages around this message ID

        Returns:
            List of message objects, ordered by ID descending (newest first)
        """
        params: dict[str, Any] = {"limit": min(limit, 100)}
        if before:
            params["before"] = before
        if after:
            params["after"] = after
        if around:
            params["around"] = around
        return await self._request(
            "GET", f"/channels/{channel_id}/messages", params=params
        )

    # -------------------------------------------------------------------------
    # User endpoints
    # -------------------------------------------------------------------------

    async def get_user(self, user_id: int) -> dict[str, Any]:
        """Fetch user information."""
        return await self._request("GET", f"/users/{user_id}")

    async def get_current_user(self) -> dict[str, Any]:
        """Fetch current user (the token owner)."""
        return await self._request("GET", "/users/@me")

    async def get_current_user_guild_member(self, guild_id: int) -> dict[str, Any]:
        """Fetch current user's member info in a guild.

        Returns member object with roles, nickname, etc.
        """
        return await self._request("GET", f"/users/@me/guilds/{guild_id}/member")

    # -------------------------------------------------------------------------
    # Emoji endpoints
    # -------------------------------------------------------------------------

    async def get_guild_emojis(self, guild_id: int) -> list[dict[str, Any]]:
        """Fetch all custom emojis in a guild."""
        return await self._request("GET", f"/guilds/{guild_id}/emojis")

    # -------------------------------------------------------------------------
    # Sticker endpoints
    # -------------------------------------------------------------------------

    async def get_guild_stickers(self, guild_id: int) -> list[dict[str, Any]]:
        """Fetch all stickers in a guild."""
        return await self._request("GET", f"/guilds/{guild_id}/stickers")

    # -------------------------------------------------------------------------
    # Scheduled Event endpoints
    # -------------------------------------------------------------------------

    async def get_guild_scheduled_events(
        self, guild_id: int, with_user_count: bool = True
    ) -> list[dict[str, Any]]:
        """Fetch all scheduled events in a guild."""
        params: dict[str, Any] = {"with_user_count": with_user_count}
        return await self._request(
            "GET", f"/guilds/{guild_id}/scheduled-events", params=params
        )
