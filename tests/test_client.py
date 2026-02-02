"""Unit tests for discord_archive.ingest.client."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from discord_archive.ingest.client import (
    INITIAL_BACKOFF,
    MAX_BACKOFF,
    MAX_RETRIES,
    DiscordAPIError,
    DiscordClient,
)


def _make_response(
    status_code: int,
    json_data: dict | list | None = None,
    text: str = "",
    headers: dict | None = None,
) -> MagicMock:
    """Build a mock httpx.Response."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.text = text
    resp.headers = headers or {}
    if json_data is not None:
        resp.json.return_value = json_data
    else:
        resp.json.side_effect = Exception("No JSON")
    return resp


def _make_client() -> DiscordClient:
    """Create a DiscordClient with a mocked HTTP layer."""
    client = DiscordClient(token="test-token", user_agent="test-agent")
    client._client = AsyncMock(spec=httpx.AsyncClient)
    return client


# ---------------------------------------------------------------------------
# TestRequest
# ---------------------------------------------------------------------------


class TestRequest:
    """Tests for DiscordClient._request."""

    @pytest.mark.asyncio
    async def test_200_returns_json(self):
        client = _make_client()
        client._client.request.return_value = _make_response(200, {"id": "1"})

        result = await client._request("GET", "/test")

        assert result == {"id": "1"}

    @pytest.mark.asyncio
    async def test_204_returns_none(self):
        client = _make_client()
        client._client.request.return_value = _make_response(204)

        result = await client._request("DELETE", "/test")

        assert result is None

    @pytest.mark.asyncio
    @patch("discord_archive.ingest.client.asyncio.sleep", new_callable=AsyncMock)
    async def test_429_sleeps_retry_after_then_retries(self, mock_sleep):
        client = _make_client()
        rate_resp = _make_response(429, headers={"Retry-After": "2.5"})
        ok_resp = _make_response(200, {"ok": True})
        client._client.request.side_effect = [rate_resp, ok_resp]

        result = await client._request("GET", "/test")

        assert result == {"ok": True}
        mock_sleep.assert_awaited_once_with(2.5)

    @pytest.mark.asyncio
    @patch("discord_archive.ingest.client.asyncio.sleep", new_callable=AsyncMock)
    async def test_429_does_not_consume_retry_attempts(self, mock_sleep):
        """429s don't count against the retry budget; all MAX_RETRIES+1
        attempts remain available for real errors."""
        client = _make_client()
        rate_resp = _make_response(429, headers={"Retry-After": "0.5"})
        ok_resp = _make_response(200, {"ok": True})
        # More 429s than MAX_RETRIES+1, then success
        client._client.request.side_effect = (
            [rate_resp] * (MAX_RETRIES + 5) + [ok_resp]
        )

        result = await client._request("GET", "/test")

        assert result == {"ok": True}
        assert mock_sleep.await_count == MAX_RETRIES + 5

    @pytest.mark.asyncio
    async def test_401_raises_immediately(self):
        client = _make_client()
        client._client.request.return_value = _make_response(
            401, text="Unauthorized"
        )

        with pytest.raises(DiscordAPIError) as exc_info:
            await client._request("GET", "/test")

        assert exc_info.value.status_code == 401
        assert client._client.request.await_count == 1

    @pytest.mark.asyncio
    async def test_403_raises_with_json_message(self):
        client = _make_client()
        resp = _make_response(
            403,
            json_data={"message": "Missing Access"},
            text="Forbidden",
        )
        client._client.request.return_value = resp

        with pytest.raises(DiscordAPIError) as exc_info:
            await client._request("GET", "/test")

        assert exc_info.value.status_code == 403
        assert exc_info.value.message == "Missing Access"

    @pytest.mark.asyncio
    async def test_404_raises_immediately(self):
        client = _make_client()
        client._client.request.return_value = _make_response(404, text="Not Found")

        with pytest.raises(DiscordAPIError) as exc_info:
            await client._request("GET", "/test")

        assert exc_info.value.status_code == 404
        assert client._client.request.await_count == 1

    @pytest.mark.asyncio
    @patch("discord_archive.ingest.client.asyncio.sleep", new_callable=AsyncMock)
    async def test_5xx_retries_then_succeeds(self, mock_sleep):
        client = _make_client()
        err_resp = _make_response(500, text="Internal Server Error")
        ok_resp = _make_response(200, {"ok": True})
        client._client.request.side_effect = [err_resp, ok_resp]

        result = await client._request("GET", "/test")

        assert result == {"ok": True}
        mock_sleep.assert_awaited_once_with(INITIAL_BACKOFF)

    @pytest.mark.asyncio
    @patch("discord_archive.ingest.client.asyncio.sleep", new_callable=AsyncMock)
    async def test_5xx_exhausts_retries_raises(self, mock_sleep):
        client = _make_client()
        err_resp = _make_response(502, text="Bad Gateway")
        client._client.request.return_value = err_resp

        with pytest.raises(DiscordAPIError) as exc_info:
            await client._request("GET", "/test")

        assert exc_info.value.status_code == 502
        assert client._client.request.await_count == MAX_RETRIES + 1

    @pytest.mark.asyncio
    @patch("discord_archive.ingest.client.asyncio.sleep", new_callable=AsyncMock)
    async def test_timeout_retries_then_succeeds(self, mock_sleep):
        client = _make_client()
        ok_resp = _make_response(200, {"ok": True})
        client._client.request.side_effect = [
            httpx.TimeoutException("timeout"),
            ok_resp,
        ]

        result = await client._request("GET", "/test")

        assert result == {"ok": True}
        mock_sleep.assert_awaited_once_with(INITIAL_BACKOFF)

    @pytest.mark.asyncio
    @patch("discord_archive.ingest.client.asyncio.sleep", new_callable=AsyncMock)
    async def test_timeout_exhausts_retries_raises(self, mock_sleep):
        client = _make_client()
        client._client.request.side_effect = httpx.TimeoutException("timeout")

        with pytest.raises(httpx.TimeoutException):
            await client._request("GET", "/test")

        assert client._client.request.await_count == MAX_RETRIES + 1

    @pytest.mark.asyncio
    @patch("discord_archive.ingest.client.asyncio.sleep", new_callable=AsyncMock)
    async def test_transport_error_retries_then_succeeds(self, mock_sleep):
        client = _make_client()
        ok_resp = _make_response(200, {"ok": True})
        client._client.request.side_effect = [
            httpx.TransportError("connection reset"),
            ok_resp,
        ]

        result = await client._request("GET", "/test")

        assert result == {"ok": True}

    @pytest.mark.asyncio
    @patch("discord_archive.ingest.client.asyncio.sleep", new_callable=AsyncMock)
    async def test_transport_error_exhausts_retries_raises(self, mock_sleep):
        client = _make_client()
        client._client.request.side_effect = httpx.TransportError("connection reset")

        with pytest.raises(httpx.TransportError):
            await client._request("GET", "/test")

        assert client._client.request.await_count == MAX_RETRIES + 1

    @pytest.mark.asyncio
    async def test_client_none_raises_runtime_error(self):
        client = DiscordClient(token="t", user_agent="u")
        # _client is None by default after __post_init__

        with pytest.raises(RuntimeError, match="Client not initialized"):
            await client._request("GET", "/test")

    @pytest.mark.asyncio
    @patch("discord_archive.ingest.client.asyncio.sleep", new_callable=AsyncMock)
    async def test_backoff_caps_at_max(self, mock_sleep):
        client = _make_client()
        err_resp = _make_response(500, text="error")
        ok_resp = _make_response(200, {"ok": True})
        # Need enough 500s to exceed MAX_BACKOFF: 1, 2, 4, 8, 16, 32, 64, 64...
        # MAX_RETRIES is 5, so we get backoffs: 1, 2, 4, 8, 16
        # Set MAX_RETRIES+1 responses (all fail except last)
        client._client.request.side_effect = [err_resp] * MAX_RETRIES + [ok_resp]

        await client._request("GET", "/test")

        # Verify backoff values are capped
        sleep_values = [call.args[0] for call in mock_sleep.await_args_list]
        expected = INITIAL_BACKOFF
        for val in sleep_values:
            assert val == expected
            expected = min(expected * 2, MAX_BACKOFF)


# ---------------------------------------------------------------------------
# TestGetMessages
# ---------------------------------------------------------------------------


class TestGetMessages:
    """Tests for DiscordClient.get_messages."""

    @pytest.mark.asyncio
    async def test_clamps_limit_to_100(self):
        client = _make_client()
        client._client.request.return_value = _make_response(200, [])

        await client.get_messages(channel_id=1, limit=200)

        _, kwargs = client._client.request.call_args
        assert kwargs["params"]["limit"] == 100

    @pytest.mark.asyncio
    async def test_passes_before_param(self):
        client = _make_client()
        client._client.request.return_value = _make_response(200, [])

        await client.get_messages(channel_id=1, before=999)

        _, kwargs = client._client.request.call_args
        assert kwargs["params"]["before"] == 999

    @pytest.mark.asyncio
    async def test_passes_after_param(self):
        client = _make_client()
        client._client.request.return_value = _make_response(200, [])

        await client.get_messages(channel_id=1, after=500)

        _, kwargs = client._client.request.call_args
        assert kwargs["params"]["after"] == 500

    @pytest.mark.asyncio
    async def test_omits_none_params(self):
        client = _make_client()
        client._client.request.return_value = _make_response(200, [])

        await client.get_messages(channel_id=1)

        _, kwargs = client._client.request.call_args
        params = kwargs["params"]
        assert "before" not in params
        assert "after" not in params
        assert "around" not in params


# ---------------------------------------------------------------------------
# TestGetGuild
# ---------------------------------------------------------------------------


class TestGetGuild:
    """Tests for DiscordClient.get_guild."""

    @pytest.mark.asyncio
    async def test_correct_path(self):
        client = _make_client()
        client._client.request.return_value = _make_response(
            200, {"id": "123", "name": "Test"}
        )

        result = await client.get_guild(123)

        assert result == {"id": "123", "name": "Test"}
        args, kwargs = client._client.request.call_args
        assert args == ("GET", "/guilds/123")


# ---------------------------------------------------------------------------
# TestDiscordAPIError
# ---------------------------------------------------------------------------


class TestDiscordAPIError:
    """Tests for DiscordAPIError exception class."""

    def test_stores_fields(self):
        err = DiscordAPIError(404, "Not Found")
        assert err.status_code == 404
        assert err.message == "Not Found"

    def test_str_repr(self):
        err = DiscordAPIError(403, "Missing Access")
        assert "403" in str(err)
        assert "Missing Access" in str(err)
