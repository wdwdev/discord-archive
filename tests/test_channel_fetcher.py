"""Tests for discord_archive.ingest.channel_fetcher module."""

from __future__ import annotations

import pytest

from discord_archive.ingest.channel_fetcher import (
    PermissionContext,
    _filter_fetchable_channels,
)
from discord_archive.ingest.mappers.channel import (
    CHANNEL_TYPE_FORUM,
    CHANNEL_TYPE_MEDIA,
)
from discord_archive.utils.permissions import VIEW_CHANNEL


class TestPermissionContext:
    """Tests for PermissionContext dataclass."""

    def test_create_context(self) -> None:
        """Should create a permission context with all fields."""
        ctx = PermissionContext(
            user_id=123,
            user_roles=[111, 222],
            base_permissions=1024,
            guild_id=999,
        )

        assert ctx.user_id == 123
        assert ctx.user_roles == [111, 222]
        assert ctx.base_permissions == 1024
        assert ctx.guild_id == 999


class TestFilterFetchableChannels:
    """Tests for _filter_fetchable_channels function."""

    @pytest.fixture
    def permission_context(self) -> PermissionContext:
        """Create a permission context with VIEW_CHANNEL permission."""
        return PermissionContext(
            user_id=123456789,
            user_roles=[],
            base_permissions=VIEW_CHANNEL,  # Has VIEW_CHANNEL
            guild_id=999999999,
        )

    def test_filters_text_channels(self, permission_context: PermissionContext) -> None:
        """Should include text channels (type 0) with VIEW_CHANNEL permission."""
        channels = [
            {"id": "1", "type": 0, "name": "general"},  # text
            {"id": "2", "type": 5, "name": "announcements"},  # announcement
        ]

        result = _filter_fetchable_channels(channels, permission_context)

        assert len(result) == 2
        assert result[0][0]["id"] == "1"
        assert result[1][0]["id"] == "2"

    def test_filters_forum_and_media_channels(
        self, permission_context: PermissionContext
    ) -> None:
        """Should include forum and media channels."""
        channels = [
            {"id": "1", "type": CHANNEL_TYPE_FORUM, "name": "forum"},
            {"id": "2", "type": CHANNEL_TYPE_MEDIA, "name": "media"},
        ]

        result = _filter_fetchable_channels(channels, permission_context)

        assert len(result) == 2

    def test_excludes_voice_channels(
        self, permission_context: PermissionContext
    ) -> None:
        """Should exclude voice and other non-text channels."""
        channels = [
            {"id": "1", "type": 2, "name": "voice"},  # voice
            {"id": "2", "type": 4, "name": "category"},  # category
            {"id": "3", "type": 13, "name": "stage"},  # stage
        ]

        result = _filter_fetchable_channels(channels, permission_context)

        assert len(result) == 0

    def test_excludes_channels_without_view_permission(self) -> None:
        """Should exclude channels where user lacks VIEW_CHANNEL."""
        ctx = PermissionContext(
            user_id=123456789,
            user_roles=[],
            base_permissions=0,  # No permissions
            guild_id=999999999,
        )
        channels = [
            {"id": "1", "type": 0, "name": "secret"},
        ]

        result = _filter_fetchable_channels(channels, ctx)

        assert len(result) == 0

    def test_respects_channel_overwrites_deny(self) -> None:
        """Should respect channel overwrite that denies VIEW_CHANNEL."""
        ctx = PermissionContext(
            user_id=123456789,
            user_roles=[],
            base_permissions=VIEW_CHANNEL,  # Has VIEW_CHANNEL from roles
            guild_id=999999999,
        )
        channels = [
            {
                "id": "1",
                "type": 0,
                "name": "private",
                "permission_overwrites": [
                    {
                        "id": "999999999",  # @everyone deny
                        "type": 0,
                        "allow": "0",
                        "deny": str(VIEW_CHANNEL),
                    }
                ],
            },
        ]

        result = _filter_fetchable_channels(channels, ctx)

        assert len(result) == 0

    def test_respects_channel_overwrites_allow(self) -> None:
        """Should respect channel overwrite that allows VIEW_CHANNEL."""
        ctx = PermissionContext(
            user_id=123456789,
            user_roles=[111111111],  # Has this role
            base_permissions=0,  # No base permissions
            guild_id=999999999,
        )
        channels = [
            {
                "id": "1",
                "type": 0,
                "name": "members-only",
                "permission_overwrites": [
                    {
                        "id": "111111111",  # Role allow
                        "type": 0,
                        "allow": str(VIEW_CHANNEL),
                        "deny": "0",
                    }
                ],
            },
        ]

        result = _filter_fetchable_channels(channels, ctx)

        assert len(result) == 1

    def test_returns_permissions_with_channel(
        self, permission_context: PermissionContext
    ) -> None:
        """Should return computed permissions along with channel data."""
        channels = [
            {"id": "1", "type": 0, "name": "general"},
        ]

        result = _filter_fetchable_channels(channels, permission_context)

        assert len(result) == 1
        channel_data, perms = result[0]
        assert channel_data["id"] == "1"
        assert perms & VIEW_CHANNEL  # Should have VIEW_CHANNEL bit set

    def test_empty_channel_list(self, permission_context: PermissionContext) -> None:
        """Should handle empty channel list."""
        result = _filter_fetchable_channels([], permission_context)
        assert result == []
