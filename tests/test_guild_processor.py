"""Tests for discord_archive.ingest.guild_processor module."""

from __future__ import annotations

import pytest

from discord_archive.ingest.channel_fetcher import PermissionContext
from discord_archive.ingest.guild_processor import (
    ChannelProcessResult,
    GuildProcessResult,
    _filter_viewable_text_channels,
)
from discord_archive.ingest.mappers.channel import (
    CHANNEL_TYPE_CATEGORY,
    CHANNEL_TYPE_FORUM,
    CHANNEL_TYPE_PUBLIC_THREAD,
    CHANNEL_TYPE_TEXT,
    CHANNEL_TYPE_VOICE,
)
from discord_archive.utils.permissions import CONNECT, VIEW_CHANNEL


class TestFilterViewableTextChannels:
    """Tests for _filter_viewable_text_channels function."""

    @pytest.fixture
    def permission_context(self) -> PermissionContext:
        """Create a permission context with VIEW_CHANNEL permission."""
        return PermissionContext(
            user_id=123456789,
            user_roles=[],
            base_permissions=VIEW_CHANNEL,
            guild_id=999999999,
        )

    def test_includes_text_channels_with_permission(
        self, permission_context: PermissionContext
    ) -> None:
        """Should include text channels when user has VIEW_CHANNEL."""
        channels = [
            {"id": "1", "type": CHANNEL_TYPE_TEXT, "name": "general"},
            {"id": "2", "type": CHANNEL_TYPE_TEXT, "name": "chat"},
        ]

        result = _filter_viewable_text_channels(channels, permission_context)

        assert len(result) == 2

    def test_includes_threads_with_permission(
        self, permission_context: PermissionContext
    ) -> None:
        """Should include thread channels."""
        channels = [
            {"id": "1", "type": CHANNEL_TYPE_PUBLIC_THREAD, "name": "thread"},
        ]

        result = _filter_viewable_text_channels(channels, permission_context)

        assert len(result) == 1

    def test_includes_voice_channels_with_connect(
        self,
    ) -> None:
        """Should include voice channels when user has VIEW_CHANNEL and CONNECT."""
        ctx = PermissionContext(
            user_id=123456789,
            user_roles=[],
            base_permissions=VIEW_CHANNEL | CONNECT,
            guild_id=999999999,
        )
        channels = [
            {"id": "1", "type": CHANNEL_TYPE_VOICE, "name": "voice"},
        ]

        result = _filter_viewable_text_channels(channels, ctx)

        assert len(result) == 1

    def test_excludes_voice_channels_without_connect(
        self, permission_context: PermissionContext
    ) -> None:
        """Should exclude voice channels when user lacks CONNECT permission."""
        # permission_context only has VIEW_CHANNEL, no CONNECT
        channels = [
            {"id": "1", "type": CHANNEL_TYPE_VOICE, "name": "voice"},
        ]

        result = _filter_viewable_text_channels(channels, permission_context)

        assert len(result) == 0

    def test_excludes_categories(self, permission_context: PermissionContext) -> None:
        """Should exclude category channels."""
        channels = [
            {"id": "1", "type": CHANNEL_TYPE_CATEGORY, "name": "Category"},
        ]

        result = _filter_viewable_text_channels(channels, permission_context)

        assert len(result) == 0

    def test_excludes_forum_channels(
        self, permission_context: PermissionContext
    ) -> None:
        """Should exclude forum channels (not text-based themselves)."""
        channels = [
            {"id": "1", "type": CHANNEL_TYPE_FORUM, "name": "forum"},
        ]

        result = _filter_viewable_text_channels(channels, permission_context)

        assert len(result) == 0

    def test_excludes_channels_without_permission(self) -> None:
        """Should exclude channels when user lacks VIEW_CHANNEL."""
        ctx = PermissionContext(
            user_id=123,
            user_roles=[],
            base_permissions=0,  # No permissions
            guild_id=999,
        )
        channels = [
            {"id": "1", "type": CHANNEL_TYPE_TEXT, "name": "secret"},
        ]

        result = _filter_viewable_text_channels(channels, ctx)

        assert len(result) == 0

    def test_respects_channel_overwrites(self) -> None:
        """Should respect channel permission overwrites."""
        ctx = PermissionContext(
            user_id=123,
            user_roles=[111],
            base_permissions=0,  # No base permissions
            guild_id=999,
        )
        channels = [
            {
                "id": "1",
                "type": CHANNEL_TYPE_TEXT,
                "name": "allowed",
                "permission_overwrites": [
                    {
                        "id": "111",  # Role gives access
                        "type": 0,
                        "allow": str(VIEW_CHANNEL),
                        "deny": "0",
                    }
                ],
            },
            {
                "id": "2",
                "type": CHANNEL_TYPE_TEXT,
                "name": "denied",
                "permission_overwrites": [],  # No overwrite, no access
            },
        ]

        result = _filter_viewable_text_channels(channels, ctx)

        assert len(result) == 1
        assert result[0]["name"] == "allowed"


class TestDataclasses:
    """Tests for dataclass structures."""

    def test_guild_process_result_defaults(self) -> None:
        """GuildProcessResult should correctly store values."""
        result = GuildProcessResult(channels_processed=5, messages_ingested=100)
        assert result.channels_processed == 5
        assert result.messages_ingested == 100

    def test_channel_process_result(self) -> None:
        """ChannelProcessResult should store messages_ingested."""
        result = ChannelProcessResult(messages_ingested=100)
        assert result.messages_ingested == 100
