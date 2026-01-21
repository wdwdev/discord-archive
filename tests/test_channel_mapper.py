"""Tests for discord_archive.ingest.mappers.channel module."""

from __future__ import annotations

import pytest

from discord_archive.ingest.mappers.channel import (
    CHANNEL_TYPE_ANNOUNCEMENT,
    CHANNEL_TYPE_ANNOUNCEMENT_THREAD,
    CHANNEL_TYPE_CATEGORY,
    CHANNEL_TYPE_DM,
    CHANNEL_TYPE_FORUM,
    CHANNEL_TYPE_MEDIA,
    CHANNEL_TYPE_PRIVATE_THREAD,
    CHANNEL_TYPE_PUBLIC_THREAD,
    CHANNEL_TYPE_STAGE,
    CHANNEL_TYPE_TEXT,
    CHANNEL_TYPE_VOICE,
    channel_type_name,
    is_text_based,
    is_thread,
    map_channel,
)


class TestMapChannel:
    """Tests for map_channel function."""

    @pytest.fixture
    def minimal_channel_data(self) -> dict:
        """Minimal valid channel data from Discord API."""
        return {
            "id": "123456789",
            "guild_id": "987654321",
            "type": 0,
            "name": "general",
        }

    def test_maps_basic_fields(self, minimal_channel_data: dict) -> None:
        """Should correctly map basic channel fields."""
        result = map_channel(minimal_channel_data)

        assert result.channel_id == 123456789
        assert result.guild_id == 987654321
        assert result.type == 0
        assert result.name == "general"

    def test_maps_parent_id_when_valid(self, minimal_channel_data: dict) -> None:
        """Should include parent_id when in valid_parent_ids set."""
        minimal_channel_data["parent_id"] = "555666777"
        valid_parents = {555666777}

        result = map_channel(minimal_channel_data, valid_parent_ids=valid_parents)

        assert result.parent_id == 555666777

    def test_nulls_parent_id_when_invalid(self, minimal_channel_data: dict) -> None:
        """Should null parent_id when not in valid_parent_ids set."""
        minimal_channel_data["parent_id"] = "555666777"
        valid_parents = {111222333}  # Different ID

        result = map_channel(minimal_channel_data, valid_parent_ids=valid_parents)

        assert result.parent_id is None

    def test_keeps_parent_id_when_no_validation(
        self, minimal_channel_data: dict
    ) -> None:
        """Should keep parent_id when valid_parent_ids is None."""
        minimal_channel_data["parent_id"] = "555666777"

        result = map_channel(minimal_channel_data, valid_parent_ids=None)

        assert result.parent_id == 555666777

    def test_maps_thread_specific_fields(self) -> None:
        """Should map thread-specific fields."""
        thread_data = {
            "id": "111",
            "guild_id": "222",
            "type": CHANNEL_TYPE_PUBLIC_THREAD,
            "name": "test-thread",
            "parent_id": "333",
            "owner_id": "444",
            "thread_metadata": {"archived": False},
            "message_count": 50,
            "member_count": 10,
        }

        result = map_channel(thread_data)

        assert result.owner_id == 444
        assert result.thread_metadata == {"archived": False}
        assert result.message_count == 50
        assert result.member_count == 10

    def test_maps_forum_fields(self) -> None:
        """Should map forum-specific fields."""
        forum_data = {
            "id": "111",
            "guild_id": "222",
            "type": CHANNEL_TYPE_FORUM,
            "name": "forum-channel",
            "available_tags": [{"id": "1", "name": "tag1"}],
            "default_reaction_emoji": {"emoji_name": "👍"},
            "default_sort_order": 0,
            "default_forum_layout": 1,
        }

        result = map_channel(forum_data)

        assert result.available_tags == [{"id": "1", "name": "tag1"}]
        assert result.default_reaction_emoji == {"emoji_name": "👍"}
        assert result.default_sort_order == 0
        assert result.default_forum_layout == 1

    def test_maps_applied_tags_as_integers(self) -> None:
        """Should convert applied_tags strings to integers."""
        thread_data = {
            "id": "111",
            "guild_id": "222",
            "type": CHANNEL_TYPE_PUBLIC_THREAD,
            "name": "tagged-thread",
            "applied_tags": ["123", "456", "789"],
        }

        result = map_channel(thread_data)

        assert result.applied_tags == [123, 456, 789]


class TestIsTextBased:
    """Tests for is_text_based function."""

    def test_text_channels_are_text_based(self) -> None:
        """Text channels should be text-based."""
        assert is_text_based(CHANNEL_TYPE_TEXT) is True
        assert is_text_based(CHANNEL_TYPE_DM) is True
        assert is_text_based(CHANNEL_TYPE_ANNOUNCEMENT) is True

    def test_threads_are_text_based(self) -> None:
        """Thread channels should be text-based."""
        assert is_text_based(CHANNEL_TYPE_PUBLIC_THREAD) is True
        assert is_text_based(CHANNEL_TYPE_PRIVATE_THREAD) is True
        assert is_text_based(CHANNEL_TYPE_ANNOUNCEMENT_THREAD) is True

    def test_voice_and_stage_are_text_based(self) -> None:
        """Voice and stage channels should be text-based (they have text chat)."""
        assert is_text_based(CHANNEL_TYPE_VOICE) is True
        assert is_text_based(CHANNEL_TYPE_STAGE) is True

    def test_category_is_not_text_based(self) -> None:
        """Category channels should not be text-based."""
        assert is_text_based(CHANNEL_TYPE_CATEGORY) is False

    def test_forum_is_not_text_based(self) -> None:
        """Forum channels themselves are not text-based (threads are)."""
        assert is_text_based(CHANNEL_TYPE_FORUM) is False
        assert is_text_based(CHANNEL_TYPE_MEDIA) is False


class TestIsThread:
    """Tests for is_thread function."""

    def test_threads_are_threads(self) -> None:
        """Thread channel types should return True."""
        assert is_thread(CHANNEL_TYPE_PUBLIC_THREAD) is True
        assert is_thread(CHANNEL_TYPE_PRIVATE_THREAD) is True
        assert is_thread(CHANNEL_TYPE_ANNOUNCEMENT_THREAD) is True

    def test_regular_channels_are_not_threads(self) -> None:
        """Regular channels should return False."""
        assert is_thread(CHANNEL_TYPE_TEXT) is False
        assert is_thread(CHANNEL_TYPE_VOICE) is False
        assert is_thread(CHANNEL_TYPE_CATEGORY) is False
        assert is_thread(CHANNEL_TYPE_FORUM) is False


class TestChannelTypeName:
    """Tests for channel_type_name function."""

    def test_known_types(self) -> None:
        """Should return correct names for known types."""
        assert channel_type_name(CHANNEL_TYPE_TEXT) == "text"
        assert channel_type_name(CHANNEL_TYPE_VOICE) == "voice"
        assert channel_type_name(CHANNEL_TYPE_CATEGORY) == "category"
        assert channel_type_name(CHANNEL_TYPE_FORUM) == "forum"
        assert channel_type_name(CHANNEL_TYPE_PUBLIC_THREAD) == "public_thread"

    def test_unknown_type(self) -> None:
        """Should return 'unknown(N)' for unknown types."""
        assert channel_type_name(999) == "unknown(999)"
