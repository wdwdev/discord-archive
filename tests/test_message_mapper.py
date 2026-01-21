"""Tests for discord_archive.ingest.mappers.message module."""

from __future__ import annotations

import pytest

from discord_archive.ingest.mappers.message import (
    _sanitize_null_bytes,
    map_attachment,
    map_message,
    map_messages,
    map_reaction,
)


class TestSanitizeNullBytes:
    """Tests for _sanitize_null_bytes function."""

    def test_removes_null_bytes_from_string(self) -> None:
        """Should remove NULL bytes from strings."""
        assert _sanitize_null_bytes("hello\x00world") == "helloworld"
        assert _sanitize_null_bytes("\x00test\x00") == "test"

    def test_handles_clean_string(self) -> None:
        """Should pass through clean strings unchanged."""
        assert _sanitize_null_bytes("hello world") == "hello world"

    def test_recursively_sanitizes_dict(self) -> None:
        """Should recursively sanitize dict values."""
        data = {"key": "value\x00", "nested": {"inner": "\x00test"}}
        result = _sanitize_null_bytes(data)
        assert result == {"key": "value", "nested": {"inner": "test"}}

    def test_recursively_sanitizes_list(self) -> None:
        """Should recursively sanitize list items."""
        data = ["a\x00b", {"key": "\x00value"}]
        result = _sanitize_null_bytes(data)
        assert result == ["ab", {"key": "value"}]

    def test_passes_through_non_string_types(self) -> None:
        """Should pass through integers, None, booleans unchanged."""
        assert _sanitize_null_bytes(123) == 123
        assert _sanitize_null_bytes(None) is None
        assert _sanitize_null_bytes(True) is True


class TestMapMessage:
    """Tests for map_message function."""

    @pytest.fixture
    def minimal_message_data(self) -> dict:
        """Minimal valid message data from Discord API."""
        return {
            "id": "123456789",
            "channel_id": "987654321",
            "author": {"id": "111222333"},
            "content": "Hello, world!",
            "timestamp": "2024-01-15T10:30:00.000000+00:00",
            "type": 0,
        }

    def test_maps_basic_fields(self, minimal_message_data: dict) -> None:
        """Should correctly map basic message fields."""
        result = map_message(minimal_message_data)

        assert result.message_id == 123456789
        assert result.channel_id == 987654321
        assert result.author_id == 111222333
        assert result.content == "Hello, world!"
        assert result.type == 0

    def test_uses_provided_guild_id(self, minimal_message_data: dict) -> None:
        """Should use provided guild_id when not in payload."""
        result = map_message(minimal_message_data, guild_id=555666777)
        assert result.guild_id == 555666777

    def test_prefers_payload_guild_id(self, minimal_message_data: dict) -> None:
        """Should prefer guild_id from payload over provided value."""
        minimal_message_data["guild_id"] = "999888777"
        result = map_message(minimal_message_data, guild_id=111)
        assert result.guild_id == 999888777

    def test_extracts_mentions(self, minimal_message_data: dict) -> None:
        """Should extract user mention IDs from mentions array."""
        minimal_message_data["mentions"] = [
            {"id": "111"},
            {"id": "222"},
        ]
        result = map_message(minimal_message_data)
        assert result.mentions == [111, 222]

    def test_extracts_mention_roles(self, minimal_message_data: dict) -> None:
        """Should extract role mention IDs."""
        minimal_message_data["mention_roles"] = ["333", "444"]
        result = map_message(minimal_message_data)
        assert result.mention_roles == [333, 444]

    def test_handles_message_reference(self, minimal_message_data: dict) -> None:
        """Should extract referenced_message_id from message_reference."""
        minimal_message_data["message_reference"] = {
            "message_id": "888999000",
            "channel_id": "987654321",
        }
        result = map_message(minimal_message_data)
        assert result.referenced_message_id == 888999000

    def test_sanitizes_null_bytes(self) -> None:
        """Should sanitize NULL bytes from content."""
        data = {
            "id": "123",
            "channel_id": "456",
            "author": {"id": "789"},
            "content": "Hello\x00World",
            "timestamp": "2024-01-15T10:30:00+00:00",
        }
        result = map_message(data)
        assert result.content == "HelloWorld"


class TestMapAttachment:
    """Tests for map_attachment function."""

    @pytest.fixture
    def attachment_data(self) -> dict:
        """Sample attachment data from Discord API."""
        return {
            "id": "999888777",
            "filename": "image.png",
            "size": 12345,
            "url": "https://cdn.discord.com/attachments/...",
            "proxy_url": "https://media.discord.com/attachments/...",
            "content_type": "image/png",
            "height": 480,
            "width": 640,
        }

    def test_maps_required_fields(self, attachment_data: dict) -> None:
        """Should correctly map required attachment fields."""
        result = map_attachment(attachment_data, message_id=123456)

        assert result.attachment_id == 999888777
        assert result.message_id == 123456
        assert result.filename == "image.png"
        assert result.size == 12345
        assert result.url == "https://cdn.discord.com/attachments/..."

    def test_maps_optional_fields(self, attachment_data: dict) -> None:
        """Should correctly map optional attachment fields."""
        result = map_attachment(attachment_data, message_id=123456)

        assert result.content_type == "image/png"
        assert result.height == 480
        assert result.width == 640
        assert result.proxy_url == "https://media.discord.com/attachments/..."

    def test_handles_missing_optional_fields(self) -> None:
        """Should handle attachments with only required fields."""
        minimal_data = {
            "id": "111",
            "filename": "file.txt",
            "size": 100,
            "url": "https://example.com/file.txt",
        }
        result = map_attachment(minimal_data, message_id=999)

        assert result.content_type is None
        assert result.height is None
        assert result.width is None


class TestMapReaction:
    """Tests for map_reaction function."""

    def test_maps_unicode_emoji(self) -> None:
        """Should correctly map unicode emoji reactions."""
        data = {
            "emoji": {"id": None, "name": "👍"},
            "count": 5,
        }
        result = map_reaction(data, message_id=123)

        assert result.emoji_key == "unicode:👍"
        assert result.emoji_id is None
        assert result.emoji_name == "👍"
        assert result.count == 5

    def test_maps_custom_emoji(self) -> None:
        """Should correctly map custom emoji reactions."""
        data = {
            "emoji": {"id": "999888777", "name": "custom_emoji", "animated": True},
            "count": 3,
        }
        result = map_reaction(data, message_id=123)

        assert result.emoji_key == "custom:999888777"
        assert result.emoji_id == 999888777
        assert result.emoji_name == "custom_emoji"
        assert result.emoji_animated is True
        assert result.count == 3

    def test_default_count_is_one(self) -> None:
        """Should default to count=1 if not provided."""
        data = {"emoji": {"id": None, "name": "🎉"}}
        result = map_reaction(data, message_id=123)
        assert result.count == 1


class TestMapMessages:
    """Tests for map_messages function."""

    def test_maps_multiple_messages(self) -> None:
        """Should map a list of messages with their attachments and reactions."""
        data_list = [
            {
                "id": "111",
                "channel_id": "999",
                "author": {"id": "555"},
                "content": "First message",
                "timestamp": "2024-01-15T10:00:00+00:00",
                "attachments": [
                    {"id": "1001", "filename": "a.png", "size": 100, "url": "..."},
                ],
                "reactions": [
                    {"emoji": {"id": None, "name": "👍"}, "count": 2},
                ],
            },
            {
                "id": "222",
                "channel_id": "999",
                "author": {"id": "666"},
                "content": "Second message",
                "timestamp": "2024-01-15T11:00:00+00:00",
            },
        ]

        messages, attachments, reactions = map_messages(data_list, guild_id=888)

        assert len(messages) == 2
        assert len(attachments) == 1
        assert len(reactions) == 1

        assert messages[0].message_id == 111
        assert messages[1].message_id == 222
        assert attachments[0].attachment_id == 1001
        assert reactions[0].emoji_name == "👍"

    def test_empty_list(self) -> None:
        """Should handle empty message list."""
        messages, attachments, reactions = map_messages([])

        assert messages == []
        assert attachments == []
        assert reactions == []
