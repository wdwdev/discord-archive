"""Tests for other mapper modules (role, emoji, sticker, user, guild_member)."""

from __future__ import annotations

from decimal import Decimal

from discord_archive.ingest.mappers.emoji import map_emoji
from discord_archive.ingest.mappers.role import map_role
from discord_archive.ingest.mappers.sticker import map_sticker
from discord_archive.ingest.mappers.user import map_user


class TestMapRole:
    """Tests for map_role function."""

    def test_maps_basic_fields(self) -> None:
        """Should map basic role fields."""
        data = {
            "id": "123456789",
            "name": "Admin",
            "color": 16711680,
            "hoist": True,
            "position": 5,
            "permissions": "8",  # Administrator
            "mentionable": True,
        }

        result = map_role(data, guild_id=999)

        assert result.role_id == 123456789
        assert result.guild_id == 999
        assert result.name == "Admin"
        assert result.color == 16711680
        assert result.hoist is True
        assert result.position == 5
        assert result.mentionable is True

    def test_parses_permissions_as_decimal(self) -> None:
        """Should parse permissions as Decimal for large bitfield support."""
        data = {
            "id": "123",
            "name": "Test",
            "permissions": "1099511627775",  # Large permission value
        }

        result = map_role(data, guild_id=999)

        assert result.permissions == Decimal("1099511627775")

    def test_handles_optional_fields(self) -> None:
        """Should handle roles with only required fields."""
        data = {"id": "123", "name": "Basic"}

        result = map_role(data, guild_id=999)

        assert result.color == 0
        assert result.hoist is False
        assert result.icon is None


class TestMapEmoji:
    """Tests for map_emoji function."""

    def test_maps_basic_fields(self) -> None:
        """Should map basic emoji fields."""
        data = {
            "id": "123456789",
            "name": "custom_emoji",
            "animated": True,
            "available": True,
        }

        result = map_emoji(data, guild_id=999)

        assert result.emoji_id == 123456789
        assert result.guild_id == 999
        assert result.name == "custom_emoji"
        assert result.animated is True
        assert result.available is True

    def test_extracts_roles_as_integers(self) -> None:
        """Should convert role IDs to integers."""
        data = {
            "id": "123",
            "name": "emoji",
            "roles": ["111", "222", "333"],
        }

        result = map_emoji(data, guild_id=999)

        assert result.roles == [111, 222, 333]

    def test_extracts_creator_user_id(self) -> None:
        """Should extract user_id from user object."""
        data = {
            "id": "123",
            "name": "emoji",
            "user": {"id": "555666777"},
        }

        result = map_emoji(data, guild_id=999)

        assert result.user_id == 555666777


class TestMapSticker:
    """Tests for map_sticker function."""

    def test_maps_guild_sticker(self) -> None:
        """Should map guild sticker fields."""
        data = {
            "id": "123456789",
            "name": "custom_sticker",
            "guild_id": "999",
            "type": 2,  # GUILD
            "format_type": 1,  # PNG
            "description": "A cool sticker",
            "tags": "happy,smile",
        }

        result = map_sticker(data)

        assert result.sticker_id == 123456789
        assert result.guild_id == 999
        assert result.name == "custom_sticker"
        assert result.type == 2
        assert result.format_type == 1
        assert result.description == "A cool sticker"
        assert result.tags == "happy,smile"

    def test_handles_standard_sticker(self) -> None:
        """Should handle standard (Nitro) stickers with pack_id."""
        data = {
            "id": "123",
            "name": "nitro_sticker",
            "type": 1,  # STANDARD
            "format_type": 3,  # LOTTIE
            "pack_id": "456",
        }

        result = map_sticker(data)

        assert result.pack_id == 456
        assert result.guild_id is None


class TestMapUser:
    """Tests for map_user function."""

    def test_maps_basic_fields(self) -> None:
        """Should map basic user fields."""
        data = {
            "id": "123456789",
            "username": "testuser",
            "discriminator": "0",
            "global_name": "Test User",
            "avatar": "abc123",
            "bot": False,
        }

        result = map_user(data)

        assert result.user_id == 123456789
        assert result.username == "testuser"
        assert result.discriminator == "0"
        assert result.global_name == "Test User"
        assert result.avatar == "abc123"
        assert result.bot is False

    def test_handles_bot_user(self) -> None:
        """Should correctly identify bot users."""
        data = {
            "id": "123",
            "username": "bot_user",
            "bot": True,
        }

        result = map_user(data)

        assert result.bot is True

    def test_handles_minimal_user(self) -> None:
        """Should handle partial user objects (e.g., from mentions)."""
        data = {"id": "123", "username": "partial"}

        result = map_user(data)

        assert result.user_id == 123
        assert result.username == "partial"
        assert result.discriminator is None
        assert result.global_name is None
