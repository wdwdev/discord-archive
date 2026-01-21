"""Tests for discord_archive.utils.permissions module."""

from __future__ import annotations

import pytest

from discord_archive.utils.permissions import (
    ADMINISTRATOR,
    MANAGE_THREADS,
    READ_MESSAGE_HISTORY,
    VIEW_CHANNEL,
    build_role_permissions_map,
    can_manage_threads,
    can_read_history,
    can_view_channel,
    compute_base_permissions,
    compute_channel_permissions,
)


class TestBuildRolePermissionsMap:
    """Tests for build_role_permissions_map."""

    def test_builds_map_from_roles(self, sample_roles: list[dict]) -> None:
        """Should create a dict mapping role_id -> permissions."""
        result = build_role_permissions_map(sample_roles)

        assert result[123456789] == 104324673
        assert result[111111111] == 1024
        assert result[222222222] == 17179869184
        assert result[333333333] == 8

    def test_empty_roles(self) -> None:
        """Should return empty dict for empty input."""
        result = build_role_permissions_map([])
        assert result == {}


class TestComputeBasePermissions:
    """Tests for compute_base_permissions."""

    def test_everyone_only(self, sample_roles: list[dict], guild_id: int) -> None:
        """User with no roles should have @everyone permissions only."""
        role_map = build_role_permissions_map(sample_roles)
        result = compute_base_permissions([], role_map, guild_id)

        assert result == 104324673

    def test_combines_role_permissions(
        self, sample_roles: list[dict], guild_id: int
    ) -> None:
        """User with roles should have combined permissions."""
        role_map = build_role_permissions_map(sample_roles)
        result = compute_base_permissions([111111111], role_map, guild_id)

        # @everyone | Member role
        assert result == (104324673 | 1024)

    def test_admin_gets_all_permissions(
        self, sample_roles: list[dict], guild_id: int
    ) -> None:
        """Administrator role should grant all permissions."""
        role_map = build_role_permissions_map(sample_roles)
        result = compute_base_permissions([333333333], role_map, guild_id)

        assert result == 0xFFFFFFFFFFFFFFFF  # All permissions


class TestComputeChannelPermissions:
    """Tests for compute_channel_permissions."""

    def test_admin_bypasses_overwrites(self, guild_id: int) -> None:
        """Administrator should bypass all channel overwrites."""
        base_perms = 0xFFFFFFFFFFFFFFFF  # Admin perms
        overwrites = [
            {"id": str(guild_id), "type": 0, "allow": "0", "deny": str(VIEW_CHANNEL)}
        ]

        result = compute_channel_permissions(
            user_id=999,
            base_permissions=base_perms,
            channel_overwrites=overwrites,
            user_roles=[],
            everyone_role_id=guild_id,
        )

        assert result == 0xFFFFFFFFFFFFFFFF

    def test_everyone_deny(self, guild_id: int) -> None:
        """@everyone deny should remove permission."""
        base_perms = VIEW_CHANNEL  # Has VIEW_CHANNEL
        overwrites = [
            {"id": str(guild_id), "type": 0, "allow": "0", "deny": str(VIEW_CHANNEL)}
        ]

        result = compute_channel_permissions(
            user_id=999,
            base_permissions=base_perms,
            channel_overwrites=overwrites,
            user_roles=[],
            everyone_role_id=guild_id,
        )

        assert not can_view_channel(result)

    def test_role_allow_overrides_everyone_deny(self, guild_id: int) -> None:
        """Role allow should override @everyone deny."""
        base_perms = VIEW_CHANNEL
        overwrites = [
            {"id": str(guild_id), "type": 0, "allow": "0", "deny": str(VIEW_CHANNEL)},
            {"id": "111111111", "type": 0, "allow": str(VIEW_CHANNEL), "deny": "0"},
        ]

        result = compute_channel_permissions(
            user_id=999,
            base_permissions=base_perms,
            channel_overwrites=overwrites,
            user_roles=[111111111],
            everyone_role_id=guild_id,
        )

        assert can_view_channel(result)

    def test_member_overwrite_takes_priority(self, guild_id: int) -> None:
        """Member-specific overwrite should take highest priority."""
        base_perms = 0  # No permissions
        overwrites = [
            {"id": str(guild_id), "type": 0, "allow": "0", "deny": str(VIEW_CHANNEL)},
            {
                "id": "999",
                "type": 1,
                "allow": str(VIEW_CHANNEL),
                "deny": "0",
            },  # Member allow
        ]

        result = compute_channel_permissions(
            user_id=999,
            base_permissions=base_perms,
            channel_overwrites=overwrites,
            user_roles=[],
            everyone_role_id=guild_id,
        )

        assert can_view_channel(result)


class TestPermissionChecks:
    """Tests for permission check functions."""

    def test_can_view_channel(self) -> None:
        """Should correctly check VIEW_CHANNEL bit."""
        assert can_view_channel(VIEW_CHANNEL)
        assert can_view_channel(VIEW_CHANNEL | READ_MESSAGE_HISTORY)
        assert not can_view_channel(0)
        assert not can_view_channel(READ_MESSAGE_HISTORY)

    def test_can_read_history(self) -> None:
        """Should correctly check READ_MESSAGE_HISTORY bit."""
        assert can_read_history(READ_MESSAGE_HISTORY)
        assert can_read_history(VIEW_CHANNEL | READ_MESSAGE_HISTORY)
        assert not can_read_history(0)
        assert not can_read_history(VIEW_CHANNEL)

    def test_can_manage_threads(self) -> None:
        """Should correctly check MANAGE_THREADS bit."""
        assert can_manage_threads(MANAGE_THREADS)
        assert can_manage_threads(MANAGE_THREADS | VIEW_CHANNEL)
        assert not can_manage_threads(0)
        assert not can_manage_threads(VIEW_CHANNEL)


class TestPermissionConstants:
    """Tests for permission bit constants."""

    def test_permission_bits(self) -> None:
        """Permission constants should match Discord's bit flags."""
        assert VIEW_CHANNEL == 1 << 10
        assert ADMINISTRATOR == 1 << 3
        assert READ_MESSAGE_HISTORY == 1 << 16
        assert MANAGE_THREADS == 1 << 34
