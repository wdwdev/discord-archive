"""Shared fixtures for discord-archive tests."""

from __future__ import annotations

import pytest


# Sample Discord role data for testing permissions
@pytest.fixture
def sample_roles() -> list[dict]:
    """Sample guild roles for permission testing."""
    return [
        {
            "id": "123456789",  # @everyone role (same as guild_id)
            "name": "@everyone",
            "permissions": "104324673",  # Basic permissions, no VIEW_CHANNEL
        },
        {
            "id": "111111111",
            "name": "Member",
            "permissions": "1024",  # VIEW_CHANNEL only
        },
        {
            "id": "222222222",
            "name": "Moderator",
            "permissions": "17179869184",  # MANAGE_THREADS (1 << 34)
        },
        {
            "id": "333333333",
            "name": "Admin",
            "permissions": "8",  # ADMINISTRATOR
        },
    ]


@pytest.fixture
def guild_id() -> int:
    """Sample guild ID (also @everyone role ID)."""
    return 123456789


@pytest.fixture
def sample_channel_overwrites() -> list[dict]:
    """Sample channel permission overwrites."""
    return [
        {
            "id": "123456789",  # @everyone
            "type": 0,  # role
            "allow": "0",
            "deny": "1024",  # Deny VIEW_CHANNEL
        },
        {
            "id": "111111111",  # Member role
            "type": 0,  # role
            "allow": "1024",  # Allow VIEW_CHANNEL
            "deny": "0",
        },
    ]


@pytest.fixture
def sample_member_overwrite() -> dict:
    """Sample member-specific permission overwrite."""
    return {
        "id": "999999999",  # User ID
        "type": 1,  # member
        "allow": "1024",  # Allow VIEW_CHANNEL
        "deny": "0",
    }
