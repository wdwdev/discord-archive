"""Discord permission utilities.

Calculates channel permissions based on roles and permission overwrites.
Used to pre-filter channels before making API requests to avoid 403 errors.
"""

from __future__ import annotations


# Permission bit flags
# See: https://discord.com/developers/docs/topics/permissions#permissions-bitwise-permission-flags
VIEW_CHANNEL = 0x0000000000000400  # 1 << 10
ADMINISTRATOR = 0x0000000000000008  # 1 << 3
READ_MESSAGE_HISTORY = 0x0000000000010000  # 1 << 16
MANAGE_THREADS = 0x0000000400000000  # 1 << 34
CONNECT = 0x0000000000100000  # 1 << 20 (voice channel connect)


def compute_base_permissions(
    user_roles: list[int],
    guild_roles: dict[int, int],
    everyone_role_id: int,
) -> int:
    """Compute base guild-level permissions for a user.

    Args:
        user_roles: List of role IDs the user has
        guild_roles: Mapping of role_id -> permission bits
        everyone_role_id: The guild's @everyone role ID (same as guild_id)

    Returns:
        Combined permission bits from all roles
    """
    # Start with @everyone permissions
    permissions = guild_roles.get(everyone_role_id, 0)

    # OR all user role permissions
    for role_id in user_roles:
        permissions |= guild_roles.get(role_id, 0)

    # Administrator overrides everything
    if permissions & ADMINISTRATOR:
        return 0xFFFFFFFFFFFFFFFF  # All permissions

    return permissions


def compute_channel_permissions(
    user_id: int,
    base_permissions: int,
    channel_overwrites: list[dict],
    user_roles: list[int],
    everyone_role_id: int,
) -> int:
    """Compute final channel-level permissions for a user.

    Applies channel permission_overwrites in order:
    1. @everyone deny -> @everyone allow
    2. Role deny (combined) -> Role allow (combined)
    3. Member deny -> Member allow

    Args:
        user_id: The user's ID
        base_permissions: Pre-computed base permissions from roles
        channel_overwrites: List of permission overwrite objects from channel
        user_roles: List of role IDs the user has
        everyone_role_id: The guild's @everyone role ID

    Returns:
        Final permission bits for the channel
    """
    # Administrator bypasses all overwrites
    if base_permissions & ADMINISTRATOR:
        return 0xFFFFFFFFFFFFFFFF

    permissions = base_permissions

    # Find role overwrites (combine all role denies/allows)
    role_allow = 0
    role_deny = 0
    member_allow = 0
    member_deny = 0
    has_member_overwrite = False

    for overwrite in channel_overwrites:
        overwrite_id = int(overwrite["id"])
        overwrite_type = overwrite["type"]  # 0 = role, 1 = member
        allow = int(overwrite.get("allow", 0) or 0)
        deny = int(overwrite.get("deny", 0) or 0)

        if overwrite_type == 0:  # Role
            if overwrite_id == everyone_role_id:
                # @everyone overwrite - apply first
                permissions &= ~deny
                permissions |= allow
            elif overwrite_id in user_roles:
                # User's role - accumulate
                role_deny |= deny
                role_allow |= allow
        elif overwrite_type == 1 and overwrite_id == user_id:
            # Member-specific overwrite - will be applied last
            member_deny = deny
            member_allow = allow
            has_member_overwrite = True

    # Apply role overwrites (after @everyone)
    permissions &= ~role_deny
    permissions |= role_allow

    # Apply member overwrites last (highest priority)
    if has_member_overwrite:
        permissions &= ~member_deny
        permissions |= member_allow

    return permissions


def can_view_channel(permissions: int) -> bool:
    """Check if permissions include VIEW_CHANNEL."""
    return bool(permissions & VIEW_CHANNEL)


def can_connect_voice(permissions: int) -> bool:
    """Check if permissions include CONNECT (for voice channels)."""
    return bool(permissions & CONNECT)


def can_access_channel(permissions: int, channel_type: int) -> bool:
    """Check if user can access a channel's messages.

    For voice/stage channels (type 2, 13), CONNECT permission is required
    in addition to VIEW_CHANNEL to see messages.

    Args:
        permissions: Computed channel permissions
        channel_type: Discord channel type (0=text, 2=voice, 13=stage, etc.)

    Returns:
        True if user can access the channel's messages
    """
    if not can_view_channel(permissions):
        return False

    # Voice channel (type 2) and Stage channel (type 13) require CONNECT
    if channel_type in (2, 13):
        return can_connect_voice(permissions)

    return True


def can_read_history(permissions: int) -> bool:
    """Check if permissions include READ_MESSAGE_HISTORY."""
    return bool(permissions & READ_MESSAGE_HISTORY)


def can_manage_threads(permissions: int) -> bool:
    """Check if permissions include MANAGE_THREADS (needed for private archived threads)."""
    return bool(permissions & MANAGE_THREADS)


def build_role_permissions_map(roles_data: list[dict]) -> dict[int, int]:
    """Build a mapping of role_id -> permissions from guild roles data.

    Args:
        roles_data: List of role objects from guild data

    Returns:
        Dict mapping role_id to permission bits
    """
    return {
        int(role["id"]): int(role.get("permissions", 0) or 0) for role in roles_data
    }
