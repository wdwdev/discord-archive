"""Tests for discord_archive.utils.snowflake module."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from discord_archive.utils.snowflake import (
    DISCORD_EPOCH,
    datetime_to_snowflake,
    snowflake_to_datetime,
)


def test_snowflake_to_datetime_known_value() -> None:
    """Should convert a known snowflake to the expected datetime."""
    ms = DISCORD_EPOCH + 1_234_567
    snowflake = (ms - DISCORD_EPOCH) << 22

    result = snowflake_to_datetime(snowflake)

    assert result == datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def test_datetime_to_snowflake_round_trip_ms_precision() -> None:
    """Round-trip should preserve time at millisecond precision."""
    dt = datetime(2024, 1, 15, 10, 30, 0, 123000, tzinfo=timezone.utc)

    snowflake = datetime_to_snowflake(dt)
    result = snowflake_to_datetime(snowflake)

    assert result == dt


def test_datetime_to_snowflake_rejects_naive_datetime() -> None:
    """Should reject naive datetimes."""
    dt = datetime(2024, 1, 15, 10, 30, 0, 123000)

    with pytest.raises(ValueError, match="timezone-aware"):
        datetime_to_snowflake(dt)
