"""Tests for discord_archive.utils.time module."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from discord_archive.utils.time import parse_iso8601, utcnow


class TestUtcnow:
    """Tests for utcnow function."""

    def test_returns_timezone_aware_datetime(self) -> None:
        """Should return a timezone-aware UTC datetime."""
        result = utcnow()
        assert result.tzinfo is not None
        assert result.tzinfo == timezone.utc

    def test_returns_current_time(self) -> None:
        """Should return approximately current time."""
        before = datetime.now(timezone.utc)
        result = utcnow()
        after = datetime.now(timezone.utc)

        assert before <= result <= after


class TestParseIso8601:
    """Tests for parse_iso8601 function."""

    def test_parses_z_suffix(self) -> None:
        """Should parse timestamps with Z suffix."""
        result = parse_iso8601("2024-01-15T10:30:00.000000Z")

        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 10
        assert result.minute == 30
        assert result.tzinfo == timezone.utc

    def test_parses_plus_zero_offset(self) -> None:
        """Should parse timestamps with +00:00 offset."""
        result = parse_iso8601("2024-01-15T10:30:00+00:00")

        assert result is not None
        assert result.tzinfo == timezone.utc

    def test_returns_none_for_none(self) -> None:
        """Should return None for None input."""
        result = parse_iso8601(None)
        assert result is None

    def test_returns_none_for_empty_string(self) -> None:
        """Should return None for empty string."""
        result = parse_iso8601("")
        assert result is None

    def test_preserves_microseconds(self) -> None:
        """Should preserve microseconds from timestamp."""
        result = parse_iso8601("2024-01-15T10:30:00.123456Z")

        assert result is not None
        assert result.microsecond == 123456

    def test_handles_no_microseconds(self) -> None:
        """Should handle timestamps without microseconds."""
        result = parse_iso8601("2024-01-15T10:30:00Z")

        assert result is not None
        assert result.microsecond == 0
