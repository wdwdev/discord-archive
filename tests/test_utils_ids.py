"""Tests for discord_archive.utils.ids."""

from __future__ import annotations

from discord_archive.utils.ids import parse_optional_int, parse_snowflake


class TestParseSnowflake:
    """Tests for parse_snowflake function."""

    def test_returns_none_for_none(self) -> None:
        assert parse_snowflake(None) is None

    def test_parses_string(self) -> None:
        assert parse_snowflake("123456789") == 123456789

    def test_parses_int(self) -> None:
        assert parse_snowflake(123456789) == 123456789

    def test_parses_large_snowflake(self) -> None:
        assert parse_snowflake("1234567890123456789") == 1234567890123456789


class TestParseOptionalInt:
    """Tests for parse_optional_int function."""

    def test_returns_none_for_none(self) -> None:
        assert parse_optional_int(None) is None

    def test_parses_string(self) -> None:
        assert parse_optional_int("42") == 42

    def test_parses_int(self) -> None:
        assert parse_optional_int(42) == 42

    def test_parses_float(self) -> None:
        assert parse_optional_int(3.9) == 3
