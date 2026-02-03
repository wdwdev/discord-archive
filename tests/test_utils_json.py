"""Tests for discord_archive.utils.json."""

from __future__ import annotations

from discord_archive.utils.json import compact_json


class TestCompactJson:
    """Tests for compact_json function."""

    def test_removes_none_values(self) -> None:
        data = {"a": 1, "b": None, "c": "hello"}

        result = compact_json(data)

        assert result == {"a": 1, "c": "hello"}

    def test_preserves_all_non_none(self) -> None:
        data = {"a": 1, "b": 2}

        result = compact_json(data)

        assert result == {"a": 1, "b": 2}

    def test_returns_empty_dict_when_all_none(self) -> None:
        data = {"a": None, "b": None}

        result = compact_json(data)

        assert result == {}

    def test_preserves_falsy_non_none_values(self) -> None:
        data = {"a": 0, "b": "", "c": False, "d": [], "e": None}

        result = compact_json(data)

        assert result == {"a": 0, "b": "", "c": False, "d": []}

    def test_returns_non_dict_unchanged(self) -> None:
        assert compact_json("hello") == "hello"
        assert compact_json(42) == 42
        assert compact_json([1, 2, 3]) == [1, 2, 3]
        assert compact_json(None) is None

    def test_empty_dict_returns_empty(self) -> None:
        assert compact_json({}) == {}

    def test_shallow_only(self) -> None:
        """Should not recurse into nested dicts."""
        data = {"a": {"nested": None}, "b": None}

        result = compact_json(data)

        assert result == {"a": {"nested": None}}
