"""Tests for discord_archive.ingest.mappers.scheduled_event."""

from __future__ import annotations

from datetime import datetime, timezone

from discord_archive.ingest.mappers.scheduled_event import map_scheduled_event


def _make_event_data(**overrides) -> dict:
    """Build a minimal scheduled event dict."""
    data = {
        "id": "900",
        "guild_id": "111",
        "name": "Game Night",
        "scheduled_start_time": "2024-06-01T20:00:00+00:00",
        "privacy_level": 2,
        "status": 1,
        "entity_type": 2,
    }
    data.update(overrides)
    return data


class TestMapScheduledEvent:
    """Tests for map_scheduled_event function."""

    def test_maps_required_fields(self) -> None:
        data = _make_event_data()

        result = map_scheduled_event(data)

        assert result.event_id == 900
        assert result.guild_id == 111
        assert result.name == "Game Night"
        assert result.privacy_level == 2
        assert result.status == 1
        assert result.entity_type == 2

    def test_parses_start_time(self) -> None:
        data = _make_event_data(scheduled_start_time="2024-06-01T20:00:00+00:00")

        result = map_scheduled_event(data)

        assert result.scheduled_start_time == datetime(
            2024, 6, 1, 20, 0, tzinfo=timezone.utc
        )

    def test_parses_end_time(self) -> None:
        data = _make_event_data(scheduled_end_time="2024-06-01T23:00:00+00:00")

        result = map_scheduled_event(data)

        assert result.scheduled_end_time == datetime(
            2024, 6, 1, 23, 0, tzinfo=timezone.utc
        )

    def test_end_time_none_when_absent(self) -> None:
        data = _make_event_data()

        result = map_scheduled_event(data)

        assert result.scheduled_end_time is None

    def test_maps_channel_id(self) -> None:
        data = _make_event_data(channel_id="555")

        result = map_scheduled_event(data)

        assert result.channel_id == 555

    def test_channel_id_none_when_absent(self) -> None:
        data = _make_event_data()

        result = map_scheduled_event(data)

        assert result.channel_id is None

    def test_maps_creator_id(self) -> None:
        data = _make_event_data(creator_id="777")

        result = map_scheduled_event(data)

        assert result.creator_id == 777

    def test_creator_id_none_when_absent(self) -> None:
        data = _make_event_data()

        result = map_scheduled_event(data)

        assert result.creator_id is None

    def test_maps_entity_id(self) -> None:
        data = _make_event_data(entity_id="888")

        result = map_scheduled_event(data)

        assert result.entity_id == 888

    def test_entity_id_none_when_absent(self) -> None:
        data = _make_event_data()

        result = map_scheduled_event(data)

        assert result.entity_id is None

    def test_maps_optional_fields(self) -> None:
        data = _make_event_data(
            description="A fun event",
            image="img_hash",
            entity_metadata={"location": "Voice Channel"},
            user_count=42,
            recurrence_rule={"frequency": 2},
        )

        result = map_scheduled_event(data)

        assert result.description == "A fun event"
        assert result.image == "img_hash"
        assert result.entity_metadata == {"location": "Voice Channel"}
        assert result.user_count == 42
        assert result.recurrence_rule == {"frequency": 2}

    def test_stores_raw_data(self) -> None:
        data = _make_event_data()

        result = map_scheduled_event(data)

        assert result.raw is data
