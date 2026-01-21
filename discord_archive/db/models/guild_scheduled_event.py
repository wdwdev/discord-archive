"""Discord Guild Scheduled Event ORM model.

This module defines the GuildScheduledEvent entity for a Discord guild message
archival system. Scheduled events are LATEST-STATE SNAPSHOTS: each re-ingestion
overwrites the previous state.

IMPORTANT: This is NOT a historical event timeline.
- No tracking of status transitions (SCHEDULED → ACTIVE → COMPLETED → CANCELED)
- No tracking of user_count changes over time
- No expansion of recurrence rules into multiple rows
- Each row represents the event's state at last ingestion

Design principles:
- event_id (snowflake) is the authoritative identity
- Soft references for creator_id, channel_id, entity_id (may not exist in archive)
- JSONB for entity_metadata and recurrence_rule (evolving structures)
- recurrence_rule is a raw snapshot, NOT expanded into child events
"""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, ForeignKey, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from discord_archive.db.base import Base, TZDateTime, utcnow

if TYPE_CHECKING:
    from discord_archive.db.models.guild import Guild


class GuildScheduledEvent(Base):
    """
    Discord Guild Scheduled Event entity.

    LATEST-STATE SNAPSHOT: Stores the current state of a scheduled event.
    Overwrites on each re-ingestion; no historical tracking.

    NOT a historical timeline:
    - Status may change (SCHEDULED → ACTIVE → COMPLETED) between ingestions
    - We only store the LAST OBSERVED status, not the transition history
    - user_count reflects last-ingested subscriber count, not historical attendance
    - Completed/canceled events may still exist in this table until pruned

    Event types:
    - STAGE_INSTANCE (entity_type=1): Event in a stage channel
    - VOICE (entity_type=2): Event in a voice channel
    - EXTERNAL (entity_type=3): Event at an external location (channel_id=NULL)
    """

    __tablename__ = "guild_scheduled_events"

    # -------------------------------------------------------------------------
    # Primary Key
    # -------------------------------------------------------------------------

    # Discord snowflake ID. Unique identifier for this scheduled event.
    event_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    # -------------------------------------------------------------------------
    # Guild Reference (Hard FK)
    # -------------------------------------------------------------------------

    # Scheduled events always belong to a guild. Hard FK with CASCADE:
    # if guild is deleted from archive, its events go too.
    guild_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("guilds.guild_id", ondelete="CASCADE"), nullable=False
    )

    # -------------------------------------------------------------------------
    # Channel Reference (SOFT - No FK)
    # -------------------------------------------------------------------------

    # Voice or stage channel where the event takes place.
    # SOFT REFERENCE (no FK) because:
    #   - EXTERNAL events (entity_type=3) have no channel; this is NULL
    #   - Channel may be deleted after event creation
    #   - Channel may not be ingested (permissions, ordering)
    channel_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # -------------------------------------------------------------------------
    # Creator Reference (SOFT - No FK)
    # -------------------------------------------------------------------------

    # User who created this event. SOFT REFERENCE (no FK) because:
    #   - Creator may not be in users table (not ingested, left guild)
    #   - Creator may be a bot or integration
    #   - Creator account may be deleted
    # Informational metadata only.
    creator_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # -------------------------------------------------------------------------
    # Event Properties
    # -------------------------------------------------------------------------

    # Event name/title. Required by Discord API.
    name: Mapped[str] = mapped_column(String(400), nullable=False)

    # Event description. Optional.
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Cover image hash. NULL if no cover image set.
    image: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # -------------------------------------------------------------------------
    # Timing
    # -------------------------------------------------------------------------

    # When the event is scheduled to start. Required.
    scheduled_start_time: Mapped[datetime] = mapped_column(TZDateTime, nullable=False)

    # When the event is scheduled to end. Optional (some events have no end time).
    scheduled_end_time: Mapped[datetime | None] = mapped_column(
        TZDateTime, nullable=True
    )

    # -------------------------------------------------------------------------
    # Status & Privacy (Integer Enums)
    # -------------------------------------------------------------------------

    # Event privacy level:
    #   2 = GUILD_ONLY (only guild members can see/join)
    # Note: Currently Discord only supports GUILD_ONLY (2). Value 1 was removed.
    privacy_level: Mapped[int] = mapped_column(Integer, nullable=False)

    # Event status:
    #   1 = SCHEDULED (upcoming, not yet started)
    #   2 = ACTIVE (currently in progress)
    #   3 = COMPLETED (finished successfully)
    #   4 = CANCELED (canceled before completion)
    # NOTE: This is the LAST OBSERVED status. We do NOT track status transitions.
    status: Mapped[int] = mapped_column(Integer, nullable=False)

    # -------------------------------------------------------------------------
    # Entity Type & Reference
    # -------------------------------------------------------------------------

    # Event entity type:
    #   1 = STAGE_INSTANCE (event takes place in a stage channel)
    #   2 = VOICE (event takes place in a voice channel)
    #   3 = EXTERNAL (event at external location, uses entity_metadata.location)
    entity_type: Mapped[int] = mapped_column(Integer, nullable=False)

    # Associated entity (e.g., stage instance ID). SOFT REFERENCE because:
    #   - Stage instances are ephemeral and may not exist in archive
    #   - Entity may be deleted or not yet created
    # NULL for EXTERNAL events or if no associated entity.
    entity_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # Entity metadata. JSONB for flexibility. For EXTERNAL events, contains:
    #   - location: string describing where the event takes place
    # May contain additional fields as Discord evolves the API.
    entity_metadata: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # -------------------------------------------------------------------------
    # Subscriber Count
    # -------------------------------------------------------------------------

    # Number of users subscribed to this event at last ingestion.
    # NOTE: This is a snapshot count, NOT historical attendance tracking.
    # May be NULL if not provided by API (depends on query parameters).
    user_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # -------------------------------------------------------------------------
    # Recurrence Rule (JSONB Snapshot)
    # -------------------------------------------------------------------------

    # Recurrence configuration for repeating events. JSONB because:
    #   - Complex nested structure (start, end, frequency, interval, by_weekday, etc.)
    #   - Evolving Discord feature; structure may change
    # IMPORTANT: This is a RAW SNAPSHOT of the recurrence rule.
    #   - We do NOT expand recurrence into multiple scheduled event rows
    #   - Each recurring event has ONE row with the recurrence configuration
    #   - Future occurrences are computed at query time if needed
    recurrence_rule: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # -------------------------------------------------------------------------
    # Forward Compatibility & Archival Metadata
    # -------------------------------------------------------------------------

    # Complete raw API response for forward compatibility.
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # When our system ingested this event snapshot.
    archived_at: Mapped[datetime] = mapped_column(
        TZDateTime, nullable=False, default=utcnow
    )

    # When this row was last updated (re-ingestion timestamp).
    updated_at: Mapped[datetime] = mapped_column(
        TZDateTime, nullable=False, default=utcnow, onupdate=utcnow
    )

    # -------------------------------------------------------------------------
    # Relationships
    # -------------------------------------------------------------------------

    guild: Mapped["Guild"] = relationship("Guild", back_populates="scheduled_events")

    # -------------------------------------------------------------------------
    # Indexes
    # -------------------------------------------------------------------------

    __table_args__ = (
        # Find all events for a guild
        Index("ix_guild_scheduled_events_guild_id", "guild_id"),
        # Filter events by status (active, scheduled, completed)
        Index("ix_guild_scheduled_events_status", "status"),
        # Find upcoming events by start time
        Index("ix_guild_scheduled_events_start_time", "scheduled_start_time"),
    )

    def __repr__(self) -> str:
        return f"<GuildScheduledEvent(event_id={self.event_id}, name='{self.name}')>"
