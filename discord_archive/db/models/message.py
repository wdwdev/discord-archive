"""Discord Message ORM model.

This module defines the Message entity for a Discord guild message archival system.
Messages are APPEND-ONLY: once ingested, they are never deleted or overwritten
(except for edit tracking via edited_timestamp).

Design principles:
- JSONB is used for evolving/nested Discord structures (embeds, components, polls)
- Soft references (no FK) are used where ingestion order is not guaranteed
- Denormalization is used where query performance justifies it
- The `raw` column preserves the complete API payload for forward compatibility
"""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Boolean, ForeignKey, Index, Integer, Text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from discord_archive.db.base import Base, TZDateTime, utcnow

if TYPE_CHECKING:
    from discord_archive.db.models.attachment import Attachment
    from discord_archive.db.models.channel import Channel
    from discord_archive.db.models.reaction import Reaction
    from discord_archive.db.models.user import User


class Message(Base):
    """
    Discord Message entity.

    APPEND-ONLY: Each message is uniquely identified by message_id (Discord snowflake).
    Messages are never deleted from this table; edits are tracked via edited_timestamp.

    Author is referenced by user_id only. We do NOT store historical username or
    avatar snapshots; the User table contains only the latest-known state.
    """

    __tablename__ = "messages"

    # -------------------------------------------------------------------------
    # Primary Key
    # -------------------------------------------------------------------------

    # Discord snowflake ID. Globally unique, encodes creation timestamp.
    message_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    # -------------------------------------------------------------------------
    # Core References
    # -------------------------------------------------------------------------

    # Hard FK to channels table. If a channel is deleted from the archive,
    # all its messages are cascaded. This is safe because we ingest channels
    # before messages.
    channel_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("channels.channel_id", ondelete="CASCADE"),
        nullable=False,
    )

    # Soft reference (NO FK constraint). The author may be:
    #   - A regular user (exists in users table)
    #   - A webhook (not in users table)
    #   - A system message author (not in users table)
    #   - A deleted Discord account (may never be ingested)
    # The relationship below attempts a JOIN but does not enforce integrity.
    author_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # Denormalized guild_id for query efficiency. Common queries like
    # "all messages in guild X" avoid joining through channels table.
    # NULL for DM and group DM messages.
    guild_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # -------------------------------------------------------------------------
    # Message Content
    # -------------------------------------------------------------------------

    # The textual content of the message. May be empty string for embed-only
    # or attachment-only messages.
    content: Mapped[str] = mapped_column(Text, nullable=False, default="")

    # -------------------------------------------------------------------------
    # Timestamps
    # -------------------------------------------------------------------------

    # Discord's authoritative message creation time. This is the `timestamp`
    # field from the Discord API (ISO8601 string), NOT derived from the
    # snowflake. While snowflakes encode creation time, we store the explicit
    # API timestamp for accuracy and to avoid extraction logic.
    created_at: Mapped[datetime] = mapped_column(TZDateTime, nullable=False)

    # Last edit timestamp. NULL if message was never edited.
    edited_timestamp: Mapped[datetime | None] = mapped_column(TZDateTime, nullable=True)

    # -------------------------------------------------------------------------
    # Message Metadata
    # -------------------------------------------------------------------------

    # Message type determines semantic meaning:
    #   0 = DEFAULT, 1 = RECIPIENT_ADD, 6 = CHANNEL_PINNED_MESSAGE,
    #   7 = USER_JOIN, 19 = REPLY, 20 = CHAT_INPUT_COMMAND, etc.
    # See Discord API docs for full enum.
    type: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Text-to-speech flag. Preserved for completeness; the actual audio is
    # not archived, only the intent.
    tts: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Message flags bitfield (CROSSPOSTED, IS_CROSSPOST, SUPPRESS_EMBEDS,
    # URGENT, HAS_THREAD, EPHEMERAL, LOADING, etc.)
    # Default to 0 (no flags) rather than NULL for consistent bitwise operations.
    flags: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Whether the message is pinned in its channel.
    pinned: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # -------------------------------------------------------------------------
    # Mentions (extracted for query convenience)
    # -------------------------------------------------------------------------

    # Whether @everyone or @here was used.
    mention_everyone: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )

    # Array of user IDs mentioned. Extracted from API for indexed queries like
    # "messages mentioning user X".
    mentions: Mapped[list[int]] = mapped_column(
        ARRAY(BigInteger), nullable=False, default=list
    )

    # Array of role IDs mentioned.
    mention_roles: Mapped[list[int]] = mapped_column(
        ARRAY(BigInteger), nullable=False, default=list
    )

    # Channel mentions. Stored as JSONB because the structure includes
    # guild_id, channel type, and name—not just IDs.
    mention_channels: Mapped[list[dict] | None] = mapped_column(JSONB, nullable=True)

    # -------------------------------------------------------------------------
    # Webhook & Application
    # -------------------------------------------------------------------------

    # If sent via webhook, the webhook's snowflake ID.
    webhook_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # Application that sent this message (Rich Presence invites, slash commands).
    # JSONB because the application object structure is complex and rarely queried.
    application: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # Application ID extracted for potential indexing. Present on interaction
    # responses and Rich Presence messages.
    application_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # -------------------------------------------------------------------------
    # Reply / Forward / Crosspost Reference
    # -------------------------------------------------------------------------

    # Full message_reference object from Discord API. Contains:
    #   - message_id: referenced message
    #   - channel_id: may differ for crossposted messages
    #   - guild_id: may differ for forwarded content
    #   - type: 0=DEFAULT (reply), 1=FORWARD
    # Stored as JSONB to preserve all fields including future additions.
    message_reference: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # Extracted referenced message ID for B-tree indexing. Enables efficient
    # queries like "find all replies to message X" without JSONB path extraction.
    # Redundant with message_reference->>'message_id' but indexable.
    referenced_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # Snapshots of forwarded messages. When a message is forwarded, Discord
    # includes partial copies of the original messages. Stored as JSONB array.
    message_snapshots: Mapped[list[dict] | None] = mapped_column(JSONB, nullable=True)

    # -------------------------------------------------------------------------
    # Interactions (Slash Commands, Buttons, Context Menus)
    # -------------------------------------------------------------------------

    # Metadata about the interaction that triggered this message. Contains:
    #   - id, type, user, authorizing_integration_owners, etc.
    # JSONB because interaction schemas evolve rapidly with new Discord features.
    interaction_metadata: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # -------------------------------------------------------------------------
    # Thread Created From This Message
    # -------------------------------------------------------------------------

    # If this message started a thread, this contains the thread's state AT
    # CREATION TIME (not current state). The Channel table stores current state.
    # This is a snapshot for historical context: "what did the thread look like
    # when it was created from this message?"
    thread: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # -------------------------------------------------------------------------
    # Rich Content (JSONB for schema stability)
    # -------------------------------------------------------------------------

    # Embeds are complex nested structures (title, description, fields[],
    # author, footer, image, video, etc.). Discord frequently adds embed
    # features. JSONB preserves exact structure for rendering.
    embeds: Mapped[list[dict]] = mapped_column(JSONB, nullable=False, default=list)

    # Interactive components (buttons, select menus, text inputs, action rows).
    # Component schemas change with every new Discord feature. JSONB required.
    components: Mapped[list[dict] | None] = mapped_column(JSONB, nullable=True)

    # Sticker items. Array of partial sticker objects with id, name, format_type.
    sticker_items: Mapped[list[dict] | None] = mapped_column(JSONB, nullable=True)

    # Poll object. Includes question, answers[], expiry, layout. Polls are
    # relatively new and still evolving. JSONB preserves full structure.
    poll: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # -------------------------------------------------------------------------
    # Specialized Message Types (JSONB)
    # -------------------------------------------------------------------------

    # Rich Presence game activity (party invites, spectate buttons).
    # Rare but preserved for completeness.
    activity: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # Call metadata for DM voice/video calls. Contains participants list and
    # ended_timestamp. Preserved for DM archival use cases.
    call: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # Role subscription purchase/renewal messages. Contains tier info.
    role_subscription_data: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # -------------------------------------------------------------------------
    # Forward Compatibility
    # -------------------------------------------------------------------------

    # Complete raw API response. If Discord adds new fields, they are preserved
    # here even if not explicitly modeled. Enables future backfill without
    # re-ingestion.
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # -------------------------------------------------------------------------
    # Archival Metadata
    # -------------------------------------------------------------------------

    # Timestamp when OUR SYSTEM ingested this message. Distinct from created_at
    # (Discord's timestamp). Use cases:
    #   - Audit trail: "when did we capture this?"
    #   - Backfill debugging: messages ingested out-of-order
    #   - Ingestion monitoring: "messages archived in last 24h"
    archived_at: Mapped[datetime] = mapped_column(
        TZDateTime, nullable=False, default=utcnow
    )

    # -------------------------------------------------------------------------
    # Relationships
    # -------------------------------------------------------------------------

    channel: Mapped["Channel"] = relationship("Channel", back_populates="messages")

    # Soft relationship to User. Uses foreign() annotation to mark author_id
    # as the foreign side of the join WITHOUT creating a database FK constraint.
    # This allows SQLAlchemy to resolve the join correctly while permitting
    # author_id values that don't exist in users table (webhooks, system, etc.)
    # The relationship is viewonly=True since we don't manage User lifecycle here.
    author: Mapped["User | None"] = relationship(
        "User",
        back_populates="messages",
        primaryjoin="foreign(Message.author_id) == User.user_id",
        lazy="joined",
        viewonly=True,
    )

    attachments: Mapped[list["Attachment"]] = relationship(
        "Attachment",
        back_populates="message",
        lazy="selectin",
        cascade="all, delete-orphan",
    )

    reactions: Mapped[list["Reaction"]] = relationship(
        "Reaction",
        back_populates="message",
        lazy="selectin",
        cascade="all, delete-orphan",
    )

    # -------------------------------------------------------------------------
    # Indexes
    # -------------------------------------------------------------------------

    __table_args__ = (
        # Single-column indexes for common filters
        Index("ix_messages_channel_id", "channel_id"),
        Index("ix_messages_author_id", "author_id"),
        Index("ix_messages_guild_id", "guild_id"),
        Index("ix_messages_created_at", "created_at"),
        Index("ix_messages_type", "type"),
        Index("ix_messages_referenced_message_id", "referenced_message_id"),
        # Composite index for paginated channel queries (most common access pattern)
        Index("ix_messages_channel_created", "channel_id", "created_at"),
        # Composite index for guild-wide time-range queries
        Index("ix_messages_guild_created", "guild_id", "created_at"),
        # GIN indexes for array containment queries on mentions.
        # Enables efficient "find messages mentioning user/role X" via:
        #   WHERE mentions @> ARRAY[user_id] or mention_roles @> ARRAY[role_id]
        Index("ix_messages_mentions_gin", "mentions", postgresql_using="gin"),
        Index("ix_messages_mention_roles_gin", "mention_roles", postgresql_using="gin"),
    )

    def __repr__(self) -> str:
        content_preview = (
            self.content[:50] + "..." if len(self.content) > 50 else self.content
        )
        return f"<Message(message_id={self.message_id}, content='{content_preview}')>"
