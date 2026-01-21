"""Discord Attachment ORM model.

This module defines the Attachment entity for a Discord guild message archival system.
Attachments represent METADATA about files attached to messages, NOT the files themselves.

CRITICAL SEMANTIC: This table stores attachment METADATA snapshots.
- Row existence = attachment was present when message was archived
- Row existence ≠ file is still downloadable
- Row existence ≠ URLs are still valid
- This is NOT file storage or a file mirror

This table provides NO:
- Actual file content storage
- Guarantee of URL permanence or accessibility
- Download access tracking or view counts
- File lifecycle tracking (deletion, expiration)
- Historical versions or replacement tracking

NON-GOALS (explicit):
- This is NOT durable file storage
- This is NOT a CDN or file mirror
- This is NOT a download log
- This does NOT guarantee files can be retrieved
- URLs stored here may expire or require authentication

File availability warning:
- Discord CDN URLs have limited lifetime
- Ephemeral attachments may disappear quickly
- Deleted messages cause attachment URLs to become invalid
- For archival purposes, files must be downloaded separately

Design principles:
- attachment_id is the Discord-assigned unique identifier (primary key)
- Hard FK to Message with CASCADE (attachment dies with message)
- Append-only relative to messages (like Message model)
- LATEST-STATE SNAPSHOT consistent with Message, Emoji, Sticker models
"""

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    Boolean,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from discord_archive.db.base import Base, TZDateTime, utcnow

if TYPE_CHECKING:
    from discord_archive.db.models.message import Message


class Attachment(Base):
    """
    Discord Message Attachment entity.

    METADATA SNAPSHOT: Stores information ABOUT attachments, not file content.
    Row existence indicates attachment was present at ingestion time.
    Row existence does NOT guarantee file is still accessible.

    This is append-only like Message:
    - Attachments are created when messages are archived
    - Attachments are not updated after initial ingestion
    - Attachments are deleted only when parent message is deleted (CASCADE)

    NOT file storage:
    - URLs may expire or become invalid
    - No file content is stored in this table
    - Ephemeral attachments may disappear quickly
    - For true archival, download files separately

    Lifecycle:
    - Message archived → attachment rows created
    - Message deleted → attachment rows CASCADE deleted
    - Attachments do NOT exist independently of messages
    """

    __tablename__ = "attachments"

    # -------------------------------------------------------------------------
    # Primary Key
    # -------------------------------------------------------------------------

    # Discord-assigned unique identifier for the attachment.
    # Immutable once assigned. Globally unique across all messages.
    attachment_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    # -------------------------------------------------------------------------
    # Message Reference
    # -------------------------------------------------------------------------

    # Parent message containing this attachment.
    # HARD FK with CASCADE:
    #   - Attachments cannot exist without a message
    #   - If message is deleted, all its attachments are deleted
    #   - This enforces the "attachment belongs to message" invariant
    message_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("messages.message_id", ondelete="CASCADE"),
        nullable=False,
    )

    # -------------------------------------------------------------------------
    # File Metadata
    # -------------------------------------------------------------------------

    # Original filename as uploaded by the user.
    # May be sanitized by Discord. No length limit (TEXT type).
    filename: Mapped[str] = mapped_column(Text, nullable=False)

    # User-provided description (alt text) for the attachment.
    # NULL if no description was provided.
    # Used for accessibility and as caption in some clients.
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # MIME content type of the file (e.g., "image/png", "video/mp4").
    # NULL if Discord could not determine the type.
    # Snapshot from Discord API; may not match actual file content.
    content_type: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # File size in bytes as reported by Discord.
    # Snapshot value; does not update if file is modified externally.
    size: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # -------------------------------------------------------------------------
    # URLs (Best-Effort References)
    # -------------------------------------------------------------------------
    # WARNING: These URLs are NOT permanent and NOT guaranteed to work.
    # Discord CDN URLs may:
    #   - Expire after a period of time
    #   - Require authentication or specific headers
    #   - Become invalid if the message is deleted
    #   - Be rate-limited or blocked
    # Treat these as best-effort references, not durable storage links.

    # Primary CDN URL for the attachment.
    # May include authentication tokens that expire.
    url: Mapped[str] = mapped_column(Text, nullable=False)

    # Proxy CDN URL (alternative endpoint).
    # NULL if not provided by Discord.
    # May have different expiration or access characteristics than url.
    proxy_url: Mapped[str | None] = mapped_column(Text, nullable=True)

    # -------------------------------------------------------------------------
    # Media Dimensions (Optional, Media-Type Dependent)
    # -------------------------------------------------------------------------
    # These fields are populated only for images and videos.
    # NULL for non-visual file types (documents, audio, etc.).
    # Values are snapshots from Discord API and may be missing or incomplete.

    # Height in pixels for images/videos. NULL if not applicable or unknown.
    height: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Width in pixels for images/videos. NULL if not applicable or unknown.
    width: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # -------------------------------------------------------------------------
    # Audio/Video Metadata (Optional)
    # -------------------------------------------------------------------------

    # Duration in seconds for audio/video attachments.
    # NULL for non-temporal media (images, documents).
    # NOTE: Not guaranteed to be precise; may be approximate.
    duration_secs: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Base64-encoded waveform data for voice message attachments.
    # Used by Discord clients to render audio waveform visualization.
    # NULL for non-audio attachments.
    # Opaque data; format is Discord-internal and may change.
    waveform: Mapped[str | None] = mapped_column(Text, nullable=True)

    # -------------------------------------------------------------------------
    # Ephemeral Attachment Flag
    # -------------------------------------------------------------------------

    # Whether this is an ephemeral attachment.
    # Ephemeral attachments:
    #   - May only be accessible for a limited time
    #   - May disappear shortly after message creation
    #   - Are typically used for slash command responses
    #   - Presence in this table does NOT imply long-term accessibility
    # NULL if ephemeral status is unknown or not applicable.
    ephemeral: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    # -------------------------------------------------------------------------
    # Flags (Bitfield)
    # -------------------------------------------------------------------------

    # Attachment flags bitfield.
    # Defined by Discord; may expand in future API versions.
    # Known flags (as of this writing):
    #   1 << 0 = IS_REMIX: Attachment has been remixed
    #   1 << 2 = IS_THUMBNAIL: Attachment is a thumbnail
    # NULL if flags are not provided or not applicable.
    flags: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # -------------------------------------------------------------------------
    # Application Metadata
    # -------------------------------------------------------------------------

    # Title for application-generated attachments.
    # Primarily used for:
    #   - Embedded content from applications
    #   - Activity attachments
    #   - Rich media from bots
    # NULL for regular user-uploaded attachments.
    title: Mapped[str | None] = mapped_column(Text, nullable=True)

    # -------------------------------------------------------------------------
    # Forward Compatibility & Archival Metadata
    # -------------------------------------------------------------------------

    # Complete raw API response for forward compatibility.
    # Contains the full attachment object as returned by Discord API.
    raw: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # When our system ingested this attachment.
    # NOT when the file was uploaded to Discord.
    # NOT when the file was created.
    # This is purely an INGESTION timestamp.
    archived_at: Mapped[datetime] = mapped_column(
        TZDateTime, nullable=False, default=utcnow
    )

    # -------------------------------------------------------------------------
    # Relationships
    # -------------------------------------------------------------------------

    # Parent message. CASCADE delete ensures attachments die with their message.
    message: Mapped["Message"] = relationship("Message", back_populates="attachments")

    # -------------------------------------------------------------------------
    # Indexes
    # -------------------------------------------------------------------------

    __table_args__ = (
        # Find all attachments for a specific message
        Index("ix_attachments_message_id", "message_id"),
        # Filter attachments by content type (e.g., find all images)
        Index("ix_attachments_content_type", "content_type"),
    )

    def __repr__(self) -> str:
        return f"<Attachment(attachment_id={self.attachment_id}, filename='{self.filename}')>"

