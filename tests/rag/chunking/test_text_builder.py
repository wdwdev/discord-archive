"""Tests for discord_archive.rag.chunking.text_builder."""

from __future__ import annotations

from datetime import datetime, timezone

from discord_archive.db.models.attachment import Attachment
from discord_archive.db.models.chunk import Chunk
from discord_archive.db.models.message import Message
from discord_archive.rag.chunking.text_builder import (
    EXTENSION_LABELS,
    MessageContext,
    TextBuilder,
    TextBuildingConfig,
)


def make_message(
    message_id: int,
    author_id: int = 1,
    content: str = "test",
    channel_id: int = 100,
    created_at: datetime | None = None,
    embeds: list[dict] | None = None,
) -> Message:
    """Create a test message."""
    return Message(
        message_id=message_id,
        channel_id=channel_id,
        author_id=author_id,
        content=content,
        created_at=created_at or datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        type=0,
        embeds=embeds or [],
    )


def make_attachment(
    attachment_id: int,
    message_id: int,
    filename: str,
) -> Attachment:
    """Create a test attachment."""
    return Attachment(
        attachment_id=attachment_id,
        message_id=message_id,
        filename=filename,
        size=1024,
        url="https://example.com/file",
    )


def make_context(
    message: Message,
    author_username: str | None = None,
    attachments: list[Attachment] | None = None,
) -> MessageContext:
    """Create a MessageContext for testing."""
    return MessageContext(
        message=message,
        author_username=author_username,
        attachments=attachments or [],
    )


def make_chunk(
    chunk_id: int,
    chunk_type: str,
    message_ids: list[int],
) -> Chunk:
    """Create a test chunk."""
    ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    return Chunk(
        chunk_id=chunk_id,
        chunk_type=chunk_type,
        guild_id=1,
        channel_id=100,
        message_ids=message_ids,
        author_ids=[1],
        chunk_state="closed",
        start_message_id=message_ids[0],
        leaf_message_id=message_ids[-1] if chunk_type == "reply_chain" else None,
        embedding_status="pending",
        first_message_at=ts,
        last_message_at=ts,
    )


class TestTextBuildingConfig:
    """Tests for TextBuildingConfig."""

    def test_default_values(self) -> None:
        config = TextBuildingConfig()
        assert config.max_filename_length == 100
        assert config.date_format == "%Y-%m-%d"

    def test_custom_values(self) -> None:
        config = TextBuildingConfig(max_filename_length=50, date_format="%d/%m/%Y")
        assert config.max_filename_length == 50
        assert config.date_format == "%d/%m/%Y"


class TestMessageContext:
    """Tests for MessageContext."""

    def test_message_context_creation(self) -> None:
        msg = make_message(1)
        ctx = MessageContext(
            message=msg,
            author_username="Alice",
            attachments=[],
        )
        assert ctx.message is msg
        assert ctx.author_username == "Alice"
        assert ctx.attachments == []


class TestTextBuilder:
    """Tests for TextBuilder."""

    def test_default_config(self) -> None:
        builder = TextBuilder()
        assert builder.config.max_filename_length == 100

    def test_custom_config(self) -> None:
        config = TextBuildingConfig(max_filename_length=50)
        builder = TextBuilder(config)
        assert builder.config.max_filename_length == 50


class TestTextBuilderFormatMessage:
    """Tests for message formatting."""

    def test_basic_message_with_username(self) -> None:
        builder = TextBuilder()
        msg = make_message(1, content="Hello everyone!")
        ctx = make_context(msg, author_username="Alice")

        text = builder._format_message(ctx)

        assert "[Alice] 2024-01-15" in text
        assert "Hello everyone!" in text

    def test_message_without_username_uses_author_id(self) -> None:
        builder = TextBuilder()
        msg = make_message(1, author_id=12345, content="Hello")
        ctx = make_context(msg, author_username=None)

        text = builder._format_message(ctx)

        assert "[12345] 2024-01-15" in text

    def test_message_with_attachment(self) -> None:
        builder = TextBuilder()
        msg = make_message(1, content="Check this out")
        att = make_attachment(1, 1, "screenshot.png")
        ctx = make_context(msg, author_username="Bob", attachments=[att])

        text = builder._format_message(ctx)

        assert "[Bob] 2024-01-15" in text
        assert "Check this out" in text
        assert "[Image: screenshot.png]" in text

    def test_message_with_embed(self) -> None:
        builder = TextBuilder()
        embed = {
            "title": "Bug Report",
            "description": "Critical issue found",
        }
        msg = make_message(1, content="", embeds=[embed])
        ctx = make_context(msg, author_username="Carol")

        text = builder._format_message(ctx)

        assert "[Carol] 2024-01-15" in text
        assert '[Embed: "Bug Report" - Critical issue found]' in text


class TestTextBuilderFormatAttachment:
    """Tests for attachment formatting."""

    def test_image_extension(self) -> None:
        builder = TextBuilder()
        att = make_attachment(1, 1, "photo.jpg")

        text = builder._format_attachment(att)

        assert text == "[Image: photo.jpg]"

    def test_video_extension(self) -> None:
        builder = TextBuilder()
        att = make_attachment(1, 1, "clip.mp4")

        text = builder._format_attachment(att)

        assert text == "[Video: clip.mp4]"

    def test_audio_extension(self) -> None:
        builder = TextBuilder()
        att = make_attachment(1, 1, "song.mp3")

        text = builder._format_attachment(att)

        assert text == "[Audio: song.mp3]"

    def test_code_extension(self) -> None:
        builder = TextBuilder()
        att = make_attachment(1, 1, "script.py")

        text = builder._format_attachment(att)

        assert text == "[Code: script.py]"

    def test_document_extension(self) -> None:
        builder = TextBuilder()
        att = make_attachment(1, 1, "report.pdf")

        text = builder._format_attachment(att)

        assert text == "[Document: report.pdf]"

    def test_archive_extension(self) -> None:
        builder = TextBuilder()
        att = make_attachment(1, 1, "backup.zip")

        text = builder._format_attachment(att)

        assert text == "[Archive: backup.zip]"

    def test_unknown_extension(self) -> None:
        builder = TextBuilder()
        att = make_attachment(1, 1, "data.xyz")

        text = builder._format_attachment(att)

        assert text == "[File: data.xyz]"

    def test_no_extension(self) -> None:
        builder = TextBuilder()
        att = make_attachment(1, 1, "README")

        text = builder._format_attachment(att)

        assert text == "[File: README]"

    def test_long_filename_truncation(self) -> None:
        builder = TextBuilder(TextBuildingConfig(max_filename_length=20))
        att = make_attachment(1, 1, "very_long_filename_that_exceeds_limit.png")

        text = builder._format_attachment(att)

        # Should be truncated but preserve extension
        assert "[Image:" in text
        assert ".png]" in text
        assert "..." in text

    def test_long_filename_truncation_no_extension(self) -> None:
        builder = TextBuilder(TextBuildingConfig(max_filename_length=20))
        att = make_attachment(1, 1, "very_long_filename_without_extension")

        text = builder._format_attachment(att)

        assert "[File:" in text
        assert "..." in text


class TestTextBuilderFormatEmbed:
    """Tests for embed formatting."""

    def test_embed_with_title_only(self) -> None:
        builder = TextBuilder()
        embed = {"title": "Announcement"}

        text = builder._format_embed(embed)

        assert text == '[Embed: "Announcement"]'

    def test_embed_with_title_and_description(self) -> None:
        builder = TextBuilder()
        embed = {
            "title": "Update",
            "description": "New features added",
        }

        text = builder._format_embed(embed)

        assert '[Embed: "Update" - New features added]' == text

    def test_embed_with_author(self) -> None:
        builder = TextBuilder()
        embed = {
            "title": "Post",
            "author": {"name": "John Doe"},
        }

        text = builder._format_embed(embed)

        assert '"Post"' in text
        assert "by John Doe" in text

    def test_embed_with_fields(self) -> None:
        builder = TextBuilder()
        embed = {
            "fields": [
                {"name": "Status", "value": "Active"},
                {"name": "Priority", "value": "High"},
            ]
        }

        text = builder._format_embed(embed)

        assert "Status: Active" in text
        assert "Priority: High" in text

    def test_embed_with_footer(self) -> None:
        builder = TextBuilder()
        embed = {
            "title": "Notice",
            "footer": {"text": "Last updated today"},
        }

        text = builder._format_embed(embed)

        assert "Last updated today" in text

    def test_embed_long_description_truncated(self) -> None:
        """Long description should be truncated using token limits."""
        from discord_archive.rag.chunking.tokenizer import estimate_tokens

        builder = TextBuilder()
        # Use varied text that will have more tokens
        long_desc = "This is a very long description with many different words. " * 20
        embed = {"description": long_desc}

        text = builder._format_embed(embed)

        # Original description has >60 tokens, should be truncated
        original_tokens = estimate_tokens(long_desc)
        result_tokens = estimate_tokens(text)

        # Result should be less than original (was truncated)
        assert result_tokens < original_tokens
        assert "..." in text

    def test_embed_limits_fields(self) -> None:
        builder = TextBuilder()
        embed = {
            "fields": [{"name": f"Field{i}", "value": f"Value{i}"} for i in range(10)]
        }

        text = builder._format_embed(embed)

        # Only first 5 fields should be included
        assert "Field0: Value0" in text
        assert "Field4: Value4" in text
        assert "Field5" not in text

    def test_empty_embed(self) -> None:
        builder = TextBuilder()
        embed = {}

        text = builder._format_embed(embed)

        assert text == ""


class TestTextBuilderClassifyExtension:
    """Tests for extension classification."""

    def test_all_image_extensions(self) -> None:
        builder = TextBuilder()
        for ext in ["png", "jpg", "jpeg", "gif", "webp", "bmp", "svg"]:
            assert builder._classify_extension(f"file.{ext}") == "Image"

    def test_all_video_extensions(self) -> None:
        builder = TextBuilder()
        for ext in ["mp4", "mov", "webm", "avi", "mkv"]:
            assert builder._classify_extension(f"file.{ext}") == "Video"

    def test_all_audio_extensions(self) -> None:
        builder = TextBuilder()
        for ext in ["mp3", "wav", "ogg", "flac", "m4a"]:
            assert builder._classify_extension(f"file.{ext}") == "Audio"

    def test_case_insensitive(self) -> None:
        builder = TextBuilder()
        assert builder._classify_extension("FILE.PNG") == "Image"
        assert builder._classify_extension("File.Mp4") == "Video"


class TestTextBuilderBuildChunkText:
    """Tests for building complete chunk texts."""

    def test_sliding_window_format(self) -> None:
        builder = TextBuilder()
        chunk = make_chunk(1, "sliding_window", [1, 2])

        msg1 = make_message(1, content="Hello everyone!")
        msg2 = make_message(2, content="Got it!")
        contexts = [
            make_context(msg1, author_username="Alice"),
            make_context(msg2, author_username="Bob"),
        ]

        text, token_count = builder.build_chunk_text(chunk, contexts)

        assert "[Alice] 2024-01-15" in text
        assert "Hello everyone!" in text
        assert "[Bob] 2024-01-15" in text
        assert "Got it!" in text
        # Sequential format: messages separated by blank line
        assert "\n\n" in text
        # No reply indicator
        assert "↳" not in text
        assert token_count > 0

    def test_author_group_format(self) -> None:
        builder = TextBuilder()
        chunk = make_chunk(1, "author_group", [1, 2])

        msg1 = make_message(1, content="First message")
        msg2 = make_message(2, content="Second message")
        contexts = [
            make_context(msg1, author_username="Alice"),
            make_context(msg2, author_username="Alice"),
        ]

        text, token_count = builder.build_chunk_text(chunk, contexts)

        # Same format as sliding_window
        assert "First message" in text
        assert "Second message" in text
        assert "↳" not in text

    def test_reply_chain_format(self) -> None:
        builder = TextBuilder()
        chunk = make_chunk(1, "reply_chain", [1, 2, 3])

        msg1 = make_message(1, content="What's the bug?")
        msg2 = make_message(2, content="See error")
        msg3 = make_message(3, content="Fixed in PR #123")
        att = make_attachment(1, 2, "error.png")
        contexts = [
            make_context(msg1, author_username="Alice"),
            make_context(msg2, author_username="Bob", attachments=[att]),
            make_context(msg3, author_username="Carol"),
        ]

        text, token_count = builder.build_chunk_text(chunk, contexts)

        # Reply chain format: root message has no indicator, replies have ↳
        lines = text.split("\n")
        # First message has no ↳
        assert not any(line.startswith("↳ [Alice]") for line in lines)
        # Reply messages have ↳
        assert any("↳ [Bob]" in line for line in lines)
        assert any("↳ [Carol]" in line for line in lines)
        # Content preserved
        assert "What's the bug?" in text
        assert "See error" in text
        assert "[Image: error.png]" in text
        assert "Fixed in PR #123" in text

    def test_token_count_matches_estimate(self) -> None:
        from discord_archive.rag.chunking.tokenizer import estimate_tokens

        builder = TextBuilder()
        chunk = make_chunk(1, "sliding_window", [1])

        msg = make_message(1, content="Hello world, this is a test message.")
        contexts = [make_context(msg, author_username="Alice")]

        text, token_count = builder.build_chunk_text(chunk, contexts)

        # Token count should match what estimate_tokens returns for the built text
        assert token_count == estimate_tokens(text)


class TestExtensionLabels:
    """Tests for the EXTENSION_LABELS constant."""

    def test_extension_labels_not_empty(self) -> None:
        assert len(EXTENSION_LABELS) > 0

    def test_common_extensions_covered(self) -> None:
        common = ["png", "jpg", "mp4", "pdf", "py", "js", "zip"]
        for ext in common:
            assert ext in EXTENSION_LABELS
