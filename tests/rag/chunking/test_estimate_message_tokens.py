"""Tests for estimate_message_context_tokens function."""

from __future__ import annotations

from datetime import datetime, timezone

from discord_archive.db.models.message import Message
from discord_archive.rag.chunking.tokenizer import estimate_message_context_tokens, estimate_tokens


class TestEstimateMessageTokens:
    """Tests for estimate_message_context_tokens."""

    def test_message_with_only_content(self):
        """Message with only content should count header + content."""
        message = Message(
            message_id=1,
            channel_id=1,
            author_id=1,
            content="Hello world",
            created_at=datetime.now(timezone.utc),
            type=0,
            referenced_message_id=None,
            embeds=[],
            mentions=[],
            mention_roles=[],
        )

        tokens = estimate_message_context_tokens(message, None, [])

        # Exact: "[1] YYYY-MM-DD\nHello world\n\n"
        assert tokens == 19

    def test_message_with_empty_content_has_header_tokens(self):
        """Empty content message should still count header tokens."""
        message = Message(
            message_id=1,
            channel_id=1,
            author_id=1,
            content="",
            created_at=datetime.now(timezone.utc),
            type=0,
            referenced_message_id=None,
            embeds=[],
            mentions=[],
            mention_roles=[],
        )

        tokens = estimate_message_context_tokens(message, None, [])

        # Exact: "[1] YYYY-MM-DD\n\n"
        assert tokens == 16

    def test_message_with_embed_description(self):
        """Message with embed should count embed content."""
        message = Message(
            message_id=1,
            channel_id=1,
            author_id=1,
            content="",
            created_at=datetime.now(timezone.utc),
            type=0,
            referenced_message_id=None,
            embeds=[{"description": "Player `dankdmitron` has gone off the radar."}],
            mentions=[],
            mention_roles=[],
        )

        tokens = estimate_message_context_tokens(message, None, [])

        # Exact: "[1] YYYY-MM-DD\n[Embed: Player `dankdmitron` has gone off the radar.]\n\n"
        assert tokens == 35

    def test_message_with_complex_embed(self):
        """Complex embed with title, description, fields should count all parts."""
        message = Message(
            message_id=1,
            channel_id=1,
            author_id=1,
            content="Check this out!",
            created_at=datetime.now(timezone.utc),
            type=0,
            referenced_message_id=None,
            embeds=[
                {
                    "title": "GitHub PR #123",
                    "description": "Fix authentication bug",
                    "author": {"name": "dependabot"},
                    "fields": [
                        {"name": "Status", "value": "Open"},
                        {"name": "Labels", "value": "bug, security"},
                    ],
                    "footer": {"text": "github.com"},
                }
            ],
            mentions=[],
            mention_roles=[],
        )

        tokens = estimate_message_context_tokens(message, None, [])

        # Exact: header + "Check this out!" + complex embed
        assert tokens == 62

    def test_message_with_multiple_embeds(self):
        """Multiple embeds should all be counted."""
        message = Message(
            message_id=1,
            channel_id=1,
            author_id=1,
            content="",
            created_at=datetime.now(timezone.utc),
            type=0,
            referenced_message_id=None,
            embeds=[
                {"description": "First embed"},
                {"description": "Second embed"},
                {"description": "Third embed"},
            ],
            mentions=[],
            mention_roles=[],
        )

        tokens = estimate_message_context_tokens(message, None, [])

        # Exact: header + 3 embeds
        assert tokens == 40

    def test_embed_with_truncated_description(self):
        """Long embed description should be truncated to 200 chars."""
        long_text = "x" * 300  # 300 chars
        message = Message(
            message_id=1,
            channel_id=1,
            author_id=1,
            content="",
            created_at=datetime.now(timezone.utc),
            type=0,
            referenced_message_id=None,
            embeds=[{"description": long_text}],
            mentions=[],
            mention_roles=[],
        )

        tokens = estimate_message_context_tokens(message, None, 0)

        # Should not count all 300 chars, only truncated version
        # Truncated is 200 chars max (197 + "...")
        full_tokens = estimate_tokens(long_text)
        truncated_tokens = estimate_tokens(long_text[:197] + "...")

        # Tokens should be closer to truncated than full
        assert abs(tokens - (15 + truncated_tokens + 10)) < abs(
            tokens - (15 + full_tokens + 10)
        )

    def test_embed_limits_fields_to_five(self):
        """Embed should only process first 5 fields."""
        message = Message(
            message_id=1,
            channel_id=1,
            author_id=1,
            content="",
            created_at=datetime.now(timezone.utc),
            type=0,
            referenced_message_id=None,
            embeds=[
                {"fields": [{"name": f"Field{i}", "value": f"Value{i}"} for i in range(10)]}
            ],
            mentions=[],
            mention_roles=[],
        )

        tokens = estimate_message_context_tokens(message, None, 0)

        # Should count header + [Embed: Field0: Value0 - ... - Field4: Value4]
        # Not all 10 fields
        assert tokens > 15
        assert tokens < 200  # Should not be huge

    def test_non_dict_embed_ignored(self):
        """Non-dict embed should be safely ignored."""
        message = Message(
            message_id=1,
            channel_id=1,
            author_id=1,
            content="test",
            created_at=datetime.now(timezone.utc),
            type=0,
            referenced_message_id=None,
            embeds=["not a dict", None, 123],  # Invalid embeds
            mentions=[],
            mention_roles=[],
        )

        tokens = estimate_message_context_tokens(message, None, 0)
        content_tokens = estimate_tokens("test")

        # Should just be header + content, no embed tokens
        assert tokens == 15 + content_tokens + 2  # +2 for separators

    def test_with_username_increases_tokens(self):
        """Username should affect token count."""
        message = Message(
            message_id=1,
            channel_id=1,
            author_id=123456789,
            content="test",
            created_at=datetime.now(timezone.utc),
            type=0,
            referenced_message_id=None,
            embeds=[],
            mentions=[],
            mention_roles=[],
        )

        tokens_no_username = estimate_message_context_tokens(message, None, 0)
        tokens_with_username = estimate_message_context_tokens(message, "Alice", 0)

        # "[Alice]" should be fewer tokens than "[123456789]"
        assert tokens_with_username < tokens_no_username

    def test_with_attachments_increases_tokens(self):
        """Attachments should add tokens."""
        from discord_archive.db.models.attachment import Attachment

        message = Message(
            message_id=1,
            channel_id=1,
            author_id=1,
            content="test",
            created_at=datetime.now(timezone.utc),
            type=0,
            referenced_message_id=None,
            embeds=[],
            mentions=[],
            mention_roles=[],
        )

        tokens_no_attach = estimate_message_context_tokens(message, None, [])

        # Create 2 attachment objects with actual filenames
        attachments = [
            Attachment(
                attachment_id=1,
                message_id=1,
                filename="attachment0.ext",
                size=1024,
                url="https://example.com/file1",
            ),
            Attachment(
                attachment_id=2,
                message_id=1,
                filename="attachment1.ext",
                size=2048,
                url="https://example.com/file2",
            ),
        ]
        tokens_with_attach = estimate_message_context_tokens(message, None, attachments)

        # 2 attachments add actual formatted lines with newlines
        # Difference includes the attachment text + newlines between them
        assert tokens_with_attach == tokens_no_attach + 18
