"""Text builder for chunk content.

Builds formatted text representations of chunks for embedding.
"""

from dataclasses import dataclass

from discord_archive.db.models.attachment import Attachment
from discord_archive.db.models.chunk import Chunk
from discord_archive.db.models.message import Message
from discord_archive.rag.chunking.tokenizer import estimate_tokens

# Extension to label mapping
EXTENSION_LABELS: dict[str, str] = {
    # Images
    "png": "Image",
    "jpg": "Image",
    "jpeg": "Image",
    "gif": "Image",
    "webp": "Image",
    "bmp": "Image",
    "svg": "Image",
    # Videos
    "mp4": "Video",
    "mov": "Video",
    "webm": "Video",
    "avi": "Video",
    "mkv": "Video",
    # Audio
    "mp3": "Audio",
    "wav": "Audio",
    "ogg": "Audio",
    "flac": "Audio",
    "m4a": "Audio",
    # Documents
    "pdf": "Document",
    "doc": "Document",
    "docx": "Document",
    "txt": "Document",
    "rtf": "Document",
    "odt": "Document",
    # Code
    "py": "Code",
    "js": "Code",
    "ts": "Code",
    "go": "Code",
    "rs": "Code",
    "java": "Code",
    "c": "Code",
    "cpp": "Code",
    "h": "Code",
    "hpp": "Code",
    "cs": "Code",
    "rb": "Code",
    "php": "Code",
    "swift": "Code",
    "kt": "Code",
    "scala": "Code",
    "sh": "Code",
    "bash": "Code",
    "zsh": "Code",
    "ps1": "Code",
    "sql": "Code",
    "html": "Code",
    "css": "Code",
    "scss": "Code",
    "less": "Code",
    "json": "Code",
    "yaml": "Code",
    "yml": "Code",
    "xml": "Code",
    "toml": "Code",
    "ini": "Code",
    "md": "Code",
    "rst": "Code",
    # Archives
    "zip": "Archive",
    "tar": "Archive",
    "gz": "Archive",
    "bz2": "Archive",
    "xz": "Archive",
    "7z": "Archive",
    "rar": "Archive",
}


@dataclass
class TextBuildingConfig:
    """Configuration for text building."""

    max_filename_length: int = 100
    date_format: str = "%Y-%m-%d"


@dataclass
class MessageContext:
    """Context for building a message's text representation.

    Contains the message along with related data needed for formatting.
    """

    message: Message
    author_username: str | None
    attachments: list[Attachment]


class TextBuilder:
    """Builds formatted text representations of chunks for embedding.

    Text formats vary by chunk type:
    - sliding_window/author_group: Sequential messages with author/date headers
    - reply_chain: Threaded format with reply indicators (↳)
    """

    def __init__(self, config: TextBuildingConfig | None = None):
        self.config = config or TextBuildingConfig()

    def build_chunk_text(
        self,
        chunk: Chunk,
        contexts: list[MessageContext],
    ) -> tuple[str, int]:
        """Build the text representation for a chunk.

        Args:
            chunk: The chunk to build text for
            contexts: MessageContext objects for each message in the chunk,
                     ordered by message_id

        Returns:
            Tuple of (formatted_text, token_count)
        """
        if chunk.chunk_type == "reply_chain":
            text = self._build_reply_chain_text(contexts)
        else:
            # sliding_window and author_group use the same format
            text = self._build_sequential_text(contexts)

        token_count = estimate_tokens(text)
        return text, token_count

    def _build_sequential_text(self, contexts: list[MessageContext]) -> str:
        """Build text for sliding_window or author_group chunks.

        Format:
            [Alice] 2024-01-15
            Hello everyone!
            [Image: screenshot.png]

            [Bob] 2024-01-15
            Got it!
        """
        parts: list[str] = []

        for ctx in contexts:
            formatted = self._format_message(ctx)
            parts.append(formatted)

        return "\n\n".join(parts)

    def _build_reply_chain_text(self, contexts: list[MessageContext]) -> str:
        """Build text for reply_chain chunks.

        Format:
            [Alice] 2024-01-15
            What's the bug?

            ↳ [Bob] 2024-01-15
            See error
            [Image: error.png]
        """
        parts: list[str] = []

        for i, ctx in enumerate(contexts):
            formatted = self._format_message(ctx)
            if i > 0:
                # Add reply indicator for non-root messages
                formatted = "↳ " + formatted
            parts.append(formatted)

        return "\n\n".join(parts)

    def _format_message(self, ctx: MessageContext) -> str:
        """Format a single message with its context.

        Format:
            [Author] YYYY-MM-DD
            Message content
            [Image: filename.png]
            [Embed: "Title" - Description...]
        """
        lines: list[str] = []

        # Header: [Author] Date
        author_display = ctx.author_username or str(ctx.message.author_id)
        date_str = ctx.message.created_at.strftime(self.config.date_format)
        lines.append(f"[{author_display}] {date_str}")

        # Content
        if ctx.message.content:
            lines.append(ctx.message.content)

        # Attachments
        for att in ctx.attachments:
            lines.append(self._format_attachment(att))

        # Embeds
        if ctx.message.embeds:
            for embed in ctx.message.embeds:
                embed_text = self._format_embed(embed)
                if embed_text:
                    lines.append(embed_text)

        return "\n".join(lines)

    def _format_attachment(self, att: Attachment) -> str:
        """Format an attachment.

        Format: [Label: filename.ext]
        Filename is truncated if too long.
        """
        filename = att.filename
        if len(filename) > self.config.max_filename_length:
            # Truncate, preserving extension
            ext_idx = filename.rfind(".")
            if ext_idx > 0:
                ext = filename[ext_idx:]
                base_len = self.config.max_filename_length - len(ext) - 3  # for "..."
                filename = filename[:base_len] + "..." + ext
            else:
                filename = filename[: self.config.max_filename_length - 3] + "..."

        label = self._classify_extension(filename)
        return f"[{label}: {filename}]"

    def _format_embed(self, embed: dict) -> str:
        """Format an embed.

        Extracts: title, description, author.name, fields[].name+value, footer.text
        Format: [Embed: "Title" - Description... | Field: value | ...]
        """
        parts: list[str] = []

        # Title
        title = embed.get("title")
        if title:
            parts.append(f'"{title}"')

        # Author name
        author = embed.get("author")
        if author and isinstance(author, dict):
            author_name = author.get("name")
            if author_name:
                parts.append(f"by {author_name}")

        # Description (truncate if long)
        description = embed.get("description")
        if description:
            if len(description) > 200:
                description = description[:197] + "..."
            parts.append(description)

        # Fields
        fields = embed.get("fields")
        if fields and isinstance(fields, list):
            for field in fields[:5]:  # Limit to first 5 fields
                if isinstance(field, dict):
                    name = field.get("name", "")
                    value = field.get("value", "")
                    if name and value:
                        if len(value) > 100:
                            value = value[:97] + "..."
                        parts.append(f"{name}: {value}")

        # Footer
        footer = embed.get("footer")
        if footer and isinstance(footer, dict):
            footer_text = footer.get("text")
            if footer_text:
                parts.append(footer_text)

        if not parts:
            return ""

        return "[Embed: " + " - ".join(parts) + "]"

    def _classify_extension(self, filename: str) -> str:
        """Classify a filename by its extension.

        Returns a human-readable label like 'Image', 'Video', 'Code', etc.
        """
        ext_idx = filename.rfind(".")
        if ext_idx < 0:
            return "File"

        ext = filename[ext_idx + 1 :].lower()
        return EXTENSION_LABELS.get(ext, "File")
