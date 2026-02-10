"""Token estimation for chunking.

Uses Mistral tokenizer for token estimation.
Requires the transformers package and network access on first run.
"""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

from transformers import AutoTokenizer, PreTrainedTokenizerBase

if TYPE_CHECKING:
    from discord_archive.db.models.attachment import Attachment
    from discord_archive.db.models.message import Message

# Model used for tokenization - must match embedding model's tokenizer
TOKENIZER_MODEL = "mistralai/Mistral-7B-v0.1"


class TokenizerLoadError(Exception):
    """Raised when the tokenizer cannot be loaded."""

    pass


@lru_cache(maxsize=1)
def get_tokenizer() -> PreTrainedTokenizerBase:
    """Get the Mistral tokenizer (cached).

    Raises:
        TokenizerLoadError: If the tokenizer cannot be loaded.
    """
    try:
        return AutoTokenizer.from_pretrained(TOKENIZER_MODEL)
    except Exception as e:
        raise TokenizerLoadError(
            f"Failed to load tokenizer '{TOKENIZER_MODEL}'. "
            f"Ensure 'transformers' is installed and you have network access "
            f"on first run. Error: {e}"
        ) from e


@lru_cache(maxsize=16384)
def estimate_tokens(text: str) -> int:
    """Estimate the number of tokens in a text using Mistral tokenizer.

    Results are cached (LRU, 16K entries) since messages are often
    processed multiple times during chunking.

    Raises:
        TokenizerLoadError: If the tokenizer cannot be loaded.
    """
    if not text:
        return 0
    return len(get_tokenizer().encode(text, add_special_tokens=False))


def truncate_to_tokens(text: str, max_tokens: int) -> str:
    """Truncate text to fit within max_tokens using tokenizer.

    Uses binary search for efficiency. Appends "..." if truncated.

    Args:
        text: Text to truncate
        max_tokens: Maximum number of tokens allowed

    Returns:
        Truncated text (with "..." if truncated)
    """
    current_tokens = estimate_tokens(text)
    if current_tokens <= max_tokens:
        return text

    # Binary search for the right character cutoff
    left, right = 0, len(text)
    result = text[:right]

    while left < right:
        mid = (left + right + 1) // 2
        candidate = text[:mid] + "..."
        tokens = estimate_tokens(candidate)

        if tokens <= max_tokens:
            left = mid
            result = candidate
        else:
            right = mid - 1

    return result


def estimate_message_context_tokens(
    message: "Message",
    author_username: str | None,
    attachments: list["Attachment"],
    date_format: str = "%Y-%m-%d",
) -> int:
    """Estimate tokens for a formatted message.

    Uses the exact same formatting logic as TextBuilder to ensure
    the token estimate matches the final chunk text.

    Args:
        message: Message object
        author_username: Author username (or None to use user_id)
        attachments: List of Attachment objects for this message
        date_format: Date format string (default: YYYY-MM-DD)

    Returns:
        Estimated token count for the formatted message

    Raises:
        TokenizerLoadError: If the tokenizer cannot be loaded.
    """
    lines = []

    # Header: [Author] Date
    author_display = author_username or str(message.author_id)
    date_str = message.created_at.strftime(date_format)
    lines.append(f"[{author_display}] {date_str}")

    # Content
    if message.content:
        lines.append(message.content)

    # Embeds (build exact formatted text with token-based truncation)
    if message.embeds:
        for embed in message.embeds:
            if isinstance(embed, dict):
                embed_text = _format_embed_with_token_limits(embed)
                if embed_text:
                    lines.append(embed_text)

    # Attachments (use exact formatting matching TextBuilder)
    if attachments:
        for att in attachments:
            att_text = _format_attachment_for_estimation(att)
            lines.append(att_text)

    # Build complete text and count tokens
    text = "\n".join(lines)
    base_tokens = estimate_tokens(text)

    # Add separator tokens between messages (\n\n)
    separator_tokens = 2

    return base_tokens + separator_tokens


def _format_embed_with_token_limits(embed: dict) -> str:
    """Format an embed with token-based truncation (mirrors TextBuilder logic).

    Uses tokenizer to limit field lengths accurately, rather than character count.

    Args:
        embed: Embed dictionary from message.embeds

    Returns:
        Formatted embed string matching TextBuilder._format_embed
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

    # Description (truncate to 60 tokens to match TextBuilder)
    description = embed.get("description")
    if description:
        truncated = truncate_to_tokens(description, 60)
        parts.append(truncated)

    # Fields (first 5, value truncated to 30 tokens to match TextBuilder)
    fields = embed.get("fields")
    if fields and isinstance(fields, list):
        for field in fields[:5]:
            if isinstance(field, dict):
                name = field.get("name", "")
                value = field.get("value", "")
                if name and value:
                    value_truncated = truncate_to_tokens(value, 30)
                    parts.append(f"{name}: {value_truncated}")

    # Footer
    footer = embed.get("footer")
    if footer and isinstance(footer, dict):
        footer_text = footer.get("text")
        if footer_text:
            parts.append(footer_text)

    if not parts:
        return ""

    return "[Embed: " + " - ".join(parts) + "]"


def _format_attachment_for_estimation(att: "Attachment") -> str:
    """Format an attachment for token estimation (mirrors TextBuilder logic).

    Args:
        att: Attachment object

    Returns:
        Formatted attachment string matching TextBuilder._format_attachment
    """
    # Import here to get EXTENSION_LABELS
    from discord_archive.rag.chunking.text_builder import EXTENSION_LABELS

    filename = att.filename
    max_filename_length = 100

    # Truncate long filenames
    if len(filename) > max_filename_length:
        ext_idx = filename.rfind(".")
        if ext_idx > 0:
            ext = filename[ext_idx:]
            base_len = max_filename_length - len(ext) - 3
            filename = filename[:base_len] + "..." + ext
        else:
            filename = filename[: max_filename_length - 3] + "..."

    # Classify extension
    ext_idx = filename.rfind(".")
    if ext_idx < 0:
        label = "File"
    else:
        ext = filename[ext_idx + 1 :].lower()
        label = EXTENSION_LABELS.get(ext, "File")

    return f"[{label}: {filename}]"
