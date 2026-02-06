"""Chunking for RAG.

Converts messages into semantic chunks for retrieval.
"""

from discord_archive.rag.chunking.author_group import (
    AuthorGroupChunker,
    AuthorGroupConfig,
    AuthorGroupState,
)
from discord_archive.rag.chunking.constants import THREAD_STARTER_MESSAGE_TYPE
from discord_archive.rag.chunking.processor import (
    ChunkingConfig,
    ChunkingProcessor,
    ChunkingStats,
)
from discord_archive.rag.chunking.reply_chain import (
    ReplyChainChunker,
    ReplyChainConfig,
)
from discord_archive.rag.chunking.run import ChunkingOrchestrator, run_chunking
from discord_archive.rag.chunking.sliding_window import (
    SlidingWindowChunker,
    SlidingWindowConfig,
    SlidingWindowState,
)
from discord_archive.rag.chunking.text_builder import (
    MessageContext,
    TextBuilder,
    TextBuildingConfig,
)
from discord_archive.rag.chunking.tokenizer import TokenizerLoadError, estimate_tokens

__all__ = [
    # Main entry points
    "run_chunking",
    "ChunkingOrchestrator",
    # Processor
    "ChunkingProcessor",
    "ChunkingConfig",
    "ChunkingStats",
    # Sliding window
    "SlidingWindowChunker",
    "SlidingWindowConfig",
    "SlidingWindowState",
    # Author group
    "AuthorGroupChunker",
    "AuthorGroupConfig",
    "AuthorGroupState",
    # Reply chain
    "ReplyChainChunker",
    "ReplyChainConfig",
    # Text builder
    "TextBuilder",
    "TextBuildingConfig",
    "MessageContext",
    # Tokenizer
    "estimate_tokens",
    "TokenizerLoadError",
    # Constants
    "THREAD_STARTER_MESSAGE_TYPE",
]
