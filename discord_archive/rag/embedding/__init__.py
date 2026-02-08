"""Embedding for RAG.

Encodes chunk texts into vectors and stores them in LanceDB.
"""

from discord_archive.rag.embedding.lancedb_store import LanceDBStore
from discord_archive.rag.embedding.model import EmbeddingModel, EmbeddingModelConfig
from discord_archive.rag.embedding.processor import (
    EmbeddingConfig,
    EmbeddingProcessor,
    EmbeddingStats,
)
from discord_archive.rag.embedding.run import EmbeddingOrchestrator, run_embedding

__all__ = [
    # Main entry points
    "run_embedding",
    "EmbeddingOrchestrator",
    # Processor
    "EmbeddingProcessor",
    "EmbeddingConfig",
    "EmbeddingStats",
    # Model
    "EmbeddingModel",
    "EmbeddingModelConfig",
    # Store
    "LanceDBStore",
]
