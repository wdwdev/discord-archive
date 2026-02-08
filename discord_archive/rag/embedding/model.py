"""Embedding model wrapper for NV-Embed-v2.

Wraps the NV-Embed-v2 model using the transformers AutoModel API
for encoding chunk texts into dense vectors.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass

import numpy as np
import torch
import torch.nn.functional as F
from transformers import AutoModel


@dataclass
class EmbeddingModelConfig:
    """Configuration for the embedding model."""

    model_name: str = "nvidia/NV-Embed-v2"
    max_length: int = 32768
    batch_token_budget: int = 8_000


class EmbeddingModel:
    """NV-Embed-v2 embedding model wrapper.

    Uses AutoModel.from_pretrained with trust_remote_code=True
    as required by the NV-Embed-v2 model card.

    Documents are encoded with an empty instruction prefix.
    Embeddings are L2-normalized before returning.

    Requires transformers>=4.42,<4.45 for compatibility with
    NV-Embed-v2's remote model code.
    """

    DIMENSION = 4096

    def __init__(self, config: EmbeddingModelConfig | None = None) -> None:
        self.config = config or EmbeddingModelConfig()
        self._model = None

    def load(self) -> None:
        """Load the model onto GPU (or CPU if no GPU available)."""
        self._model = AutoModel.from_pretrained(
            self.config.model_name,
            trust_remote_code=True,
            torch_dtype=torch.float16,
        )
        self._model.eval()
        if torch.cuda.is_available():
            self._model = self._model.cuda()

    def unload(self) -> None:
        """Unload the model and free GPU memory."""
        self._model = None
        if torch.cuda.is_available():
            torch.cuda.empty_cache()

    def encode_documents(self, texts: list[str]) -> np.ndarray:
        """Encode document texts into normalized embeddings.

        Documents use an empty instruction prefix per the model card.

        Args:
            texts: List of document texts to encode.

        Returns:
            np.ndarray of shape (n, 4096), dtype float32, L2-normalized.
        """
        if self._model is None:
            raise RuntimeError("Model not loaded. Call load() first.")

        with torch.no_grad(), warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="To copy construct from a tensor")
            warnings.filterwarnings("ignore", message=".*sdp_kernel.*is deprecated")
            embeddings = self._model.encode(
                texts,
                instruction="",
                max_length=self.config.max_length,
            )
            if isinstance(embeddings, np.ndarray):
                embeddings = torch.from_numpy(embeddings)
            embeddings = F.normalize(embeddings, p=2, dim=1)

        return embeddings.cpu().numpy().astype(np.float32)

    @property
    def dimension(self) -> int:
        """Return the embedding dimension (4096 for NV-Embed-v2)."""
        return self.DIMENSION
