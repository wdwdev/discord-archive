"""Logger for the embedding module.

Follows the same patterns as the chunking logger.
"""

from typing import Any

from discord_archive.utils.logger import BaseLogger


class EmbeddingLogger(BaseLogger):
    """Logger for the embedding module.

    Provides structured logging for embedding operations.
    """

    def __init__(self) -> None:
        super().__init__("discord_archive.rag.embedding")

    def model_loading(self) -> None:
        """Log that the model is being loaded."""
        self.console.print("\n[bold yellow]Loading NV-Embed-v2...[/bold yellow]")

    def model_loaded(self, elapsed: float) -> None:
        """Log that the model has been loaded."""
        self.console.print(
            f"[green]✓[/green] Model loaded in {elapsed:.1f}s"
        )

    def summary(self, **kwargs: Any) -> None:
        """Print final summary.

        Expected kwargs:
        - elapsed: float (seconds)
        - chunks_embedded: int
        """
        elapsed = kwargs.get("elapsed", 0.0)
        stats = {
            "Chunks embedded": kwargs.get("chunks_embedded", 0),
        }
        self.print_summary("Embedding", elapsed=elapsed, stats=stats, style="green")


# Global logger instance
logger = EmbeddingLogger()
