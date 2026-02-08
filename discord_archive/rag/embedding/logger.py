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

    def channel_start(
        self,
        channel_name: str,
        channel_id: int,
        pending_count: int,
    ) -> None:
        """Log the start of channel processing."""
        self._clear_progress_line()
        self.console.print(f"\n[bold cyan]{channel_name}[/bold cyan]")
        self.console.print(f"    [dim]channel ID:[/dim] {channel_id}")
        self.console.print(f"    [dim]pending chunks:[/dim] {pending_count:,}")

    def channel_progress(self, chunks_embedded: int) -> None:
        """Log progress during channel processing."""
        print("\033[2K", end="")
        self.console.print(
            f"    [dim]Embedded {chunks_embedded:,} chunks[/dim]",
            end="\r",
        )
        self._has_progress_line = True

    def channel_complete(
        self,
        channel_name: str,
        chunks_embedded: int,
    ) -> None:
        """Log completion of channel processing."""
        self._clear_progress_line()
        self.console.print(
            f"    [green]✓[/green] {chunks_embedded:,} chunks embedded"
        )

    def channel_empty(self, channel_name: str) -> None:
        """Log that a channel had no pending chunks."""
        self._clear_progress_line()
        self.console.print("    [dim]No pending chunks[/dim]")

    def guild_start(self, guild_id: int, guild_name: str) -> None:
        """Log the start of guild processing."""
        self.console.rule(f"[bold blue]{guild_name}[/bold blue] ({guild_id})")

    def summary(self, **kwargs: Any) -> None:
        """Print final summary.

        Expected kwargs:
        - elapsed: float (seconds)
        - guilds: int
        - channels: int
        - chunks_embedded: int
        """
        elapsed = kwargs.get("elapsed", 0.0)
        stats = {
            "Guilds processed": kwargs.get("guilds", 0),
            "Channels processed": kwargs.get("channels", 0),
            "Chunks embedded": kwargs.get("chunks_embedded", 0),
        }
        self.print_summary("Embedding", elapsed=elapsed, stats=stats, style="green")


# Global logger instance
logger = EmbeddingLogger()
