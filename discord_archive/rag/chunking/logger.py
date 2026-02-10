"""Logger for the chunking module.

Follows the same patterns as the ingest logger.
"""

from typing import Any

from discord_archive.utils.logger import BaseLogger


class ChunkingLogger(BaseLogger):
    """Logger for the chunking module.

    Provides structured logging for chunking operations.
    """

    def __init__(self) -> None:
        super().__init__("discord_archive.rag.chunking")

    def channel_start(
        self,
        channel_name: str,
        channel_id: int,
        last_message_id: int | None = None,
    ) -> None:
        """Log the start of channel processing."""
        self._clear_progress_line()
        self.console.print(f"\n[bold cyan]{channel_name}[/bold cyan]")
        self.console.print(f"    [dim]channel ID:[/dim] {channel_id}")
        if last_message_id:
            self.console.print(f"    [dim]resuming from:[/dim] {last_message_id}")
        else:
            self.console.print("    [dim]starting fresh[/dim]")

    def channel_complete(
        self,
        channel_name: str,
        messages_processed: int,
        sliding_window: tuple[int, int],  # (created, closed)
        author_group: tuple[int, int],
        reply_chain: int,
    ) -> None:
        """Log completion of channel processing."""
        self._clear_progress_line()
        self.console.print(f"    [green]✓[/green] {messages_processed:,} messages")
        self.console.print(
            f"      [dim]sliding_window:[/dim] {sliding_window[0]} created, "
            f"{sliding_window[1]} closed"
        )
        self.console.print(
            f"      [dim]author_group:[/dim] {author_group[0]} created, "
            f"{author_group[1]} closed"
        )
        self.console.print(f"      [dim]reply_chain:[/dim] {reply_chain} created")

    def channel_empty(self, channel_name: str) -> None:
        """Log that a channel had no messages to process."""
        self._clear_progress_line()
        self.console.print("    [dim]No new messages[/dim]")

    def guild_start(self, guild_id: int, guild_name: str) -> None:
        """Log the start of guild processing."""
        self.console.rule(f"[bold blue]{guild_name}[/bold blue] ({guild_id})")

    def summary(self, **kwargs: Any) -> None:
        """Print final summary.

        Expected kwargs:
        - elapsed: float (seconds)
        - guilds: int
        - channels: int
        - messages: int
        - chunks_created: int
        - chunks_closed: int
        """
        elapsed = kwargs.get("elapsed", 0.0)
        stats = {
            "Guilds processed": kwargs.get("guilds", 0),
            "Channels processed": kwargs.get("channels", 0),
            "Messages processed": kwargs.get("messages", 0),
            "Chunks created": kwargs.get("chunks_created", 0),
            "Chunks closed": kwargs.get("chunks_closed", 0),
        }
        self.print_summary("Chunking", elapsed=elapsed, stats=stats, style="green")


# Global logger instance
logger = ChunkingLogger()
