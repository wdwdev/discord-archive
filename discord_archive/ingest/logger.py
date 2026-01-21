"""Rich-based logging utilities for Discord ingest pipeline.

Provides beautiful console output with progress bars, live status updates,
and color-coded messages for different event types.
"""

from __future__ import annotations

from typing import Any

from discord_archive.utils.pipeline_logger import BasePipelineLogger


class IngestLogger(BasePipelineLogger):
    """Logger for Discord ingest operations with rich output.

    Extends BasePipelineLogger with ingest-specific methods for
    guild, entity, and channel processing output.
    """

    def __init__(self) -> None:
        super().__init__(__name__)

    # -------------------------------------------------------------------------
    # Ingest-specific: Rate Limiting & Retries
    # -------------------------------------------------------------------------

    def rate_limit(self, retry_after: float) -> None:
        """Log a rate limit warning with retry time."""
        self._logger.warning(f"Rate limited. Waiting {retry_after:.1f}s...")

    def retry(
        self, attempt: int, max_attempts: int, wait_time: float, reason: str = ""
    ) -> None:
        """Log a retry attempt with optional reason."""
        msg = f"Retry {attempt}/{max_attempts} in {wait_time:.1f}s"
        if reason:
            msg += f" ({reason})"
        self._logger.warning(msg)

    # -------------------------------------------------------------------------
    # Ingest-specific: Guild Processing
    # -------------------------------------------------------------------------

    def guild_start(self, guild_id: int, guild_name: str) -> None:
        """Log the start of guild processing."""
        self.console.print()
        self.console.rule(f"[bold cyan]{guild_name}[/bold cyan]", style="cyan")
        self.console.print(f"[dim]Guild ID: {guild_id}[/dim]")

    # -------------------------------------------------------------------------
    # Ingest-specific: Entity Processing
    # -------------------------------------------------------------------------

    def entity_start(self, entity_type: str) -> None:
        """Log the start of entity ingestion."""
        self.console.print(f"\n[bold]{entity_type}[/bold]")

    def entity_progress(self, fetched: int, total: int | None = None) -> None:
        """Log entity fetch progress (inline update)."""
        total_str = f"/{total:,}" if total else ""
        print("\033[2K", end="")
        self.console.print(
            f"  [dim]Fetched {fetched:,}{total_str}...[/dim]",
            end="\r",
        )
        self._has_progress_line = True

    def entity_complete(self, count: int) -> None:
        """Log entity ingestion completion."""
        self._clear_progress_line()
        self.console.print(f"  [green]✓[/green] {count:,} ingested")

    def entity_skip(self, reason: str) -> None:
        """Log skipped entity ingestion."""
        self._clear_progress_line()
        self.console.print(f"  [dim]Skipped: {reason}[/dim]")

    # -------------------------------------------------------------------------
    # Ingest-specific: Channel Processing
    # -------------------------------------------------------------------------

    def channel_start(self, channel_name: str, channel_type: str, mode: str) -> None:
        """Log the start of channel processing."""
        mode_color = "magenta" if mode == "backfill" else "green"
        self.console.print(
            f"\n[bold]{channel_name}[/bold] [dim]({channel_type})[/dim] "
            f"[[{mode_color}]{mode}[/{mode_color}]]"
        )

    def channel_complete(
        self, channel_name: str, message_count: int, mode: str
    ) -> None:
        """Log channel completion."""
        self._clear_progress_line()
        self.console.print(f"  [green]✓[/green] {message_count:,} messages ({mode})")

    def channel_empty(self, channel_name: str) -> None:
        """Log an empty channel."""
        self.console.print("  [dim]Empty channel, skipping[/dim]")

    def channel_skip(self, channel_name: str, reason: str) -> None:
        """Log a skipped channel."""
        self._clear_progress_line()
        self.console.print(f"    [dim]Skipped: {reason}[/dim]")

    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------

    def summary(
        self,
        guilds: int = 0,
        channels: int = 0,
        messages: int = 0,
        elapsed: float = 0.0,
        **kwargs: Any,
    ) -> None:
        """Print final ingest summary."""
        self.print_summary(
            "Ingest",
            elapsed=elapsed,
            stats={
                "Guilds": guilds,
                "Channels": channels,
                "Messages ingested": messages,
            },
            style="cyan",
        )


# Global logger instance
logger = IngestLogger()
