"""Base pipeline logger with shared components.

Provides reusable building blocks for pipeline-specific loggers:
- StructuredBlock: Context manager for key-value style output
- BasePipelineLogger: Abstract base with common logging methods

All pipeline loggers should inherit from BasePipelineLogger to ensure consistent output.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Generator

from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table
from rich.text import Text

from discord_archive.utils.logging import console

if TYPE_CHECKING:
    from typing import Self


class StructuredBlock:
    """A context manager for displaying structured key-value info blocks.

    Usage:
        with logger.block("channel-name") as block:
            block.field("channel ID", 123456789)
            block.field("mode", "backfill", color="magenta")
            # ... do processing ...
            block.result("ingested 1,234 messages", success=True)

    Output:
        channel-name
            channel ID: 123456789
            mode: backfill
            ✓ ingested 1,234 messages
    """

    def __init__(self, title: str, parent: "BasePipelineLogger") -> None:
        self.title = title
        self.console = parent.console
        self._parent = parent

    def __enter__(self) -> "Self":
        self._parent._clear_progress_line()
        self.console.print(f"\n[bold]{self.title}[/bold]")
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._parent._clear_progress_line()

    def field(self, key: str, value: Any, color: str | None = None) -> None:
        """Add a key-value field to the block."""
        if color:
            self.console.print(f"    [dim]{key}:[/dim] [{color}]{value}[/{color}]")
        else:
            self.console.print(f"    [dim]{key}:[/dim] {value}")

    def progress(self, message: str) -> None:
        """Show an inline progress update (will be overwritten)."""
        print("\033[2K", end="")  # ANSI clear line
        self.console.print(f"    [dim]{message}[/dim]", end="\r")
        self._parent._has_progress_line = True

    def result(self, message: str, success: bool = True) -> None:
        """Show the final result of the block."""
        self._parent._clear_progress_line()
        icon = "[green]✓[/green]" if success else "[red]✗[/red]"
        self.console.print(f"    {icon} {message}")

    def skip(self, reason: str) -> None:
        """Show that this block was skipped."""
        self._parent._clear_progress_line()
        self.console.print(f"    [dim]Skipped: {reason}[/dim]")

    def empty(self) -> None:
        """Show that this block had no content to process."""
        self._parent._clear_progress_line()
        self.console.print("    [dim]Empty, skipping[/dim]")


class BasePipelineLogger(ABC):
    """Abstract base class for pipeline loggers.

    Provides common functionality:
    - Shared console instance
    - Progress line management
    - Standard logging methods (info, warning, error, debug)
    - Structured block context manager
    - Progress bar context manager

    Subclasses should implement pipeline-specific methods like
    summary(), channel_start(), etc.
    """

    def __init__(self, logger_name: str | None = None) -> None:
        """Initialize the pipeline logger.

        Args:
            logger_name: Name for the Python logger. If None, uses __name__.
        """
        self.console: Console = console
        self._has_progress_line = False
        self._logger = logging.getLogger(logger_name or self.__class__.__module__)

    # -------------------------------------------------------------------------
    # Progress Line Management
    # -------------------------------------------------------------------------

    def _clear_progress_line(self) -> None:
        """Clear the in-place progress line if present."""
        if self._has_progress_line:
            print("\033[2K", end="\r")
            self._has_progress_line = False

    # -------------------------------------------------------------------------
    # Structured Block Context Manager
    # -------------------------------------------------------------------------

    @contextmanager
    def block(self, title: str) -> Generator[StructuredBlock, None, None]:
        """Create a structured block for key-value style output.

        Args:
            title: The title/header of the block

        Yields:
            StructuredBlock for adding fields and results
        """
        block = StructuredBlock(title, self)
        with block:
            yield block

    # -------------------------------------------------------------------------
    # Standard Logging (goes through Python logging)
    # -------------------------------------------------------------------------

    def info(self, message: str) -> None:
        """Log an info message."""
        self._logger.info(message)

    def warning(self, message: str) -> None:
        """Log a warning message."""
        self._logger.warning(message)

    def error(self, message: str) -> None:
        """Log an error message."""
        self._logger.error(message)

    def debug(self, message: str) -> None:
        """Log a debug message."""
        self._logger.debug(message)

    def success(self, message: str) -> None:
        """Log a success message with green checkmark."""
        self.console.print(f"[green]✓[/green] {message}")

    # -------------------------------------------------------------------------
    # Common Rich Output Methods
    # -------------------------------------------------------------------------

    def batch_progress(
        self,
        count: int,
        total: int | None = None,
        *,
        oldest_date: str | None = None,
        newest_date: str | None = None,
        prefix: str = "Processed",
        unit: str = "messages",
    ) -> None:
        """Log batch processing progress (inline update).

        Args:
            count: Number of items processed so far
            total: Total number of items (optional)
            oldest_date: Oldest date in the batch (optional)
            newest_date: Newest date in the batch (optional)
            prefix: Action prefix (e.g., "Processed", "Fetched")
            unit: Unit name
        """
        date_info = ""
        if oldest_date and newest_date:
            date_info = f" [{oldest_date} → {newest_date}]"
        elif oldest_date:
            date_info = f" [→ {oldest_date}]"
        elif newest_date:
            date_info = f" [{newest_date} →]"

        total_str = f"/{total:,}" if total else ""
        print("\033[2K", end="")
        self.console.print(
            f"    [dim]{prefix} {count:,}{total_str} {unit}{date_info}[/dim]",
            end="\r",
        )
        self._has_progress_line = True

    @contextmanager
    def progress_context(
        self, description: str = "Processing..."
    ) -> Generator[Progress, None, None]:
        """Context manager for progress bar display."""
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=self.console,
        )
        with progress:
            yield progress

    # -------------------------------------------------------------------------
    # Summary Table Helper
    # -------------------------------------------------------------------------

    def _print_summary_table(
        self,
        title: str,
        rows: list[tuple[str, str | int]],
        *,
        style: str = "cyan",
    ) -> None:
        """Print a beautiful summary panel.

        Args:
            title: Panel title
            rows: List of (label, value) tuples
            style: Border color style (default: cyan)
        """
        self.console.print()

        # Build the content
        table = Table.grid(padding=(0, 2))
        table.add_column("Metric", style="bold")
        table.add_column("Value", justify="right", style="green")

        for label, value in rows:
            if isinstance(value, int):
                table.add_row(label, f"{value:,}")
            else:
                table.add_row(label, str(value))

        # Wrap in a panel for visual appeal
        panel = Panel(
            table,
            title=f"[bold]{title}[/bold]",
            border_style=style,
            padding=(1, 2),
        )
        self.console.print(panel)

    def print_summary(
        self,
        pipeline_name: str,
        *,
        elapsed: float,
        stats: dict[str, int | str],
        extra_sections: dict[str, dict[str, int]] | None = None,
        style: str = "cyan",
    ) -> None:
        """Print a unified pipeline summary.

        This is the preferred method for printing summaries.
        Provides consistent formatting across all pipelines.

        Args:
            pipeline_name: Name of the pipeline
            elapsed: Time elapsed in seconds
            stats: Main statistics as {label: value}
            extra_sections: Optional nested sections
            style: Border color style
        """
        rows: list[tuple[str, str | int]] = []

        # Add main stats
        for label, value in stats.items():
            rows.append((label, value))

        # Add extra sections with indentation
        if extra_sections:
            for section_name, section_stats in extra_sections.items():
                rows.append((f"[dim]{section_name}[/dim]", ""))
                for label, value in section_stats.items():
                    rows.append((f"  {label}", value))

        # Add elapsed time at the end
        rows.append(("Time elapsed", f"{elapsed:.1f}s"))

        self._print_summary_table(f"{pipeline_name} Complete", rows, style=style)

    # -------------------------------------------------------------------------
    # Abstract Methods (must be implemented by subclasses)
    # -------------------------------------------------------------------------

    @abstractmethod
    def summary(self, **kwargs: Any) -> None:
        """Print final summary. Implementation varies by pipeline."""
        ...
