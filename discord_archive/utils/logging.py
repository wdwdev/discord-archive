"""Centralized logging configuration with rich integration.

This module provides:
- A single shared Console instance for all rich output
- Logging configuration with RichHandler
- Optional file logging with traditional formatting

Usage:
    from discord_archive.utils.logging import console, setup_logging
    import logging

    setup_logging(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    logger.info("Hello")

IMPORTANT:
- Do NOT create additional Console() instances anywhere else
- Always use logging.getLogger(__name__) for module-specific loggers
- Call setup_logging() once at application startup, not at import time
"""

from __future__ import annotations

import logging
from pathlib import Path

from rich.console import Console
from rich.logging import RichHandler


# The ONE and ONLY shared Console instance for the entire project.
# All RichHandler, Progress, Live, Status, and tables MUST use this console.
console = Console()


def setup_logging(
    level: int = logging.INFO,
    log_file: str | Path | None = None,
    debug_third_party: bool = False,
) -> None:
    """Configure logging with RichHandler using the shared console.

    This function should be called ONCE at application startup (e.g., in CLI main).
    Do NOT call this at import time in any module.

    Args:
        level: Logging level for the root logger (default: INFO)
        log_file: Optional path to a log file for persistent logging
        debug_third_party: If True, set httpx/sqlalchemy to DEBUG level
    """
    handlers: list[logging.Handler] = [
        RichHandler(
            console=console,
            show_path=False,
            rich_tracebacks=True,
            tracebacks_show_locals=False,
        )
    ]

    # Optional file handler with traditional formatting
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        handlers.append(file_handler)

    # Configure root logger with force=True to avoid silent misconfiguration
    logging.basicConfig(
        level=level,
        handlers=handlers,
        format="%(message)s",
        force=True,
    )

    # Adjust third-party library log levels
    if debug_third_party:
        logging.getLogger("httpx").setLevel(logging.DEBUG)
        logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
    else:
        # Quiet third-party libs by default
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
