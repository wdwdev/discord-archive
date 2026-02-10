"""CLI entry point for discord_archive.rag.embedding.

Usage:
    python -m discord_archive.rag.embedding                    # Process all guilds
    python -m discord_archive.rag.embedding --guild-id 123     # Process specific guild
    python -m discord_archive.rag.embedding --channel-id 456   # Process specific channel
    python -m discord_archive.rag.embedding --verbose          # Show more details
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from discord_archive.config.settings import get_settings
from discord_archive.rag.embedding.logger import logger
from discord_archive.rag.embedding.run import run_embedding
from discord_archive.utils.logging import setup_logging


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Discord Archive Embedding",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m discord_archive.rag.embedding
      Embed all pending chunks across all guilds

  python -m discord_archive.rag.embedding --guild-id 123456789
      Embed pending chunks for the specified guild only

  python -m discord_archive.rag.embedding --channel-id 987654321
      Embed pending chunks for the specified channel only

  python -m discord_archive.rag.embedding --config /path/to/config.json
      Use a custom config file

  python -m discord_archive.rag.embedding -v
      Enable verbose output (DEBUG level)

  python -m discord_archive.rag.embedding -v --show-sql
      Enable verbose output with SQL queries
        """,
    )

    parser.add_argument(
        "--config",
        type=str,
        default="config.json",
        help="Path to config.json (default: config.json)",
    )
    parser.add_argument(
        "--guild-id",
        type=int,
        help="Process only this guild ID",
    )
    parser.add_argument(
        "--channel-id",
        type=int,
        help="Process only this channel ID",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose output (DEBUG level, no SQL queries)",
    )
    parser.add_argument(
        "--show-sql",
        action="store_true",
        help="Show SQL queries (use with --verbose for full debug output)",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        help="Optional file to write logs to",
    )

    args = parser.parse_args()

    # Configure logging based on CLI flags
    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    show_sql = args.show_sql
    setup_logging(level=log_level, log_file=args.log_file, show_sql=show_sql)

    logger.info("Starting Discord Archive Embedding")

    try:
        settings = get_settings(args.config)

        asyncio.run(
            run_embedding(
                database_url=settings.database_url,
                guild_id=args.guild_id,
                channel_id=args.channel_id,
            )
        )
        logger.success("Embedding complete!")
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()
