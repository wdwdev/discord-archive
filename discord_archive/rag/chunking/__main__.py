"""CLI entry point for discord_archive.rag.chunking.

Usage:
    python -m discord_archive.rag.chunking                    # Process all guilds
    python -m discord_archive.rag.chunking --guild-id 123     # Process specific guild
    python -m discord_archive.rag.chunking --channel-id 456   # Process specific channel
    python -m discord_archive.rag.chunking --verbose          # Show more details
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from discord_archive.config.settings import get_settings
from discord_archive.rag.chunking.logger import logger
from discord_archive.rag.chunking.run import run_chunking
from discord_archive.utils.logging import setup_logging


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Discord Archive Chunking",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m discord_archive.rag.chunking
      Process all guilds with messages in the database

  python -m discord_archive.rag.chunking --guild-id 123456789
      Process only the specified guild

  python -m discord_archive.rag.chunking --channel-id 987654321
      Process only the specified channel

  python -m discord_archive.rag.chunking --config /path/to/config.json
      Use a custom config file

  python -m discord_archive.rag.chunking --debug
      Enable debug logging
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
        help="Enable verbose output (DEBUG level)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode with third-party library logs",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        help="Optional file to write logs to",
    )

    args = parser.parse_args()

    # Configure logging based on CLI flags
    if args.debug:
        log_level = logging.DEBUG
        debug_third_party = True
    elif args.verbose:
        log_level = logging.DEBUG
        debug_third_party = False
    else:
        log_level = logging.INFO
        debug_third_party = False

    setup_logging(
        level=log_level,
        log_file=args.log_file,
        debug_third_party=debug_third_party,
    )

    logger.info("Starting Discord Archive Chunking")

    try:
        # Load settings to get database URL
        settings = get_settings(args.config)

        asyncio.run(
            run_chunking(
                database_url=settings.database_url,
                guild_id=args.guild_id,
                channel_id=args.channel_id,
            )
        )
        logger.success("Chunking complete!")
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()
