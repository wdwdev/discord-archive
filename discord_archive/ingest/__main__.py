"""CLI entry point for discord_archive.ingest.

Usage:
    python -m discord_archive.ingest                    # Process all guilds
    python -m discord_archive.ingest --guild-id 123     # Process specific guild
    python -m discord_archive.ingest --channel-id 456   # Process specific channel
    python -m discord_archive.ingest --verbose          # Show more details
    python -m discord_archive.ingest --debug            # Show debug info
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from discord_archive.ingest.logger import logger
from discord_archive.ingest.run import run_ingest
from discord_archive.utils.logging import setup_logging


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Discord Archive Ingest",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m discord_archive.ingest
      Process all guilds defined in config.json

  python -m discord_archive.ingest --guild-id 123456789
      Process only the specified guild

  python -m discord_archive.ingest --channel-id 987654321
      Process only the specified channel

  python -m discord_archive.ingest --config /path/to/config.json
      Use a custom config file

  python -m discord_archive.ingest -v
      Enable verbose output (DEBUG level)

  python -m discord_archive.ingest -v --show-sql
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

    logger.info("Starting Discord Archive Ingest")

    try:
        asyncio.run(
            run_ingest(
                config_path=args.config,
                guild_id=args.guild_id,
                channel_id=args.channel_id,
            )
        )
        logger.success("Ingest complete!")
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()
