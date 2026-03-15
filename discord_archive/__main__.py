"""Unified CLI for discord-archive.

Usage:
    discord-archive ingest [OPTIONS]
    discord-archive chunk [OPTIONS]
    discord-archive embed [OPTIONS]
    discord-archive project
    discord-archive serve
"""

from __future__ import annotations

import argparse
import sys


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="discord-archive",
        description="Discord Archive — ingest, chunk, embed, project, serve.",
    )
    sub = parser.add_subparsers(dest="command")

    # Shared options for ingest/chunk/embed
    def add_common_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--config", default="config.json", help="Path to config.json")
        p.add_argument("--guild-id", type=int, help="Process only this guild")
        p.add_argument("--channel-id", type=int, help="Process only this channel")
        p.add_argument("-v", "--verbose", action="store_true", help="DEBUG logging")
        p.add_argument("--show-sql", action="store_true", help="Show SQL queries")
        p.add_argument("--log-file", type=str, help="Log to file")

    p_ingest = sub.add_parser("ingest", help="Download Discord data to PostgreSQL")
    add_common_args(p_ingest)

    p_chunk = sub.add_parser("chunk", help="Create semantic chunks from messages")
    add_common_args(p_chunk)

    p_embed = sub.add_parser("embed", help="Encode chunks with NV-Embed-v2")
    add_common_args(p_embed)

    p_project = sub.add_parser("project", help="Compute 3D projections (PCA + UMAP)")
    p_project.add_argument("--config", default="config.json", help="Path to config.json")

    p_serve = sub.add_parser("serve", help="Start Galaxy web server")
    p_serve.add_argument("--host", default="0.0.0.0", help="Bind host")
    p_serve.add_argument("--port", type=int, default=8000, help="Bind port")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command in ("ingest", "chunk", "embed"):
        import asyncio
        import logging

        from discord_archive.utils.logging import setup_logging

        log_level = logging.DEBUG if args.verbose else logging.INFO
        setup_logging(level=log_level, log_file=args.log_file, show_sql=args.show_sql)

    if args.command == "ingest":
        import asyncio

        from discord_archive.ingest.run import run_ingest

        asyncio.run(run_ingest(
            config_path=args.config,
            guild_id=args.guild_id,
            channel_id=args.channel_id,
        ))

    elif args.command == "chunk":
        import asyncio

        from discord_archive.config.settings import get_settings
        from discord_archive.rag.chunking.run import run_chunking

        settings = get_settings(args.config)
        asyncio.run(run_chunking(
            database_url=settings.database_url,
            guild_id=args.guild_id,
            channel_id=args.channel_id,
        ))

    elif args.command == "embed":
        import asyncio

        from discord_archive.config.settings import get_settings
        from discord_archive.rag.embedding.run import run_embedding

        settings = get_settings(args.config)
        asyncio.run(run_embedding(
            database_url=settings.database_url,
            guild_id=args.guild_id,
            channel_id=args.channel_id,
        ))

    elif args.command == "project":
        from discord_archive.rag.projection.compute import run

        run()

    elif args.command == "serve":
        import uvicorn

        uvicorn.run(
            "discord_archive.galaxy.app:app",
            host=args.host,
            port=args.port,
            reload=True,
        )


if __name__ == "__main__":
    main()
