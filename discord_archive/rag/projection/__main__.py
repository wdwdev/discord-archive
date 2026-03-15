"""CLI entry point for the projection pipeline.

Usage:
    uv run --extra rag --extra galaxy python -m discord_archive.rag.projection
"""

from discord_archive.rag.projection.compute import run

if __name__ == "__main__":
    run()
