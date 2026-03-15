"""CLI entry point for the Galaxy web server.

Usage:
    uv run --extra rag --extra galaxy python -m discord_archive.galaxy
"""

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "discord_archive.galaxy.app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
