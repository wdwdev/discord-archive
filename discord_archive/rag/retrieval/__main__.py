"""Entry point for the MCP retrieval server."""

from discord_archive.rag.retrieval.server import mcp

mcp.run(transport="stdio")
