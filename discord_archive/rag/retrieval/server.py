"""FastMCP server providing semantic_search and sql_query tools.

Connects to a read-only PostgreSQL user and LanceDB for vector search.
The NV-Embed-v2 model is loaded lazily on first semantic_search call.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime

import httpx
import numpy as np
import sqlalchemy
from mcp.server.fastmcp import Context, FastMCP
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from discord_archive.config.settings import get_settings
from discord_archive.rag.embedding.lancedb_store import LanceDBStore, SearchResult
from discord_archive.rag.embedding.model import EmbeddingModel

logger = logging.getLogger(__name__)


@dataclass
class ServerState:
    """Shared state for the MCP server lifetime."""

    engine: AsyncEngine
    lancedb_store: LanceDBStore
    discord_token: str = ""
    _embedding_model: EmbeddingModel | None = field(default=None, init=False)
    _discord_client: httpx.AsyncClient | None = field(default=None, init=False)

    @property
    def embedding_model(self) -> EmbeddingModel:
        """Lazy-load the embedding model on first access."""
        if self._embedding_model is None:
            logger.info("Loading NV-Embed-v2 model (first semantic_search call)...")
            self._embedding_model = EmbeddingModel()
            self._embedding_model.load()
            logger.info("Model loaded.")
        return self._embedding_model

    @property
    def discord_client(self) -> httpx.AsyncClient:
        """Lazy-create Discord API client on first access."""
        if self._discord_client is None:
            self._discord_client = httpx.AsyncClient(
                base_url="https://discord.com/api/v10",
                headers={
                    "Authorization": self.discord_token,
                    "Content-Type": "application/json",
                },
                timeout=30.0,
            )
        return self._discord_client

    def unload_model(self) -> None:
        if self._embedding_model is not None:
            self._embedding_model.unload()
            self._embedding_model = None

    async def close(self) -> None:
        if self._discord_client is not None:
            await self._discord_client.aclose()
            self._discord_client = None


@asynccontextmanager
async def server_lifespan(server: FastMCP) -> AsyncIterator[ServerState]:
    """Initialize PostgreSQL engine and LanceDB on startup, dispose on shutdown."""
    settings = get_settings()
    db_url = settings.readonly_database_url or settings.database_url
    discord_token = settings.accounts[0].token if settings.accounts else ""

    engine = create_async_engine(db_url, echo=False, pool_pre_ping=True)
    store = LanceDBStore("data/lancedb")
    store.connect()

    state = ServerState(engine=engine, lancedb_store=store, discord_token=discord_token)
    try:
        yield state
    finally:
        await state.close()
        state.unload_model()
        await engine.dispose()


mcp = FastMCP("discord-archive", lifespan=server_lifespan)


def _serialize_results(
    results: list[SearchResult],
    texts: dict[int, str] | None = None,
) -> list[dict]:
    """Convert SearchResult list to JSON-serializable dicts."""
    out = []
    for r in results:
        entry: dict = {
            "chunk_id": r.chunk_id,
            "distance": round(r.distance, 6),
            "guild_id": r.guild_id,
            "channel_id": r.channel_id,
            "author_ids": r.author_ids,
            "first_message_at": r.first_message_at.isoformat() if r.first_message_at else None,
            "last_message_at": r.last_message_at.isoformat() if r.last_message_at else None,
        }
        if texts is not None and r.chunk_id in texts:
            entry["text"] = texts[r.chunk_id]
        out.append(entry)
    return out


async def _fetch_chunk_texts(
    engine: AsyncEngine, chunk_ids: list[int]
) -> dict[int, str]:
    """Batch-fetch chunk texts from PostgreSQL by chunk_id."""
    if not chunk_ids:
        return {}
    query = sqlalchemy.text(
        "SELECT chunk_id, text FROM chunk_texts WHERE chunk_id = ANY(:ids)"
    )
    async with engine.connect() as conn:
        result = await conn.execute(query, {"ids": chunk_ids})
        return {row[0]: row[1] for row in result.fetchall()}


def _parse_snowflake(value: str | None) -> int | None:
    """Parse a snowflake ID from string or int, returning None if empty.

    Strips surrounding quotes to handle LLM tool calls that pass
    snowflake strings as ``"583750578094735360"`` (with literal quotes).
    """
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip("\"'")
        if not value:
            return None
    return int(value)


@mcp.tool()
async def semantic_search(
    query: str,
    limit: int = 20,
    guild_id: str | None = None,
    channel_id: str | None = None,
    author_id: str | None = None,
    after: str | None = None,
    before: str | None = None,
    include_text: bool = False,
    instruction: str | None = None,
    ctx: Context | None = None,
) -> str:
    """Search Discord archive chunks by semantic similarity.

    Encodes the query with NV-Embed-v2 and performs ANN search in LanceDB.
    Returns JSON array of {chunk_id, distance, guild_id, channel_id, author_ids, timestamps}.
    Lower distance = more similar. Use sql_query to fetch chunk_text by chunk_id.

    Args:
        query: Natural language search query.
        limit: Max results (default 20).
        guild_id: Filter by guild ID (pass as string to avoid precision loss).
        channel_id: Filter by channel ID (pass as string to avoid precision loss).
        author_id: Filter by author ID (pass as string to avoid precision loss).
        after: Filter chunks after this ISO datetime.
        before: Filter chunks before this ISO datetime.
        include_text: Include chunk text in results (default false).
        instruction: Custom instruction prefix for the embedding model.
            Defaults to a Discord-optimized instruction. Use this to steer
            retrieval toward specific content types, e.g.
            "Instruct: Retrieve messages where someone describes their personal background or experience\\nQuery: "
    """
    state: ServerState = ctx.request_context.lifespan_context

    after_dt = datetime.fromisoformat(after) if after else None
    before_dt = datetime.fromisoformat(before) if before else None

    query_vector = state.embedding_model.encode_query(query, instruction=instruction)

    results = state.lancedb_store.search(
        query_vector,
        limit=limit,
        guild_id=_parse_snowflake(guild_id),
        channel_id=_parse_snowflake(channel_id),
        author_id=_parse_snowflake(author_id),
        after=after_dt,
        before=before_dt,
    )

    texts = None
    if include_text and results:
        chunk_ids = [r.chunk_id for r in results]
        texts = await _fetch_chunk_texts(state.engine, chunk_ids)

    return json.dumps(_serialize_results(results, texts), ensure_ascii=False)


_LIMIT_RE = re.compile(r"\bLIMIT\s+\d+", re.IGNORECASE)


@mcp.tool()
async def sql_query(
    sql: str,
    ctx: Context | None = None,
) -> str:
    """Execute a read-only SQL query against the PostgreSQL database.

    Returns JSON {columns, rows, row_count}. Security is enforced by database
    user permissions (read-only). Auto-appends LIMIT 500 if no LIMIT clause present.

    Args:
        sql: SQL query to execute (SELECT only, enforced by DB permissions).
    """
    state: ServerState = ctx.request_context.lifespan_context

    query = sql.rstrip().rstrip(";")
    if not _LIMIT_RE.search(query):
        query += " LIMIT 500"

    async with state.engine.connect() as conn:
        result = await conn.execute(sqlalchemy.text(query))
        columns = list(result.keys())
        rows = []
        for row in result.fetchall():
            rows.append([_to_json_safe(v) for v in row])

    return json.dumps(
        {"columns": columns, "rows": rows, "row_count": len(rows)},
        ensure_ascii=False,
        default=str,
    )


def _to_json_safe(value: object) -> object:
    """Convert database values to JSON-serializable types."""
    if value is None:
        return None
    if isinstance(value, (int, float, bool, str)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    return str(value)


@mcp.tool()
async def refresh_attachment_url(
    attachment_id: str | None = None,
    message_id: str | None = None,
    ctx: Context | None = None,
) -> str:
    """Refresh expired Discord CDN attachment URLs.

    Discord signed URLs expire after ~24 hours. This tool looks up
    attachment(s) from the database and refreshes their URLs via
    the Discord API.

    Args:
        attachment_id: Refresh a specific attachment by ID (pass as string to avoid precision loss).
        message_id: Refresh all attachments for a message (pass as string to avoid precision loss).
    """
    state: ServerState = ctx.request_context.lifespan_context

    attachment_id = _parse_snowflake(attachment_id)
    message_id = _parse_snowflake(message_id)

    if not attachment_id and not message_id:
        return json.dumps({"error": "Provide attachment_id or message_id"})

    if not state.discord_token:
        return json.dumps({"error": "No Discord token configured"})

    # Look up old URLs from database
    if attachment_id is not None:
        query = sqlalchemy.text(
            "SELECT attachment_id, message_id, filename, content_type, "
            "size, url, width, height "
            "FROM attachments WHERE attachment_id = :id"
        )
        bind = {"id": attachment_id}
    else:
        query = sqlalchemy.text(
            "SELECT attachment_id, message_id, filename, content_type, "
            "size, url, width, height "
            "FROM attachments WHERE message_id = :id"
        )
        bind = {"id": message_id}

    async with state.engine.connect() as conn:
        result = await conn.execute(query, bind)
        rows = result.fetchall()

    if not rows:
        return json.dumps({"error": "No attachments found"})

    # Refresh URLs via Discord API
    old_urls = [row[5] for row in rows]
    response = await state.discord_client.post(
        "/attachments/refresh-urls",
        json={"attachment_urls": old_urls},
    )

    if response.status_code != 200:
        return json.dumps(
            {"error": f"Discord API {response.status_code}: {response.text}"}
        )

    refreshed = response.json().get("refreshed_urls", [])
    url_map = {item["original"]: item["refreshed"] for item in refreshed}

    attachments = []
    for row in rows:
        old_url = row[5]
        attachments.append({
            "attachment_id": row[0],
            "message_id": row[1],
            "filename": row[2],
            "content_type": row[3],
            "size": row[4],
            "url": url_map.get(old_url, old_url),
            "width": row[6],
            "height": row[7],
        })

    return json.dumps(attachments, ensure_ascii=False)
