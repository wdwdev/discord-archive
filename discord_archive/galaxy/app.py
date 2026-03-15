"""FastAPI server for the Semantic Galaxy visualizer.

Serves the React frontend, projection binary files, and API
endpoints for guild/channel metadata, chunk text, and search.

Usage:
    uv run --extra rag --extra galaxy python -m discord_archive.galaxy
"""

from __future__ import annotations

import gzip
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import sqlalchemy
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from fastapi.staticfiles import StaticFiles
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from discord_archive.config.settings import get_settings

from discord_archive.rag.embedding.lancedb_store import LanceDBStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

logger = logging.getLogger(__name__)

PROJECTIONS_DIR = Path("data/projections")
FRONTEND_DIR = Path("web/dist")


class AppState:
    """Shared application state."""

    def __init__(self, engine: AsyncEngine, lancedb_store: LanceDBStore) -> None:
        self.engine = engine
        self.lancedb_store = lancedb_store
        self._embedding_model: object | None = None

    @property
    def embedding_model(self):
        """Lazy-load the embedding model on first search."""
        if self._embedding_model is None:
            from discord_archive.rag.embedding.model import EmbeddingModel

            logger.info("Loading NV-Embed-v2 model...")
            self._embedding_model = EmbeddingModel()
            self._embedding_model.load()
            logger.info("Model loaded.")
        return self._embedding_model

    def unload_model(self) -> None:
        if self._embedding_model is not None:
            self._embedding_model.unload()
            self._embedding_model = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Initialize DB engine and LanceDB on startup."""
    settings = get_settings()
    db_url = settings.readonly_database_url or settings.database_url

    engine = create_async_engine(db_url, echo=False, pool_pre_ping=True)
    store = LanceDBStore("data/lancedb")
    store.connect()

    app.state.ctx = AppState(engine=engine, lancedb_store=store)
    try:
        yield
    finally:
        app.state.ctx.unload_model()
        await engine.dispose()


app = FastAPI(title="Semantic Galaxy", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _ctx() -> AppState:
    return app.state.ctx


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------


@app.get("/api/guilds")
async def list_guilds() -> list[dict]:
    """Return guilds that have projection files available."""
    ctx = _ctx()
    query = sqlalchemy.text(
        "SELECT guild_id, name FROM guilds ORDER BY name"
    )
    async with ctx.engine.connect() as conn:
        rows = (await conn.execute(query)).fetchall()

    guilds = []
    for guild_id, name in rows:
        projection_file = PROJECTIONS_DIR / f"{guild_id}.bin"
        if projection_file.exists():
            guilds.append({
                "guild_id": str(guild_id),
                "name": name,
                "has_projection": True,
            })
    return guilds


@app.get("/api/guilds/{guild_id}/channels")
async def list_channels(guild_id: str) -> list[dict]:
    """Return channels for a guild."""
    ctx = _ctx()
    query = sqlalchemy.text(
        "SELECT channel_id, name, parent_id, type "
        "FROM channels WHERE guild_id = :gid "
        "ORDER BY position, name"
    )
    async with ctx.engine.connect() as conn:
        rows = (await conn.execute(query, {"gid": int(guild_id)})).fetchall()

    return [
        {
            "channel_id": str(row[0]),
            "name": row[1],
            "parent_id": str(row[2]) if row[2] else None,
            "type": row[3],
        }
        for row in rows
    ]


@app.get("/api/projections/{guild_id}")
async def get_projection(guild_id: str) -> Response:
    """Serve a guild's projection binary file with gzip compression."""
    filepath = PROJECTIONS_DIR / f"{guild_id}.bin"
    if not filepath.exists():
        raise HTTPException(status_code=404, detail="Projection not found")

    raw = filepath.read_bytes()
    compressed = gzip.compress(raw, compresslevel=6)

    return Response(
        content=compressed,
        media_type="application/octet-stream",
        headers={
            "Content-Encoding": "gzip",
            "Cache-Control": "public, max-age=86400",
        },
    )


@app.get("/api/chunks/{chunk_id}")
async def get_chunk(chunk_id: int) -> dict:
    """Return chunk text and metadata."""
    ctx = _ctx()
    query = sqlalchemy.text(
        "SELECT ct.text, c.guild_id, c.channel_id, c.chunk_type, "
        "c.first_message_at, c.last_message_at "
        "FROM chunk_texts ct "
        "JOIN chunks c ON c.chunk_id = ct.chunk_id "
        "WHERE ct.chunk_id = :cid"
    )
    async with ctx.engine.connect() as conn:
        row = (await conn.execute(query, {"cid": chunk_id})).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="Chunk not found")

    channel_query = sqlalchemy.text(
        "SELECT name FROM channels WHERE channel_id = :ch_id"
    )
    async with ctx.engine.connect() as conn:
        ch_row = (await conn.execute(channel_query, {"ch_id": row[2]})).fetchone()

    return {
        "chunk_id": chunk_id,
        "text": row[0],
        "guild_id": str(row[1]),
        "channel_id": str(row[2]),
        "channel_name": ch_row[0] if ch_row else None,
        "chunk_type": row[3],
        "first_message_at": row[4].isoformat() if row[4] else None,
        "last_message_at": row[5].isoformat() if row[5] else None,
    }


class SearchRequest(BaseModel):
    query: str
    guild_ids: list[str] | None = None
    limit: int = 50


@app.post("/api/search")
async def search(body: SearchRequest) -> list[dict]:
    """Semantic search across chunks.

    Returns chunk_ids with similarity scores, ordered by relevance.
    """
    ctx = _ctx()
    query_vector = ctx.embedding_model.encode_query(body.query)

    if body.guild_ids:
        all_results = []
        per_guild_limit = max(body.limit // len(body.guild_ids), 10)
        for gid_str in body.guild_ids:
            results = ctx.lancedb_store.search(
                query_vector, limit=per_guild_limit, guild_id=int(gid_str)
            )
            all_results.extend(results)
        all_results.sort(key=lambda r: r.distance)
    else:
        all_results = ctx.lancedb_store.search(query_vector, limit=body.limit)

    # Deduplicate by chunk_id
    seen: set[int] = set()
    deduped = []
    for r in all_results:
        if r.chunk_id not in seen:
            seen.add(r.chunk_id)
            deduped.append(r)
    all_results = deduped[:body.limit]

    # Fetch text previews and channel names for results
    chunk_ids = [r.chunk_id for r in all_results]
    channel_ids = list({r.channel_id for r in all_results})

    texts: dict[int, str] = {}
    channel_names: dict[int, str] = {}

    if chunk_ids:
        placeholders = ",".join(str(cid) for cid in chunk_ids)
        text_query = sqlalchemy.text(
            f"SELECT chunk_id, substr(text, 1, 150) FROM chunk_texts "
            f"WHERE chunk_id IN ({placeholders})"
        )
        async with ctx.engine.connect() as conn:
            for row in (await conn.execute(text_query)).fetchall():
                texts[row[0]] = row[1]

    if channel_ids:
        placeholders = ",".join(str(cid) for cid in channel_ids)
        ch_query = sqlalchemy.text(
            f"SELECT channel_id, name FROM channels "
            f"WHERE channel_id IN ({placeholders})"
        )
        async with ctx.engine.connect() as conn:
            for row in (await conn.execute(ch_query)).fetchall():
                channel_names[row[0]] = row[1]

    return [
        {
            "chunk_id": r.chunk_id,
            "distance": round(r.distance, 6),
            "guild_id": str(r.guild_id),
            "channel_id": str(r.channel_id),
            "channel_name": channel_names.get(r.channel_id),
            "preview": texts.get(r.chunk_id, ""),
        }
        for r in all_results
    ]


# Serve frontend in production
if FRONTEND_DIR.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True))
