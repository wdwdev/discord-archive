"""LanceDB vector store for chunk embeddings.

Manages connection, table creation, and upsert operations
for storing chunk embeddings in LanceDB.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import lancedb
import numpy as np
import pyarrow as pa

CHUNKS_TABLE = "chunks"

CHUNKS_SCHEMA = pa.schema([
    pa.field("chunk_id", pa.int64()),
    pa.field("vector", pa.list_(pa.float32(), 4096)),
    pa.field("guild_id", pa.int64()),
    pa.field("channel_id", pa.int64()),
    pa.field("author_ids", pa.list_(pa.int64())),
    pa.field("mentioned_user_ids", pa.list_(pa.int64())),
    pa.field("mentioned_role_ids", pa.list_(pa.int64())),
    pa.field("has_attachments", pa.bool_()),
    pa.field("first_message_at", pa.timestamp("us", tz="UTC")),
    pa.field("last_message_at", pa.timestamp("us", tz="UTC")),
])


@dataclass
class SearchResult:
    """A single vector search result."""

    chunk_id: int
    distance: float
    guild_id: int
    channel_id: int
    author_ids: list[int]
    first_message_at: datetime | None
    last_message_at: datetime | None


class LanceDBStore:
    """LanceDB vector store for chunk embeddings.

    Handles connection management, table creation with a fixed
    pyarrow schema, and merge-insert (upsert) operations.
    """

    def __init__(self, data_dir: str | Path) -> None:
        """Initialize the store.

        Args:
            data_dir: Path to the LanceDB data directory.
        """
        self.data_dir = Path(data_dir)
        self._db = None

    def connect(self) -> None:
        """Connect to the LanceDB database.

        Creates the data directory if it doesn't exist.
        """
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._db = lancedb.connect(str(self.data_dir))
        self._ensure_table()

    def _ensure_table(self) -> None:
        """Create the chunks table if it doesn't exist."""
        if CHUNKS_TABLE not in self._db.list_tables().tables:
            self._db.create_table(CHUNKS_TABLE, schema=CHUNKS_SCHEMA)

    def add(self, records: pa.Table | list[dict]) -> None:
        """Append records to the chunks table.

        Fast bulk insert — use for new records that don't exist
        in the table yet (e.g. initial embedding of pending chunks).

        Args:
            records: PyArrow table or list of dicts matching the chunks schema.
        """
        if isinstance(records, list) and not records:
            return
        if isinstance(records, pa.Table) and records.num_rows == 0:
            return

        table = self._db.open_table(CHUNKS_TABLE)
        table.add(records)

    def upsert(self, records: pa.Table | list[dict]) -> None:
        """Upsert records into the chunks table.

        Uses merge_insert on chunk_id to handle both new and
        re-embedded chunks.  Slower than add() due to dedup scan.

        Args:
            records: PyArrow table or list of dicts matching the chunks schema.
        """
        if isinstance(records, list) and not records:
            return
        if isinstance(records, pa.Table) and records.num_rows == 0:
            return

        table = self._db.open_table(CHUNKS_TABLE)
        table.merge_insert(
            "chunk_id"
        ).when_matched_update_all().when_not_matched_insert_all().execute(records)

    def search(
        self,
        query_vector: np.ndarray,
        limit: int = 20,
        *,
        guild_id: int | None = None,
        channel_id: int | None = None,
        author_id: int | None = None,
        after: datetime | None = None,
        before: datetime | None = None,
    ) -> list[SearchResult]:
        """Search for similar chunks by vector.

        Args:
            query_vector: Query embedding, shape (1, 4096) or (4096,).
            limit: Maximum number of results to return.
            guild_id: Filter by guild.
            channel_id: Filter by channel.
            author_id: Filter by author (array_contains on author_ids).
            after: Filter chunks with first_message_at >= this time.
            before: Filter chunks with last_message_at <= this time.

        Returns:
            List of SearchResult ordered by distance (ascending).
        """
        if self._db is None:
            raise RuntimeError("Store not connected. Call connect() first.")

        vec = query_vector.flatten().tolist()
        table = self._db.open_table(CHUNKS_TABLE)
        query = table.search(vec)

        filters = []
        if guild_id is not None:
            filters.append(f"guild_id = {guild_id}")
        if channel_id is not None:
            filters.append(f"channel_id = {channel_id}")
        if author_id is not None:
            filters.append(f"array_contains(author_ids, {author_id})")
        if after is not None:
            filters.append(f"first_message_at >= timestamp '{after.isoformat()}'")
        if before is not None:
            filters.append(f"last_message_at <= timestamp '{before.isoformat()}'")

        if filters:
            query = query.where(" AND ".join(filters))

        rows = query.limit(limit).to_list()

        results = []
        for row in rows:
            results.append(SearchResult(
                chunk_id=row["chunk_id"],
                distance=row["_distance"],
                guild_id=row["guild_id"],
                channel_id=row["channel_id"],
                author_ids=row["author_ids"],
                first_message_at=row.get("first_message_at"),
                last_message_at=row.get("last_message_at"),
            ))
        return results
