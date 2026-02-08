"""LanceDB vector store for chunk embeddings.

Manages connection, table creation, and upsert operations
for storing chunk embeddings in LanceDB.
"""

from __future__ import annotations

from pathlib import Path

import lancedb
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
