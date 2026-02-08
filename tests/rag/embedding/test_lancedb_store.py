"""Tests for discord_archive.rag.embedding.lancedb_store."""

from __future__ import annotations

import tempfile
from datetime import datetime, timezone
from pathlib import Path

from discord_archive.rag.embedding.lancedb_store import (
    CHUNKS_TABLE,
    LanceDBStore,
)


def _make_record(
    chunk_id: int = 1,
    guild_id: int = 100,
    channel_id: int = 200,
) -> dict:
    """Create a test record matching the LanceDB chunks schema."""
    return {
        "chunk_id": chunk_id,
        "vector": [0.1] * 4096,
        "guild_id": guild_id,
        "channel_id": channel_id,
        "author_ids": [1, 2],
        "mentioned_user_ids": [3],
        "mentioned_role_ids": [],
        "has_attachments": False,
        "first_message_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "last_message_at": datetime(2024, 1, 2, tzinfo=timezone.utc),
    }


class TestLanceDBStoreConnect:
    """Tests for LanceDBStore.connect()."""

    def test_connect_creates_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "lancedb_test"
            store = LanceDBStore(data_dir)
            store.connect()
            assert data_dir.exists()

    def test_connect_creates_table(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LanceDBStore(tmpdir)
            store.connect()
            assert CHUNKS_TABLE in store._db.list_tables().tables


class TestLanceDBStoreUpsert:
    """Tests for LanceDBStore.upsert()."""

    def test_upsert_creates_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LanceDBStore(tmpdir)
            store.connect()

            record = _make_record(chunk_id=1)
            store.upsert([record])

            table = store._db.open_table(CHUNKS_TABLE)
            arrow_table = table.to_arrow()
            assert arrow_table.num_rows == 1
            assert arrow_table.column("chunk_id")[0].as_py() == 1

    def test_upsert_and_read_back(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LanceDBStore(tmpdir)
            store.connect()

            records = [_make_record(chunk_id=i) for i in range(1, 4)]
            store.upsert(records)

            table = store._db.open_table(CHUNKS_TABLE)
            arrow_table = table.to_arrow()
            assert arrow_table.num_rows == 3
            chunk_ids = {v.as_py() for v in arrow_table.column("chunk_id")}
            assert chunk_ids == {1, 2, 3}

    def test_upsert_overwrites_existing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LanceDBStore(tmpdir)
            store.connect()

            # Insert initial record
            record = _make_record(chunk_id=1, guild_id=100)
            store.upsert([record])

            # Upsert with updated guild_id
            updated = _make_record(chunk_id=1, guild_id=999)
            store.upsert([updated])

            table = store._db.open_table(CHUNKS_TABLE)
            arrow_table = table.to_arrow()
            assert arrow_table.num_rows == 1
            assert arrow_table.column("guild_id")[0].as_py() == 999

    def test_upsert_empty_list(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LanceDBStore(tmpdir)
            store.connect()
            store.upsert([])  # Should not raise

            table = store._db.open_table(CHUNKS_TABLE)
            arrow_table = table.to_arrow()
            assert arrow_table.num_rows == 0


class TestLanceDBStoreAdd:
    """Tests for LanceDBStore.add()."""

    def test_add_creates_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LanceDBStore(tmpdir)
            store.connect()

            records = [_make_record(chunk_id=i) for i in range(1, 4)]
            store.add(records)

            table = store._db.open_table(CHUNKS_TABLE)
            arrow_table = table.to_arrow()
            assert arrow_table.num_rows == 3
            chunk_ids = {v.as_py() for v in arrow_table.column("chunk_id")}
            assert chunk_ids == {1, 2, 3}

    def test_add_appends_without_dedup(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LanceDBStore(tmpdir)
            store.connect()

            record = _make_record(chunk_id=1)
            store.add([record])
            store.add([record])  # Same chunk_id — should duplicate

            table = store._db.open_table(CHUNKS_TABLE)
            arrow_table = table.to_arrow()
            assert arrow_table.num_rows == 2

    def test_add_empty_list(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LanceDBStore(tmpdir)
            store.connect()
            store.add([])  # Should not raise

            table = store._db.open_table(CHUNKS_TABLE)
            arrow_table = table.to_arrow()
            assert arrow_table.num_rows == 0
