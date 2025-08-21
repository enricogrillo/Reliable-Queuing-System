from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterator, Optional, Tuple


class SqliteManager:
    """
    Simple SQLite-backed queue storage.

    Schema:
      - queues(id INTEGER PRIMARY KEY)
      - messages(id INTEGER PRIMARY KEY, queue_id INTEGER NOT NULL, value TEXT NOT NULL,
                 FOREIGN KEY(queue_id) REFERENCES queues(id))
      - read_pointers(queue_id INTEGER NOT NULL, client_id TEXT NOT NULL,
                      last_read_message_id INTEGER NOT NULL DEFAULT 0,
                      PRIMARY KEY(queue_id, client_id),
                      FOREIGN KEY(queue_id) REFERENCES queues(id) ON DELETE CASCADE)

    Methods provide both auto-id inserts and explicit-id variants to support
    deterministic replication across broker nodes.
    """

    def __init__(self, db_path: str) -> None:
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self._db_path = db_path
        # check_same_thread=False allows connection usage across threads if externally synchronized
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._lock = threading.RLock()
        self._ensure_schema()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def _cursor(self) -> Iterator[sqlite3.Cursor]:
        with self._lock:
            cur = self._conn.cursor()
            try:
                yield cur
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
            finally:
                cur.close()

    def _ensure_schema(self) -> None:
        with self._cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS queues (
                    id INTEGER PRIMARY KEY
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY,
                    queue_id INTEGER NOT NULL,
                    value TEXT NOT NULL,
                    FOREIGN KEY(queue_id) REFERENCES queues(id) ON DELETE CASCADE
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS read_pointers (
                    queue_id INTEGER NOT NULL,
                    client_id TEXT NOT NULL,
                    last_read_message_id INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(queue_id, client_id),
                    FOREIGN KEY(queue_id) REFERENCES queues(id) ON DELETE CASCADE
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_messages_queue_id_id
                ON messages(queue_id, id);
                """
            )

    # Internal convenience helpers
    def _execute(self, sql: str, params: Tuple = ()) -> None:
        with self._cursor() as cur:
            cur.execute(sql, params)

    def _query_one(self, sql: str, params: Tuple = ()) -> Optional[Tuple]:
        with self._cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()

    def _insert_and_return_id(self, sql: str, params: Tuple = ()) -> int:
        with self._cursor() as cur:
            cur.execute(sql, params)
            return int(cur.lastrowid)

    def _require_queue_exists(self, queue_id: int) -> None:
        row = self._query_one("SELECT 1 FROM queues WHERE id=?", (queue_id,))
        if row is None:
            raise ValueError(f"Queue {queue_id} does not exist")

    # Queue operations
    def create_queue(self) -> int:
        """Create a queue with auto-assigned id; returns the id."""
        return self._insert_and_return_id("INSERT INTO queues DEFAULT VALUES")

    def create_queue_with_id(self, queue_id: int) -> None:
        """Create a queue with a specified id (used during replication)."""
        self._execute("INSERT INTO queues(id) VALUES (?)", (queue_id,))

    def delete_queue(self, queue_id: int) -> None:
        self._execute("DELETE FROM queues WHERE id=?", (queue_id,))

    def queue_exists(self, queue_id: int) -> bool:
        return self._query_one("SELECT 1 FROM queues WHERE id=?", (queue_id,)) is not None

    # Message operations
    def append_message(self, queue_id: int, value: str) -> int:
        # Validate queue exists to surface clear errors
        self._require_queue_exists(queue_id)
        return self._insert_and_return_id(
            "INSERT INTO messages(queue_id, value) VALUES (?, ?)", (queue_id, value)
        )

    def append_message_with_id(self, message_id: int, queue_id: int, value: str) -> None:
        self._require_queue_exists(queue_id)
        self._execute(
            "INSERT INTO messages(id, queue_id, value) VALUES (?, ?, ?)",
            (message_id, queue_id, value),
        )

    def fetch_next_message(self, queue_id: int) -> Optional[Tuple[int, str]]:
        """Return (message_id, value) of the next message without removing it."""
        row = self._query_one(
            "SELECT id, value FROM messages WHERE queue_id=? ORDER BY id ASC LIMIT 1",
            (queue_id,),
        )
        if row is None:
            return None
        message_id, value = int(row[0]), str(row[1])
        return message_id, value

    def delete_message_by_id(self, queue_id: int, message_id: int) -> None:
        self._execute(
            "DELETE FROM messages WHERE queue_id=? AND id=?",
            (queue_id, message_id),
        )

    # Read pointer operations (per client per queue)
    def get_last_read_message_id(self, queue_id: int, client_id: str) -> int:
        row = self._query_one(
            "SELECT last_read_message_id FROM read_pointers WHERE queue_id=? AND client_id=?",
            (queue_id, client_id),
        )
        return int(row[0]) if row is not None else 0

    def set_last_read_message_id(self, queue_id: int, client_id: str, message_id: int) -> None:
        self._execute(
            """
            INSERT INTO read_pointers(queue_id, client_id, last_read_message_id)
            VALUES (?, ?, ?)
            ON CONFLICT(queue_id, client_id) DO UPDATE SET last_read_message_id=excluded.last_read_message_id
            """,
            (queue_id, client_id, message_id),
        )

    def fetch_next_after(self, queue_id: int, after_message_id: int) -> Optional[Tuple[int, str]]:
        row = self._query_one(
            "SELECT id, value FROM messages WHERE queue_id=? AND id>? ORDER BY id ASC LIMIT 1",
            (queue_id, after_message_id),
        )
        if row is None:
            return None
        return int(row[0]), str(row[1])


