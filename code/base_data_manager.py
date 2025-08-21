from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterator, Optional, Tuple, List
from abc import ABC, abstractmethod


class BaseDataManager(ABC):
    """
    Base class for SQLite-based data managers.
    
    Provides common database operations, connection management,
    and thread-safe query execution methods.
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
        """Close the database connection."""
        with self._lock:
            self._conn.close()

    @contextmanager
    def _cursor(self) -> Iterator[sqlite3.Cursor]:
        """Context manager for database operations with automatic transaction handling."""
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

    @abstractmethod
    def _ensure_schema(self) -> None:
        """Ensure the database schema is created. Must be implemented by subclasses."""
        pass

    # Internal convenience helpers
    def _execute(self, sql: str, params: Tuple = ()) -> None:
        """Execute a SQL statement without returning results."""
        with self._cursor() as cur:
            cur.execute(sql, params)

    def _query_one(self, sql: str, params: Tuple = ()) -> Optional[Tuple]:
        """Execute a query and return one result."""
        with self._cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()

    def _query_all(self, sql: str, params: Tuple = ()) -> List[Tuple]:
        """Execute a query and return all results."""
        with self._cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def _insert_and_return_id(self, sql: str, params: Tuple = ()) -> int:
        """Execute an INSERT statement and return the last row ID."""
        with self._cursor() as cur:
            cur.execute(sql, params)
            return int(cur.lastrowid)
