from __future__ import annotations

from typing import List, Dict, Optional, Tuple
from .base_data_manager import BaseDataManager


class ProxyDataManager(BaseDataManager):
    """
    SQLite-based data manager for Proxy persistent storage.
    
    Manages broker groups, group leaders, broker configurations,
    and proxy state across restarts.
    """

    def _ensure_schema(self) -> None:
        """Initialize the database schema."""
        with self._cursor() as cur:
            # Broker groups table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS broker_groups (
                    group_name TEXT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Brokers table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS brokers (
                    broker_name TEXT PRIMARY KEY,
                    broker_id INTEGER NOT NULL,
                    group_name TEXT NOT NULL,
                    address TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    is_leader BOOLEAN NOT NULL DEFAULT 0,
                    url TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(group_name) REFERENCES broker_groups(group_name) ON DELETE CASCADE
                )
            """)
            
            # Group leaders table (for faster lookup)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS group_leaders (
                    group_name TEXT PRIMARY KEY,
                    leader_broker_name TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(group_name) REFERENCES broker_groups(group_name) ON DELETE CASCADE,
                    FOREIGN KEY(leader_broker_name) REFERENCES brokers(broker_name) ON DELETE CASCADE
                )
            """)
            
            # Proxy configuration table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS proxy_config (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)



    # Broker group operations
    def create_broker_group(self, group_name: str) -> None:
        """Create a new broker group."""
        self._execute(
            "INSERT OR IGNORE INTO broker_groups (group_name) VALUES (?)",
            (group_name,)
        )

    def delete_broker_group(self, group_name: str) -> None:
        """Delete a broker group and all its brokers."""
        self._execute("DELETE FROM broker_groups WHERE group_name = ?", (group_name,))

    def get_all_broker_groups(self) -> List[str]:
        """Get all broker group names."""
        rows = self._query_all("SELECT group_name FROM broker_groups ORDER BY group_name")
        return [row[0] for row in rows]

    def broker_group_exists(self, group_name: str) -> bool:
        """Check if a broker group exists."""
        return self._query_one(
            "SELECT 1 FROM broker_groups WHERE group_name = ?", (group_name,)
        ) is not None

    # Broker operations
    def add_broker(self, broker_name: str, broker_id: int, group_name: str, 
                   address: str, port: int, is_leader: bool, url: str) -> None:
        """Add a broker to a group."""
        self._execute("""
            INSERT OR REPLACE INTO brokers 
            (broker_name, broker_id, group_name, address, port, is_leader, url)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (broker_name, broker_id, group_name, address, port, is_leader, url))

    def remove_broker(self, broker_name: str) -> None:
        """Remove a broker."""
        self._execute("DELETE FROM brokers WHERE broker_name = ?", (broker_name,))

    def get_brokers_in_group(self, group_name: str) -> List[Dict[str, any]]:
        """Get all brokers in a group."""
        rows = self._query_all("""
            SELECT broker_name, broker_id, address, port, is_leader, url
            FROM brokers WHERE group_name = ? ORDER BY broker_name
        """, (group_name,))
        
        return [
            {
                "broker_name": row[0],
                "broker_id": row[1],
                "address": row[2],
                "port": row[3],
                "is_leader": bool(row[4]),
                "url": row[5]
            }
            for row in rows
        ]

    def get_all_brokers(self) -> Dict[str, List[str]]:
        """Get all brokers organized by group."""
        rows = self._query_all("""
            SELECT group_name, broker_name FROM brokers ORDER BY group_name, broker_name
        """)
        
        result: Dict[str, List[str]] = {}
        for group_name, broker_name in rows:
            if group_name not in result:
                result[group_name] = []
            result[group_name].append(broker_name)
        return result

    def get_broker_urls(self) -> Dict[str, str]:
        """Get all broker name to URL mappings."""
        rows = self._query_all("SELECT broker_name, url FROM brokers")
        return {row[0]: row[1] for row in rows}

    def broker_exists(self, broker_name: str) -> bool:
        """Check if a broker exists."""
        return self._query_one(
            "SELECT 1 FROM brokers WHERE broker_name = ?", (broker_name,)
        ) is not None

    def get_broker_info(self, broker_name: str) -> Optional[Dict[str, any]]:
        """Get broker information by name."""
        row = self._query_one("""
            SELECT broker_id, group_name, address, port, is_leader, url
            FROM brokers WHERE broker_name = ?
        """, (broker_name,))
        
        if row is None:
            return None
        
        return {
            "broker_id": row[0],
            "group_name": row[1],
            "address": row[2],
            "port": row[3],
            "is_leader": bool(row[4]),
            "url": row[5]
        }

    # Group leader operations
    def set_group_leader(self, group_name: str, leader_broker_name: str) -> None:
        """Set the leader for a group."""
        # First, unset any existing leader in this group
        self._execute("""
            UPDATE brokers SET is_leader = 0 WHERE group_name = ?
        """, (group_name,))
        
        # Set the new leader
        self._execute("""
            UPDATE brokers SET is_leader = 1 WHERE broker_name = ?
        """, (leader_broker_name,))
        
        # Update or insert group leader record
        self._execute("""
            INSERT OR REPLACE INTO group_leaders (group_name, leader_broker_name, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        """, (group_name, leader_broker_name))

    def get_group_leader(self, group_name: str) -> Optional[str]:
        """Get the leader broker name for a group."""
        row = self._query_one("""
            SELECT leader_broker_name FROM group_leaders WHERE group_name = ?
        """, (group_name,))
        return row[0] if row else None

    def get_all_group_leaders(self) -> Dict[str, str]:
        """Get all group leaders."""
        rows = self._query_all("""
            SELECT group_name, leader_broker_name FROM group_leaders
        """)
        return {row[0]: row[1] for row in rows}

    def clear_group_leader(self, group_name: str) -> None:
        """Clear the leader for a group."""
        self._execute("""
            UPDATE brokers SET is_leader = 0 WHERE group_name = ?
        """, (group_name,))
        self._execute("""
            DELETE FROM group_leaders WHERE group_name = ?
        """, (group_name,))

    # Proxy configuration operations
    def set_config(self, key: str, value: str) -> None:
        """Set a configuration value."""
        self._execute("""
            INSERT OR REPLACE INTO proxy_config (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        """, (key, value))

    def get_config(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Get a configuration value."""
        row = self._query_one("SELECT value FROM proxy_config WHERE key = ?", (key,))
        return row[0] if row else default

    def get_all_config(self) -> Dict[str, str]:
        """Get all configuration values."""
        rows = self._query_all("SELECT key, value FROM proxy_config")
        return {row[0]: row[1] for row in rows}

    def delete_config(self, key: str) -> None:
        """Delete a configuration value."""
        self._execute("DELETE FROM proxy_config WHERE key = ?", (key,))

    # Utility methods
    def get_next_broker_id(self) -> int:
        """Get the next available broker ID."""
        row = self._query_one("SELECT COALESCE(MAX(broker_id), 0) + 1 FROM brokers")
        return row[0] if row else 1

    def clear_all_data(self) -> None:
        """Clear all data (for testing)."""
        with self._cursor() as cur:
            cur.execute("DELETE FROM group_leaders")
            cur.execute("DELETE FROM brokers")
            cur.execute("DELETE FROM broker_groups")
            cur.execute("DELETE FROM proxy_config")
