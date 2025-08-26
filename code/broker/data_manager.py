import sqlite3
import threading
import time
import os
from typing import List, Tuple, Optional, Dict, Any


class BrokerDataManager:
    """Handles all persistent data operations using SQLite storage."""
    
    def __init__(self, db_path: str):
        """Initialize the data manager with SQLite database."""
        # Ensure data directory exists
        data_dir = "data"
        os.makedirs(data_dir, exist_ok=True)
        
        # Put database in data directory
        db_filename = os.path.basename(db_path)
        self.db_path = os.path.join(data_dir, db_filename)
        
        self.db_connection = None
        self.transaction_lock = threading.Lock()
        self._initialize_database()
    
    def _initialize_database(self):
        """Create database connection and initialize schema."""
        self.db_connection = sqlite3.connect(self.db_path, check_same_thread=False)
        self.db_connection.execute("PRAGMA journal_mode=WAL")  # Better concurrency
        self._create_tables()
    
    def _create_tables(self):
        """Create the required database tables."""
        with self.transaction_lock:
            cursor = self.db_connection.cursor()
            
            # Queues table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS queues (
                    queue_id TEXT PRIMARY KEY
                )
            """)
            
            # Queue data table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS queue_data (
                    queue_id TEXT,
                    sequence_num INTEGER,
                    data INTEGER,
                    PRIMARY KEY (queue_id, sequence_num)
                )
            """)
            
            # Client positions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS client_positions (
                    client_id TEXT,
                    queue_id TEXT,
                    read_position INTEGER DEFAULT 0,
                    PRIMARY KEY (client_id, queue_id)
                )
            """)
            

            
            self.db_connection.commit()
    
    # Queue Operations
    def create_queue(self, queue_id: str) -> bool:
        """Create new queue metadata."""
        with self.transaction_lock:
            try:
                cursor = self.db_connection.cursor()
                cursor.execute("INSERT INTO queues (queue_id) VALUES (?)", (queue_id,))
                self.db_connection.commit()
                return True
            except sqlite3.IntegrityError:
                # Queue already exists
                return False
    
    def queue_exists(self, queue_id: str) -> bool:
        """Check if queue exists."""
        cursor = self.db_connection.cursor()
        cursor.execute("SELECT 1 FROM queues WHERE queue_id = ?", (queue_id,))
        return cursor.fetchone() is not None
    
    def get_all_queues(self) -> List[str]:
        """Return list of all queues."""
        cursor = self.db_connection.cursor()
        cursor.execute("SELECT queue_id FROM queues")
        return [row[0] for row in cursor.fetchall()]
    
    # Message Operations
    def append_message(self, queue_id: str, data: int) -> int:
        """Add message with next sequence number. Returns sequence number."""
        with self.transaction_lock:
            cursor = self.db_connection.cursor()
            
            # Get next sequence number
            cursor.execute(
                "SELECT COALESCE(MAX(sequence_num), 0) + 1 FROM queue_data WHERE queue_id = ?",
                (queue_id,)
            )
            next_sequence = cursor.fetchone()[0]
            
            # Insert message
            cursor.execute(
                "INSERT INTO queue_data (queue_id, sequence_num, data) VALUES (?, ?, ?)",
                (queue_id, next_sequence, data)
            )
            
            self.db_connection.commit()
            return next_sequence
    
    def get_next_message(self, queue_id: str, after_sequence: int) -> Optional[Tuple[int, int]]:
        """Retrieve next message in FIFO order. Returns (sequence_num, data) or None."""
        cursor = self.db_connection.cursor()
        cursor.execute(
            """SELECT sequence_num, data FROM queue_data 
               WHERE queue_id = ? AND sequence_num > ? 
               ORDER BY sequence_num LIMIT 1""",
            (queue_id, after_sequence)
        )
        result = cursor.fetchone()
        return result if result else None
    
    def get_message_count(self, queue_id: str) -> int:
        """Return total messages in queue."""
        cursor = self.db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM queue_data WHERE queue_id = ?", (queue_id,))
        return cursor.fetchone()[0]
    
    # Client Position Operations
    def get_client_position(self, client_id: str, queue_id: str) -> int:
        """Get current read position for client."""
        cursor = self.db_connection.cursor()
        cursor.execute(
            "SELECT read_position FROM client_positions WHERE client_id = ? AND queue_id = ?",
            (client_id, queue_id)
        )
        result = cursor.fetchone()
        return result[0] if result else 0
    
    def update_client_position(self, client_id: str, queue_id: str, position: int):
        """Update read position for client."""
        with self.transaction_lock:
            cursor = self.db_connection.cursor()
            cursor.execute(
                """INSERT OR REPLACE INTO client_positions (client_id, queue_id, read_position)
                   VALUES (?, ?, ?)""",
                (client_id, queue_id, position)
            )
            self.db_connection.commit()
    
    def get_all_client_positions(self) -> List[Tuple[str, str, int]]:
        """Return all client positions as (client_id, queue_id, position)."""
        cursor = self.db_connection.cursor()
        cursor.execute("SELECT client_id, queue_id, read_position FROM client_positions")
        return cursor.fetchall()
    
    # Recovery Operations
    def restore_broker_state(self) -> Dict[str, Any]:
        """Load all data on broker startup."""
        state = {
            'queues': self.get_all_queues(),
            'client_positions': dict(
                ((client_id, queue_id), position) 
                for client_id, queue_id, position in self.get_all_client_positions()
            ),
            'queue_stats': {}
        }
        
        # Get statistics for each queue
        for queue_id in state['queues']:
            state['queue_stats'][queue_id] = {
                'message_count': self.get_message_count(queue_id),
                'latest_sequence': self.get_latest_sequence(queue_id)
            }
        
        return state
    
    def get_latest_sequence(self, queue_id: str) -> int:
        """Get highest sequence number for queue."""
        cursor = self.db_connection.cursor()
        cursor.execute(
            "SELECT COALESCE(MAX(sequence_num), 0) FROM queue_data WHERE queue_id = ?",
            (queue_id,)
        )
        return cursor.fetchone()[0]
    
    def cleanup_old_data(self, queue_id: str, before_sequence: int):
        """Remove obsolete data (optional optimization)."""
        with self.transaction_lock:
            cursor = self.db_connection.cursor()
            cursor.execute(
                "DELETE FROM queue_data WHERE queue_id = ? AND sequence_num < ?",
                (queue_id, before_sequence)
            )
            self.db_connection.commit()
    
    # Transaction Management
    def begin_transaction(self):
        """Begin a database transaction."""
        self.transaction_lock.acquire()
        self.db_connection.execute("BEGIN")
    
    def commit_transaction(self):
        """Commit the current transaction."""
        try:
            self.db_connection.commit()
        finally:
            self.transaction_lock.release()
    
    def rollback_transaction(self):
        """Rollback the current transaction."""
        try:
            self.db_connection.rollback()
        finally:
            self.transaction_lock.release()
    
    # Data Synchronization Operations for Catch-up
    def get_full_data_snapshot(self) -> Dict[str, Any]:
        """Get complete snapshot of all data for broker catch-up."""
        snapshot = {
            'queues': [],
            'messages': [],
            'client_positions': []
        }
        
        cursor = self.db_connection.cursor()
        
        # Get all queues
        cursor.execute("SELECT queue_id FROM queues")
        snapshot['queues'] = [row[0] for row in cursor.fetchall()]
        
        # Get all messages
        cursor.execute("SELECT queue_id, sequence_num, data FROM queue_data ORDER BY queue_id, sequence_num")
        snapshot['messages'] = [{'queue_id': row[0], 'sequence_num': row[1], 'data': row[2]} for row in cursor.fetchall()]
        
        # Get all client positions
        cursor.execute("SELECT client_id, queue_id, read_position FROM client_positions")
        snapshot['client_positions'] = [{'client_id': row[0], 'queue_id': row[1], 'position': row[2]} for row in cursor.fetchall()]
        
        return snapshot
    
    def apply_data_snapshot(self, snapshot: Dict[str, Any]) -> bool:
        """Apply complete data snapshot during broker catch-up."""
        try:
            self.begin_transaction()
            
            # Clear existing data
            cursor = self.db_connection.cursor()
            cursor.execute("DELETE FROM client_positions")
            cursor.execute("DELETE FROM queue_data")
            cursor.execute("DELETE FROM queues")
            
            # Apply queues
            for queue_id in snapshot.get('queues', []):
                cursor.execute("INSERT INTO queues (queue_id) VALUES (?)", (queue_id,))
            
            # Apply messages
            for msg in snapshot.get('messages', []):
                cursor.execute(
                    "INSERT INTO queue_data (queue_id, sequence_num, data) VALUES (?, ?, ?)",
                    (msg['queue_id'], msg['sequence_num'], msg['data'])
                )
            
            # Apply client positions
            for pos in snapshot.get('client_positions', []):
                cursor.execute(
                    "INSERT INTO client_positions (client_id, queue_id, read_position) VALUES (?, ?, ?)",
                    (pos['client_id'], pos['queue_id'], pos['position'])
                )
            
            self.commit_transaction()
            print(f"Applied data snapshot: {len(snapshot.get('queues', []))} queues, "
                  f"{len(snapshot.get('messages', []))} messages, "
                  f"{len(snapshot.get('client_positions', []))} client positions")
            return True
            
        except Exception as e:
            print(f"Failed to apply data snapshot: {e}")
            self.rollback_transaction()
            return False
    
    def get_queue_messages_since(self, queue_id: str, since_sequence: int) -> List[Tuple[int, int]]:
        """Get messages from queue starting from a specific sequence number."""
        cursor = self.db_connection.cursor()
        cursor.execute(
            "SELECT sequence_num, data FROM queue_data WHERE queue_id = ? AND sequence_num > ? ORDER BY sequence_num",
            (queue_id, since_sequence)
        )
        return cursor.fetchall()
    
    def bulk_insert_messages(self, messages: List[Dict[str, Any]]) -> bool:
        """Insert multiple messages for catch-up scenarios."""
        try:
            with self.transaction_lock:
                cursor = self.db_connection.cursor()
                for msg in messages:
                    cursor.execute(
                        "INSERT OR REPLACE INTO queue_data (queue_id, sequence_num, data) VALUES (?, ?, ?)",
                        (msg['queue_id'], msg['sequence_num'], msg['data'])
                    )
                self.db_connection.commit()
                return True
        except Exception as e:
            print(f"Failed to bulk insert messages: {e}")
            return False
    

    def close(self):
        """Close database connection."""
        if self.db_connection:
            self.db_connection.close()
            self.db_connection = None
