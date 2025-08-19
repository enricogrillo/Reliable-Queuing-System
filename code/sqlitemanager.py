import sqlite3
import uuid

class SqliteManager:
    def __init__(self, db_name='broker.db'):
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self._create_tables()

    def _create_tables(self):
        c = self.conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS broker (
                broker_id INTEGER PRIMARY KEY,
                ip_address TEXT,
                port INTEGER,
                group INTEGER
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS queue (
                queue_id TEXT PRIMARY KEY
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS holds (
                broker_id TEXT,
                queue_id TEXT,
                PRIMARY KEY (broker_id, queue_id),
                FOREIGN KEY (broker_id) REFERENCES broker(broker_id),
                FOREIGN KEY (queue_id) REFERENCES queue(queue_id)
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS queue_value (
                queue_id TEXT,
                value INTEGER,
                index INTEGER,
                PRIMARY KEY (queue_id, value, index),
                FOREIGN KEY (queue_id) REFERENCES queue(queue_id)
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS client (
                client_id TEXT PRIMARY KEY
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS reads (
                client_id TEXT,
                queue_id TEXT,
                index INTEGER,
                PRIMARY KEY (client_id, queue_id),
                FOREIGN KEY (client_id) REFERENCES client(client_id),
                FOREIGN KEY (queue_id) REFERENCES queue(queue_id)
            )
        ''')
        self.conn.commit()

    def register_client(self, client_id):
        c = self.conn.cursor()
        c.execute('SELECT client_key FROM clients WHERE client_id=?', (client_id,))
        row = c.fetchone()
        if row:
            return row[0]
        client_key = str(uuid.uuid4())
        c.execute('INSERT INTO clients (client_id, client_key) VALUES (?, ?)', (client_id, client_key))
        self.conn.commit()
        return client_key

    def authenticate(self, client_id, client_key):
        c = self.conn.cursor()
        c.execute('SELECT client_key FROM clients WHERE client_id=?', (client_id,))
        row = c.fetchone()
        return row and row[0] == client_key

    def create_queue(self, client_id):
        queue_id = str(uuid.uuid4())
        c = self.conn.cursor()
        c.execute('INSERT INTO queues (queue_id, client_id) VALUES (?, ?)', (queue_id, client_id))
        self.conn.commit()
        return queue_id

    def append_value(self, queue_id, val):
        c = self.conn.cursor()
        c.execute('INSERT INTO queue_values (queue_id, value) VALUES (?, ?)', (queue_id, val))
        self.conn.commit()
        return True

    def read_queue(self, queue_id):
        c = self.conn.cursor()
        c.execute('SELECT rowid, value FROM queue_values WHERE queue_id=? ORDER BY rowid LIMIT 1', (queue_id,))
        row = c.fetchone()
        if not row:
            return None
        rowid, value = row
        c.execute('DELETE FROM queue_values WHERE rowid=?', (rowid,))
        self.conn.commit()
        return value

    def list_queues(self, client_id):
        c = self.conn.cursor()
        c.execute('SELECT queue_id FROM queues WHERE client_id=?', (client_id,))
        return [row[0] for row in c.fetchall()]
