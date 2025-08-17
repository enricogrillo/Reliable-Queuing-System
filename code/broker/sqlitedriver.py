import sqlite3

class SQLiteManager:
    def __init__(self, db_name='test.db'):
        self.connection = sqlite3.connect(db_name)
        self.cursor = self.connection.cursor()
        self.create_table()

    def create_table(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                age INTEGER NOT NULL
            )
        ''')
        self.connection.commit()

    def add_record(self, name, age):
        self.cursor.execute('''
            INSERT INTO records (name, age) VALUES (?, ?)
        ''', (name, age))
        self.connection.commit()

    def get_records(self):
        self.cursor.execute('SELECT * FROM records')
        return self.cursor.fetchall()

    def delete_record(self, record_id):
        self.cursor.execute('DELETE FROM records WHERE id = ?', (record_id,))
        self.connection.commit()

    def close(self):
        self.connection.close()


if __name__ == "__main__":
    db_manager = SQLiteManager()

    # Adding records
    db_manager.add_record("Alice", 30)
    db_manager.add_record("Bob", 25)

    # Retrieving records
    records = db_manager.get_records()
    print("Records in the database:")
    for record in records:
        print(record)

    # Deleting a record
    db_manager.delete_record(1)  # Deletes the record with id 1

    # Closing the database connection
    db_manager.close()