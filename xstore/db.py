import sqlite3
import pickle

from .entry import Entry

class DB:

    def __init__(self, path):
        self.conn = sqlite3.connect(path)
        self.init()

    def init(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS collections (
                name TEXT NOT NULL PRIMARY KEY
            );
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS entries (
                collection STRING NOT NULL,
                key TEXT NOT NULL,
                config BLOB NOT NULL,
                value BLOB NOT NULL,
                created TEXT NOT NULL,

                PRIMARY KEY (collection, key)
                CONSTRAINT collection_ref
                    FOREIGN KEY (collection)
                    REFERENCES collections(name)
                    ON DELETE CASCADE
            );
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS deps (
                collection1 STRING NOT NULL,
                key1 STRING NOT NULL,
                collection2 STRING NOT NULL,
                key2 STRING NOT NULL,

                UNIQUE(collection1, key1, collection2, key2),

                CONSTRAINT entry1_ref
                    FOREIGN KEY (collection1, key1)
                    REFERENCES entries(collection, key)
                    ON DELETE CASCADE,
                CONSTRAINT entry2_ref
                    FOREIGN KEY (collection2, key2)
                    REFERENCES entries(collection, key)
                    ON DELETE CASCADE
            );
        """)

    def ensure_collection(self, name):
        c = self.conn.cursor()
        c.execute("INSERT OR IGNORE INTO collections VALUES (?)", [name])
        self.conn.commit()

    def create_entry(self, entry):
        collection = entry.collection
        c = self.conn.cursor()
        c.execute("INSERT INTO entries VALUES (?, ?, ?, ?, ?)",
                  [collection.name,
                   collection.make_key(entry.config),
                   pickle.dumps(entry.config),
                   pickle.dumps(entry.value),
                   entry.created])
        self.conn.commit()

    def get_entry_by_config(self, collection, config):
        key = collection.make_key(config)
        c = self.conn.cursor()
        c.execute("SELECT value, created FROM entries WHERE collection = ? AND key = ?",
                  [collection.name, key])
        result = c.fetchone()
        if result is None:
            return None
        return Entry(collection, config, pickle.loads(result[0]), result[1])

    def get_entry_by_key(self, collection, key):
        c = self.conn.cursor()
        c.execute("SELECT config, value, created FROM entries WHERE collection = ? AND key = ?",
                  [collection.name, key])
        result = c.fetchone()
        if result is None:
            return None
        config, value, created = result
        return Entry(collection, pickle.loads(config), pickle.loads(value), created)