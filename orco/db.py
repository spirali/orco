import sqlite3
import pickle
from concurrent.futures import ThreadPoolExecutor


from .entry import Entry


class DB:

    def __init__(self, path):
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.executor.submit(self.init, path).result()
        assert self.conn is not None

    def init(self, path):
        self.conn = sqlite3.connect(path)
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
        def _helper():
            c = self.conn.cursor()
            c.execute("INSERT OR IGNORE INTO collections VALUES (?)", [name])
            self.conn.commit()
        self.executor.submit(_helper).result()

    def create_entry(self, entry):
        def _helper():
            collection = entry.collection
            c = self.conn.cursor()
            c.execute("INSERT INTO entries VALUES (?, ?, ?, ?, ?)",
                    [collection.name,
                    collection.make_key(entry.config),
                    pickle.dumps(entry.config),
                    pickle.dumps(entry.value),
                    entry.created])
            self.conn.commit()
        self.executor.submit(_helper).result()

    def get_entry_by_config(self, collection, config):
        key = collection.make_key(config)
        def _helper():
            c = self.conn.cursor()
            c.execute("SELECT value, created FROM entries WHERE collection = ? AND key = ?",
                    [collection.name, key])
            return c.fetchone()
        result = self.executor.submit(_helper).result()
        if result is None:
            return None
        return Entry(collection, config, pickle.loads(result[0]), result[1])

    def has_entry_by_key(self, collection, key):
        def _helper():
            c = self.conn.cursor()
            c.execute("SELECT COUNT(*) FROM entries WHERE collection = ? AND key = ?",
                      [collection.name, key])
            return bool(c.fetchone()[0])
        return self.executor.submit(_helper).result()

    def get_entry_by_key(self, collection, key):
        def _helper():
            c = self.conn.cursor()
            c.execute("SELECT config, value, created FROM entries WHERE collection = ? AND key = ?",
                    [collection.name, key])
            return c.fetchone()
        result = self.executor.submit(_helper).result()
        if result is None:
            return None
        config, value, created = result
        return Entry(collection, pickle.loads(config), pickle.loads(value), created)

    def remove_entry_by_key(self, collection, key):
        def _helper():
            self.conn.execute("DELETE FROM entries WHERE collection = ? AND key = ?",
                [collection.name, key])
        self.executor.submit(_helper).result()

    def remove_entries(self, collection_key_pairs):
        def _helper():
            self.conn.executemany("DELETE FROM entries WHERE collection = ? AND key = ?", collection_key_pairs)
        self.executor.submit(_helper).result()

    def collection_summaries(self):
        def _helper():
            c = self.conn.cursor()
            r = c.execute("SELECT collection, COUNT(key), SUM(length(value)), SUM(length(config)) FROM entries GROUP BY collection ORDER BY collection")
            result = []
            found = set()
            for name, count, size_value, size_config in r.fetchall():
                found.add(name)
                result.append({"name": name, "count": count, "size": size_value + size_config})

            c.execute("SELECT name FROM collections")
            for x in r.fetchall():
                name = x[0]
                if name in found:
                    continue
                result.append({"name": name, "count": 0, "size": 0})

            result.sort(key=lambda x: x["name"])
            return result
        return self.executor.submit(_helper).result()


    def entry_summaries(self, collection):
        def _helper():
            c = self.conn.cursor()
            r = c.execute("SELECT key, config, length(value) FROM entries WHERE collection = ?", [collection.name])
            return [
                {"key": key, "config": pickle.loads(config), "size": value_size + len(config)}
                for key, config, value_size in r.fetchall()
            ]
        return self.executor.submit(_helper).result()