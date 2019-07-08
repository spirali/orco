import sqlite3


class DB:

    def __init__(self, path):
        self.conn = sqlite3.connect(path)
        self.init()

    def init(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS collections (
                name TEXT NOT NULL PRIMARY KEY,
                serialized_config BLOB NOT NULL
            );
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS entries (
                collection STRING NOT NULL,
                key TEXT NOT NULL,
                value BLOB NOT NULL,
                created TEXT NOT NULL,

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

    def new_collection(self, name, serialized_config):
        c = self.conn.cursor()
        c.execute("INSERT INTO collections VALUES (?, ?)", [name, serialized_config])
        self.conn.commit()