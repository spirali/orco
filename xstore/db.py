import sqlite3


class DB:

    def __init__(self, path):
        self.conn = sqlite3.connect(path)
        self.init()

    def init(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS collections (
                name STRING NOT NULL PRIMARY KEY,
                serialized_config BLOB NOT NULL
            );
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS entries (
                collection STRING NOT NULL,
                config STRING NOT NULL,
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
                config1 STRING NOT NULL,
                collection2 STRING NOT NULL,
                config2 STRING NOT NULL,

                UNIQUE(collection1, config1, collection2, config2),

                CONSTRAINT entry1_ref
                    FOREIGN KEY (collection1, config1)
                    REFERENCES entries(collection, config)
                    ON DELETE CASCADE,
                CONSTRAINT entry2_ref
                    FOREIGN KEY (collection2, config2)
                    REFERENCES entries(collection, config)
                    ON DELETE CASCADE
            );
        """)

    def new_collection(self, name, serialized_config):
        c = self.conn.cursor()
        c.execute("INSERT INTO collections VALUES (?, ?)", [name, serialized_config])
        self.conn.commit()