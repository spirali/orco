import json
import logging
import math
import pickle
from concurrent.futures import ThreadPoolExecutor

import apsw
import pandas as pd

from ..entry import Entry
from ..report import Report

logger = logging.getLogger(__name__)


class DB:

    DEAD_EXECUTOR_QUERY = "((STRFTIME('%s', heartbeat) + heartbeat_interval * 2) - STRFTIME('%s', 'now') < 0)"
    LIVE_EXECUTOR_QUERY = "((STRFTIME('%s', heartbeat) + heartbeat_interval * 2) - STRFTIME('%s', 'now') >= 0)"
    RECURSIVE_CONSUMERS = """
            WITH RECURSIVE
            selected(builder, key) AS (
                VALUES(?, ?)
                UNION
                SELECT builder_t,  cast(key_t as TEXT) FROM selected, deps WHERE selected.builder == deps.builder_s AND selected.key == deps.key_s
            )"""

    def __init__(self, path, threading=True):
        logger.debug("Opening DB: %s, threading = %s", path, threading)

        def _helper():
            self.conn = apsw.Connection(path)
            self.conn.setbusytimeout(5000)  # In milliseconds

            # The following HAS TO BE OUTSIDE OF TRANSACTION!
            self.conn.cursor().execute("""
                    PRAGMA foreign_keys = ON
                """)

        self.path = path
        if threading:
            self._thread = ThreadPoolExecutor(max_workers=1)
        else:
            self._thread = None
        self._run(_helper)

    def _run(self, fn):
        thread = self._thread
        if thread:
            return thread.submit(fn).result()
        else:
            return fn()

    def init(self):

        def _helper():
            with self.conn:
                cursor = self.conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS builders (
                        name TEXT NOT NULL PRIMARY KEY
                    );
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS executors (
                        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                        heartbeat TEXT NOT NULL,
                        heartbeat_interval FLOAT NOT NULL,
                        stats TEXT,
                        created TEXT NOT NULL,
                        name STRING NOT NULL,
                        hostname STRING NOT NULL,
                        resources STRING NOT NULL
                    );
                """)

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS entries (
                        builder STRING NOT NULL,
                        key TEXT NOT NULL,
                        executor INTEGER,
                        value BLOB,
                        config BLOB NOT NULL,
                        value_repr STRING,
                        job_setup BLOB,
                        created TEXT,
                        comp_time FLOAT,

                        PRIMARY KEY (builder, key)
                        CONSTRAINT builder_task
                            FOREIGN KEY (builder)
                            REFERENCES builders(name)
                            ON DELETE CASCADE
                        CONSTRAINT executor_task
                            FOREIGN KEY (executor)
                            REFERENCES executors(id)
                            ON DELETE CASCADE
                    );
                """)

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS deps (
                        builder_s STRING NOT NULL,
                        key_s STRING NOT NULL,
                        builder_t STRING NOT NULL,
                        key_t STRING NOT NULL,

                        UNIQUE(builder_s, key_s, builder_t, key_t),

                        CONSTRAINT entry_s_task
                            FOREIGN KEY (builder_s, key_s)
                            REFERENCES entries(builder, key)
                            ON DELETE CASCADE
                            DEFERRABLE INITIALLY DEFERRED,
                        CONSTRAINT entry_t_task
                            FOREIGN KEY (builder_t, key_t)
                            REFERENCES entries(builder, key)
                            ON DELETE CASCADE
                            DEFERRABLE INITIALLY DEFERRED
                    );
                """)

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS reports (
                        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        type TEXT NOT NULL,
                        executor INTEGER,
                        builder TEXT,
                        message TEXT,
                        config BLOB
                    );
                """)

        self._run(_helper)

    def _dump(self):

        def _helper():
            print("ENTRIES ----")
            c = self.conn.cursor()
            r = c.execute("SELECT * FROM entries")
            for x in r:
                print(x)
            print("DEPS -----")
            c = self.conn.cursor()
            r = c.execute("SELECT * FROM deps")
            for x in r:
                print(x)

        self._run(_helper)

    def _ensure_builder_query(self, cursor, name):
        cursor.execute("INSERT OR IGNORE INTO builders VALUES (?)", [name])

    def ensure_builder(self, name):

        def _helper():
            with self.conn:
                c = self.conn.cursor()
                self._ensure_builder_query(c, name)

        self._run(_helper)

    def clean_builder(self, name):

        def _helper():
            with self.conn:
                c = self.conn.cursor()
                c.execute(
                    """
WITH RECURSIVE
    selected(builder, key) AS (
        SELECT builder, key
        FROM entries
        WHERE builder = (?)
        UNION
        SELECT builder_t,  cast(key_t as TEXT)
        FROM selected
        JOIN deps ON selected.builder == deps.builder_s AND selected.key == deps.key_s
    )
DELETE FROM entries
WHERE rowid IN
    (SELECT entries.rowid
    FROM selected
    LEFT JOIN entries ON entries.builder == selected.builder AND entries.key == selected.key)
    """, [name])

        self._run(_helper)

    def create_entries(self, raw_entries):

        def _helper():
            data = [(e.builder_name, e.key, e.value, e.config, e.value_repr, e.job_setup)
                    for e in raw_entries]
            with self.conn:
                c = self.conn.cursor()
                c.executemany(
                    "INSERT INTO entries VALUES (?, ?, null, ?, ?, ?, ?, DATETIME('now'), null)", data)

        self._run(_helper)

    def set_entry_values(self, executor_id, raw_entries, stats=None, reports=None):
        if stats is not None:
            stats_data = (json.dumps(stats), executor_id)
        else:
            stats_data = None

        if reports:
            reports_data = [self._unfold_report(report) for report in reports]

        def _helper():
            data = [(e.value, e.value_repr, e.job_setup, e.comp_time, e.builder_name, e.key, executor_id)
                    for e in raw_entries]
            changes = 0
            with self.conn:
                c = self.conn.cursor()
                for d in data:
                    c.execute(
                        "UPDATE entries SET value = ?, value_repr = ?, job_setup = ?, created = DATETIME('now'), comp_time = ? WHERE builder = ? AND key = ? AND executor = ? AND value is null",
                        d)
                    changes += self.conn.changes()
                if stats_data:
                    c.execute(
                        """UPDATE executors SET stats = ?, heartbeat = DATETIME('now') WHERE id = ?""",
                        stats_data)
                if reports:
                    for r in reports_data:
                        self._insert_report(c, r)
            return changes

        if self._run(_helper) != len(raw_entries):
            raise Exception("Setting value to unannouced config (all configs: {})",
                            ["{}/{}".format(c.builder_name, c.key) for c in raw_entries])

    def get_recursive_consumers(self, builder_name, key):
        #WHERE EXISTS(SELECT null FROM selected AS s WHERE deps.builder_s == selected.builder AND deps.key_s == selected.key
        query = """
            {}
            SELECT builder, key FROM selected
        """.format(self.RECURSIVE_CONSUMERS)

        def _helper():
            c = self.conn.cursor()
            rs = c.execute(query, [builder_name, key])
            return [(r[0], r[1]) for r in rs]

        return self._run(_helper)

    def has_entry_by_key(self, builder_name, key):

        def _helper():
            c = self.conn.cursor()
            c.execute(
                "SELECT COUNT(*) FROM entries WHERE builder = ? AND key = ? AND value is not null",
                [builder_name, key])
            return bool(c.fetchone()[0])

        return self._run(_helper)

    def get_entry_state(self, builder_name, key):

        def _helper():
            c = self.conn.cursor()
            c.execute(
                "SELECT value is not null FROM entries WHERE builder = ? AND key = ? AND (value is not null OR executor is null OR executor in (SELECT id FROM executors WHERE {}))"
                .format(self.LIVE_EXECUTOR_QUERY), [builder_name, key])
            v = c.fetchone()
            if v is None:
                return None
            if v[0]:
                return "finished"
            else:
                return "announced"

        return self._run(_helper)

    def get_entry_no_config(self, builder_name, key, include_announced=False):

        def _helper():
            c = self.conn.cursor()
            if include_announced:
                c.execute(
                    """
                    SELECT value, job_setup, created, comp_time
                    FROM entries
                    WHERE builder = ? AND key = ? AND
                        (value is not null OR executor is null OR executor in
                        (SELECT id FROM executors WHERE {}))""".format(self.LIVE_EXECUTOR_QUERY),
                    [builder_name, key])
            else:
                c.execute(
                    """
                    SELECT value, job_setup, created, comp_time
                    FROM entries
                    WHERE builder = ? AND key = ? AND value is not null""",
                    [builder_name, key])
            return c.fetchone()

        result = self._run(_helper)
        if result is None:
            return None
        value, job_setup, created, comp_time = result
        return Entry(None,
                     pickle.loads(value) if value is not None else None,
                     pickle.loads(job_setup) if job_setup is not None else None,
                     created,
                     comp_time)

    def get_entry(self, builder_name, key):

        def _helper():
            c = self.conn.cursor()
            c.execute(
                """
                SELECT config, value, job_setup, created, comp_time
                FROM entries
                WHERE builder = ? AND key = ? AND value is not null""", [builder_name, key])
            return c.fetchone()

        result = self._run(_helper)
        if result is None:
            return None
        config, value, job_setup, created, comp_time = result
        return Entry(
            pickle.loads(config),
            pickle.loads(value) if value is not None else None,
            pickle.loads(job_setup) if job_setup is not None else None,
            created, comp_time)

    def get_config(self, builder_name, key):

        def _helper():
            c = self.conn.cursor()
            c.execute(
                """
                SELECT config
                FROM entries
                WHERE builder = ? AND key = ?""", [builder_name, key])
            return c.fetchone()

        result = self._run(_helper)
        if result is None:
            return None
        config = result[0]
        return pickle.loads(config)

    def remove_entries_by_key(self, task_keys):
        data = [(r.builder_name, r.key) for r in task_keys]

        def _helper():
            with self.conn:
                self.conn.cursor().executemany(
                    """{}
                DELETE FROM entries
                WHERE rowid IN
                    (SELECT entries.rowid
                     FROM selected LEFT JOIN entries ON
                     entries.builder == selected.builder AND entries.key == selected.key)
                """.format(self.RECURSIVE_CONSUMERS), data)

        self._run(_helper)

    def invalidate_entries_by_key(self, task_keys):
        data = [(r.builder_name, r.key) for r in task_keys]

        def _helper():
            with self.conn:
                self.conn.cursor().executemany(
                    """
WITH RECURSIVE
children(builder, key) AS (
    WITH RECURSIVE
    parents(builder, key) AS (
        VALUES(?, ?)
        UNION
        SELECT builder_s,  cast(key_s as TEXT) FROM parents, deps WHERE parents.builder == deps.builder_t AND parents.key == deps.key_t
    )
    SELECT *
    FROM parents
    UNION
    SELECT builder_t,  cast(key_t as TEXT) FROM children, deps WHERE children.builder == deps.builder_s AND children.key == deps.key_s
)
DELETE FROM entries
    WHERE rowid IN
        (SELECT entries.rowid
         FROM children LEFT JOIN entries ON
         entries.builder == children.builder AND entries.key == children.key)
                """.format(self.RECURSIVE_CONSUMERS), data)

        self._run(_helper)

    """
    def remove_entries(self, builder_key_pairs):
        def _helper():
            self.conn.executemany("DELETE FROM entries WHERE builder = ? AND key = ?", builder_key_pairs)
        self.executor.submit(_helper).result()
    """

    def builder_summaries(self):

        def _helper():
            c = self.conn.cursor()
            r = c.execute(
                "SELECT builder, COUNT(key), TOTAL(length(value)), TOTAL(length(config)) FROM entries GROUP BY builder ORDER BY builder"
            )
            result = []
            found = set()
            for name, count, size_value, size_config in r.fetchall():
                found.add(name)
                result.append({"name": name, "count": count, "size": size_value + size_config})

            c.execute("SELECT name FROM builders")
            for x in r.fetchall():
                name = x[0]
                if name in found:
                    continue
                result.append({"name": name, "count": 0, "size": 0})

            result.sort(key=lambda x: x["name"])
            return result

        return self._run(_helper)

    def _cleanup_lost_entries(self, cursor):
        cursor.execute(
            "DELETE FROM entries WHERE value is null AND executor IN (SELECT id FROM executors WHERE {})"
            .format(self.DEAD_EXECUTOR_QUERY))

    def _unfold_report(self, report):
        return (report.report_type, report.executor_id, report.builder_name, report.message,
                pickle.dumps(report.config) if report.config is not None else None)

    def _insert_report(self, cursor, unfolded_report):
        cursor.execute(
            "INSERT INTO reports (timestamp, type, executor, builder, message, config)"
            "VALUES (DATETIME('now'), ?, ?, ?, ?, ?)", unfolded_report)

    def insert_report(self, report):
        report = self._unfold_report(report)

        def _helper():
            with self.conn:
                c = self.conn.cursor()
                self._insert_report(c, report)

        self._run(_helper)

    def get_reports(self, count):

        def _helper():
            c = self.conn.cursor()
            c.execute(
                "SELECT timestamp, type, executor, builder, message, config "
                "FROM reports ORDER BY id DESC LIMIT ?", (count, ))
            return list(c.fetchall())

        return [
            Report(
                report_type,
                executor_id,
                message,
                builder_name=builder_name,
                config=pickle.loads(config) if config else None,
                timestamp=timestamp) for timestamp, report_type, executor_id, builder_name,
            message, config in self._run(_helper)
        ]

    def announce_entries(self, executor_id, tasks, deps, report=None):

        def _helper():
            if report:
                report_data = self._unfold_report(report)
            entry_data = [(r.builder_name, r.key, pickle.dumps(r.config), executor_id)
                          for r in tasks]
            deps_data = [(r1.builder_name, r1.key, r2.builder_name, r2.key)
                         for r1, r2 in deps]
            try:
                with self.conn:
                    c = self.conn.cursor()
                    self._cleanup_lost_entries(c)
                    c.executemany(
                        "INSERT INTO entries(builder, key, config, executor) VALUES (?, ?, ?, ?)",
                        entry_data)
                    c.executemany("INSERT INTO deps VALUES (?, ?, ?, ?)", deps_data)
                    if report:
                        self._insert_report(c, report_data)
                return True
            except apsw.ConstraintError as e:
                logger.debug(e)
                return False

        assert executor_id is not None
        return self._run(_helper)

    def unannounce_entries(self, executor_id, task_keys):

        def _helper():
            data = [(r.builder_name, r.key, executor_id) for r in task_keys]
            with self.conn:
                c = self.conn.cursor()
                self._cleanup_lost_entries(c)
                c.executemany(
                    "DELETE FROM entries WHERE builder = ? AND key = ? AND executor = ? AND value is null",
                    data)

        return self._run(_helper)

    def entry_summaries(self, builder_name):

        def _helper():
            c = self.conn.cursor()
            r = c.execute(
                "SELECT key, config, length(value), value_repr, created, comp_time FROM entries WHERE builder = ?",
                [builder_name])
            return [{
                "key": key,
                "config": pickle.loads(config),
                "size": value_size + len(config) if value_size else len(config),
                "value_repr": value_repr,
                "created": created,
                "comp_time": comp_time
            } for key, config, value_size, value_repr, created, comp_time in r.fetchall()]

        return self._run(_helper)

    def register_executor(self, executor):
        assert executor.id is None

        def _helper():
            stats = json.dumps(executor.get_stats())
            with self.conn:
                c = self.conn.cursor()
                c.execute(
                    "INSERT INTO executors(created, heartbeat, heartbeat_interval, stats, name, hostname, resources) VALUES (?, DATETIME('now'), ?, ?, ?, ?, ?)",
                    [
                        executor.created.isoformat(), executor.heartbeat_interval, stats,
                        executor.name, executor.hostname, executor.resources
                    ])
                executor.id = self.conn.last_insert_rowid()

        self._run(_helper)

    def executor_summaries(self):

        def get_status(is_dead, stats):
            if stats is None:
                return "stopped"
            if not is_dead:
                return "running"
            else:
                return "lost"

        def _helper():
            c = self.conn.cursor()
            r = c.execute(
                "SELECT id, created, {}, stats, name, hostname, resources FROM executors".format(
                    self.DEAD_EXECUTOR_QUERY))
            #r = c.execute("SELECT uuid, created, , stats, type, version, resources FROM executors")

            return [{
                "id": id,
                "created": created,
                "status": get_status(is_dead, stats),
                "stats": json.loads(stats) if stats else None,
                "name": name,
                "hostname": hostname,
                "resources": resources,
            } for id, created, is_dead, stats, name, hostname, resources in r.fetchall()]

        return self._run(_helper)

    def get_run_stats(self, builder_name):

        def _helper():
            with self.conn:
                c = self.conn.cursor()
                c.execute(
                    """SELECT AVG(comp_time), COUNT(comp_time) FROM entries WHERE builder = ? AND comp_time is not null""",
                    [builder_name])
                avg, count = c.fetchone()
                if avg and count > 2:
                    c.execute(
                        """SELECT SUM((comp_time - ?2) * (comp_time - ?2)) FROM entries WHERE builder = ?1 AND comp_time is not null""",
                        [builder_name, avg])
                    stdev = math.sqrt(c.fetchone()[0] / (count - 1))
                elif avg:
                    stdev = 0
                else:
                    stdev = None
                return {"avg": avg, "stdev": stdev, "count": count}

        return self._run(_helper)

    def update_heartbeat(self, id):

        def _helper():
            id_list = [id]
            with self.conn:
                c = self.conn.cursor()
                c.execute(
                    """UPDATE executors SET heartbeat = DATETIME('now') WHERE id = ? AND stats is not null""",
                    id_list)

        self._run(_helper)

    def update_stats(self, id, stats):

        def _helper():
            with self.conn:
                c = self.conn.cursor()
                c.execute(
                    """UPDATE executors SET stats = ?, heartbeat = DATETIME('now') WHERE id = ?""",
                    stats_data)

        stats_data = [json.dumps(stats), id]
        self._run(_helper)

    def update_executor_stats(self, uuid, stats):
        assert stats != None
        raise NotImplementedError

    def stop_executor(self, id):

        def _helper():
            with self.conn:
                c = self.conn.cursor()
                c.execute(
                    """UPDATE executors SET heartbeat = DATETIME('now'), stats = null WHERE id = ?""",
                    [id])
                c.execute("""DELETE FROM entries WHERE executor == ? AND value is null""", [id])

        self._run(_helper)

    def get_all_entries(self, builder_name):

        def _helper():
            c = self.conn.cursor()
            r = c.execute(
                "SELECT config, value, comp_time, job_setup, created FROM entries WHERE builder = ?",
                [builder_name])
            return list(r.fetchall())

        return [
            Entry(pickle.loads(config),
                  pickle.loads(value),
                  pickle.loads(job_setup) if job_setup is not None else None,
                  created, comp_time)
            for (config, value, comp_time, job_setup, created) in self._run(_helper)
        ]

    def get_all_configs(self, builder_name):

        def _helper():
            c = self.conn.cursor()
            r = c.execute("SELECT config FROM entries WHERE builder = ? AND value is not null",
                          [builder_name])
            return list(r.fetchall())

        return [pickle.loads(row[0]) for row in self._run(_helper)]

    def upgrade_builder(self, builder_name, data):
        # Data has to be [(builder_name, old_key, new_key, config)]
        # UPDATE deps SET key_t = ?3 WHERE builder_t = ?1 AND key_t = ?2;
        shorter_data = [d[:3] for d in data]

        def _helper():
            with self.conn:
                c = self.conn.cursor()
                c.executemany("UPDATE deps SET key_s = ?3 WHERE builder_s = ?1 AND key_s = ?2;",
                              shorter_data)
                c.executemany("UPDATE deps SET key_t = ?3 WHERE builder_t = ?1 AND key_t = ?2;",
                              shorter_data)
                c.executemany(
                    "UPDATE entries SET key = ?3, config = ?4 WHERE builder = ?1 AND key = ?2;",
                    data)

        self._run(_helper)
