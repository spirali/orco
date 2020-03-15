
import sqlalchemy as sa
import enum

from orco.entry import EntryKey, EntryMetadata


def _set_sqlite_pragma(dbapi_connection, _connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


class JobState(enum.Enum):
    NONE = "n"
    ANNOUNCED = "a"
    RUNNING = "r"
    FINISHED = "f"
    ERROR = "e"


class Database:

    def __init__(self, url):
        engine = sa.create_engine(url)
        if "sqlite" in engine.dialect.name:
            sa.event.listen(engine, "connect", _set_sqlite_pragma)
        self.url = url

        metadata = sa.MetaData()
        self.jobs = sa.Table(
            "jobs",
            metadata,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("state", sa.Enum(JobState)),
            sa.Column("builder", sa.String(80)),
            sa.Column("key", sa.String),
            sa.Column("config", sa.PickleType),
            sa.Column("job_setup", sa.PickleType, nullable=True),
            sa.Column("created", sa.DateTime(timezone=True), nullable=False, server_default=sa.sql.func.now()),
            sa.Column("computation_time", sa.Integer(), nullable=True),
            sa.Index("builder", "key"),
        )

        self.job_deps = sa.Table(
            "job_deps",
            metadata,
            sa.Column("source_id", sa.Integer(), sa.ForeignKey("jobs.id", ondelete="cascade")),
            sa.Column("target_id", sa.Integer(), sa.ForeignKey("jobs.id", ondelete="cascade")))

        self.blobs = sa.Table(
            "blobs",
            metadata,
            sa.Column("job_id", sa.ForeignKey("jobs.id", ondelete="RESTRICT"), primary_key=True, nullable=False),
            # None = primary result
            sa.Column("name", sa.String, nullable=True, primary_key=True),
            sa.Column("data", sa.LargeBinary)

        )

        self.metadata = metadata
        self.engine = engine
        self.conn = engine.connect()

    def init(self):
        self.metadata.create_all(self.engine)

    def get_entry_state(self, builder_name, key):
        js = self.jobs
        r = self.conn.execute(sa.select([js.c.state]).where(sa.and_(js.c.builder == builder_name, js.c.key == key, js.c.state != JobState.ERROR))).fetchone()
        if r is None:
            return JobState.NONE
        else:
            return r[0]

    def _remove_jobs(self, cond):
        self.conn.execute(sa.delete(self.jobs).where(cond))

    def fix_crashed_jobs(self):
        js = self.jobs
        with self.conn.begin():
            cond = sa.or_(js.c.state == JobState.RUNNING, js.c.state == JobState.ANNOUNCED)
            self._remove_jobs(cond)

    def set_running(self, job_id):
        assert job_id is not None
        c = self.jobs.c
        with self.conn.begin():
            cond = sa.and_(c.id == job_id, c.state == JobState.ANNOUNCED)
            r = self.conn.execute(sa.update(self.jobs).where(cond).values(state=JobState.RUNNING))
            if r.rowcount != 1:
                raise Exception("Setting a job into a running state failed")

        job = self.conn.execute(sa.select([c.config, c.job_setup]).where(c.id == job_id)).fetchone()

        d = self.job_deps.c
        query = sa.select([c.id, c.builder, c.key])\
            .where(c.id.in_(sa.select([d.source_id]).where(d.target_id == job_id)))

        keys_to_job_ids = {
            EntryKey(r.builder, r.key): r.id
            for r in self.conn.execute(query)
        }

        return job.job_setup, job.config, keys_to_job_ids

    def set_finished(self, job_id, value, computation_time):
        assert job_id is not None
        c = self.jobs.c
        with self.conn.begin():
            cond = sa.and_(c.id == job_id, c.state == JobState.RUNNING)
            r = self.conn.execute(sa.update(self.jobs).where(cond).values(state=JobState.FINISHED, computation_time=computation_time))
            if r.rowcount != 1:
                raise Exception("Setting a job into finished state failed")
            if value is not None:
                self.conn.execute(sa.insert(self.blobs).values(job_id=job_id, name=None, data=value))

    def set_error(self, job_id, message, computation_time):
        assert job_id is not None
        c = self.jobs.c
        with self.conn.begin():
            cond = sa.and_(c.id == job_id, c.state.in_((JobState.RUNNING, JobState.ANNOUNCED)))
            r = self.conn.execute(sa.update(self.jobs).where(cond).values(state=JobState.ERROR, computation_time=computation_time))
            if r.rowcount != 1:
                raise Exception("Setting a job into finished state failed")
            if message is not None:
                self.conn.execute(sa.insert(self.blobs).values(job_id=job_id, name="!error_message", data=message.encode()))

    def get_entry_job_id_and_state(self, builder_name, key):
        c = self.jobs.c
        r = self.conn.execute(sa.select([c.id, c.state]).where(sa.and_(c.builder == builder_name, c.key == key, c.state != JobState.ERROR))).fetchone()
        if r is None:
            return None, JobState.NONE
        else:
            return r

    def get_blob(self, job_id, name):
        c = self.blobs.c
        r = self.conn.execute(sa.select([c.data]).where(sa.and_(c.job_id == job_id, c.name == name))).fetchone()
        if r is None:
            return r
        return r[0]

    def create_job_with_value(self, builder_name, key, config, value):
        c = self.jobs.c
        conn = self.conn
        columns = [
            c.state,
            c.builder,
            c.key,
            c.config,
        ]
        with conn.begin():
            test_existing = sa.exists([c.id]).where(
                sa.and_(c.builder == builder_name,
                        c.key == key,
                        c.state != JobState.ERROR))
            create_data = sa.select([sa.literal(JobState.FINISHED, sa.Enum(JobState)),
                                     sa.literal(builder_name),
                                     sa.literal(key),
                                     sa.literal(config, sa.PickleType)]).where(~test_existing)
            r = conn.execute(self.jobs.insert().from_select(columns, create_data))
            if r.rowcount != 1:
                return False
            job_id = r.lastrowid
            assert job_id is not None
            if value is not None:
                self.conn.execute(sa.insert(self.blobs).values(job_id=job_id, name=None, data=value))
            return True

    def announce_jobs(self, plan):
        """
            Because not all databases support partial indices, we are doing it
            rather complicated way :(
        """
        c = self.jobs.c
        columns = [
            c.state,
            c.builder,
            c.key,
            c.config,
            c.job_setup,
        ]
        conn = self.conn
        with conn.begin() as transaction:
            # Try to announce entries
            for pn in plan.nodes:
                assert pn.job_id is None
                test_existing = sa.exists([c.id]).where(
                    sa.and_(c.builder == pn.builder_name,
                            c.key == pn.key,
                            c.state != JobState.ERROR))
                create_data = sa.select([sa.literal(JobState.ANNOUNCED, sa.Enum(JobState)),
                                         sa.literal(pn.builder_name),
                                         sa.literal(pn.key),
                                         sa.literal(pn.config, sa.PickleType),
                                         sa.literal(pn.job_setup, sa.PickleType)]).where(~test_existing)
                r = conn.execute(self.jobs.insert().from_select(columns, create_data))
                if r.rowcount != 1:
                    transaction.rollback()
                    return False
                # ???? For some reason r.inserted_primary_key does not work here
                job_id = r.lastrowid
                assert job_id is not None
                pn.job_id = job_id

            # Announce deps
            deps = []
            for pn in plan.nodes:
                job_id = pn.job_id
                for inp in pn.inputs:
                    deps.append({"source_id": inp.job_id, "target_id": job_id})
                for j_id in pn.existing_dep_ids:
                    deps.append({"source_id": j_id, "target_id": job_id})
            if deps:
                conn.execute(self.job_deps.insert(), deps)
        return True

    def read_metadata(self, job_id):
        c = self.jobs.c
        r = self.conn.execute(sa.select([c.created, c.computation_time]).where(c.id == job_id)).fetchone()
        if r is None:
            return None
        return EntryMetadata(created=r.created, computation_time=r.computation_time)

    def unannounce_jobs(self, plan):
        c = self.jobs.c
        ids = [pn.job_id for pn in plan.nodes]
        with self.conn.begin():
            cond = sa.and_(c.id.in_(ids), c.state.in_((JobState.RUNNING, JobState.ANNOUNCED)))
            self._remove_jobs(cond)

    def get_run_stats(self, builder_name):
        # TODO: UPDAE THIS
        return {"avg": 1.0, "stdev": 1.0, "count": 1}

    def get_all_configs(self, builder_name):
        c = self.jobs.c
        return [r[0] for r in self.conn.execute(sa.select([c.config]).where(sa.and_(c.builder == builder_name, c.state == JobState.FINISHED)))]

    def builder_summaries(self, registered_builders):
        c = self.jobs.c
        query = sa.select([c.builder, sa.func.total(sa.func.length(c.config)).label("size")]).group_by(c.builder)
        #sa.func.length(c.config)

        result = {
            row.builder: {"name": row.builder, "n_finished": 0, "n_failed": 0, "n_running": 0, "n_in_progress": 0, "size": row.size}
            for row in self.conn.execute(query)
        }

        switch = {
            JobState.FINISHED: "n_finished",
            JobState.ERROR: "n_failed",
            JobState.RUNNING: "n_in_progress",
            JobState.ANNOUNCED: "n_in_progress",
        }
        query = sa.select([c.builder, c.state, sa.func.count(c.key).label("count")]).group_by(c.builder, c.state)
        for r in self.conn.execute(query):
            result[r.builder][switch[r.state]] = r.count

        query = sa.select([c.builder, sa.func.total(sa.func.length(self.blobs.c.data)).label("size")]) \
                .select_from(self.blobs.join(self.jobs)).group_by(c.builder)
        for row in self.engine.connect().execute(query):
            result[row.builder]["size"] += row.size

        for builder in registered_builders:
            if builder.name not in result:
                result[builder.name] = {"name": builder.name, "n_finished": 0, "n_failed": 0, "n_running": 0, "n_in_progress": 0, "size": 0}
        return sorted(result.values(), key=lambda r: r["name"])

    def entry_summaries(self, builder_name):
        c = self.jobs.c
        query = sa.select([c.key, c.state, c.config, c.created, c.computation_time, sa.func.total(sa.func.length(c.config)).label("size")])\
                .select_from(self.jobs.join(self.blobs, isouter=True)).where(c.builder == builder_name).group_by(c.key)
        return [
            {
                "key": row.key,
                "state": row.state.value,
                "config": row.config,
                "size": row.size,
                "comp_time": row.computation_time,
                "created": str(row.created),
            }
            for row in self.engine.connect().execute(query)
        ]
