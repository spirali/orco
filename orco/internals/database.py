
from orco import consts
import sqlalchemy as sa
import enum
import base64

from orco.entry import EntryKey, EntryMetadata, Entry


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
            self.is_sqlite = True
        else:
            self.is_sqlite = False
        self.url = url

        metadata = sa.MetaData()
        self.jobs = sa.Table(
            "jobs",
            metadata,
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("state", sa.Enum(JobState)),
            sa.Column("builder", sa.String(80)),
            sa.Column("key", sa.String(56)),  # 56 = hexdigest of sha224
            sa.Column("config", sa.PickleType),
            sa.Column("job_setup", sa.PickleType, nullable=True),
            sa.Column("created_date", sa.DateTime(timezone=True), nullable=False, server_default=sa.sql.func.now()),
            sa.Column("finished_date", sa.DateTime(timezone=True), nullable=True),
            sa.Column("computation_time", sa.Integer(), nullable=True),
            sa.Index("builder_idx", "builder"),
            sa.Index("key_idx", "key"),
            sa.Index("finished_date_idx", "finished_date"),
        )

        self.announcements = sa.Table("announcements", metadata,
                                      sa.Column("key", sa.String(56)),  # 56 = hexdigest of sha224
                                      sa.Column("job_id", sa.ForeignKey("jobs.id", ondelete="cascade"), index=True),
                                      sa.UniqueConstraint("key", name="uq_bk"))

        self.job_deps = sa.Table(
            "job_deps",
            metadata,
            sa.Column("source_id", sa.Integer(), sa.ForeignKey("jobs.id", ondelete="cascade")),
            sa.Column("target_id", sa.Integer(), sa.ForeignKey("jobs.id", ondelete="cascade")))


        self.blobs = sa.Table(
            "blobs",
            metadata,
            sa.Column("job_id", sa.ForeignKey("jobs.id", ondelete="cascade")),
            sa.Column("name", sa.String, nullable=True),
            sa.Column("data", sa.LargeBinary, nullable=False),
            sa.Column("mime", sa.String(255), nullable=False),
            sa.Column("repr", sa.String(85), nullable=True),
            # !!! (job_id, name) CANNOT be primary_key because postgresql do not allow None in primary key
            # !!! but unqiue is ok
            sa.UniqueConstraint("job_id", "name")
        )

        self.metadata = metadata
        self.engine = engine
        self.conn = engine.connect()

    def stop(self):
        self.conn = None

    def init(self):
        self.metadata.create_all(self.engine)

    def read_entry_all(self, entry):
        c = self.jobs.c
        result = []
        for r in self.conn.execute(sa.select([c.id, c.config]).where(c.key == entry.key)):
            entry = Entry(entry.builder_name, entry.key, r.config)
            entry.set_job_id(r.id, self)
            result.append(entry)
        return result

    def get_entry_state(self, key):
        js = self.jobs
        r = self.conn.execute(sa.select([js.c.state]).where(sa.and_(js.c.key == key, js.c.state != JobState.ERROR))).fetchone()
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

    def insert_blob(self, job_id, name, value, mime, repr_value):
        try:
            self.conn.execute(sa.insert(self.blobs).values(job_id=job_id, name=name, data=value, mime=mime, repr=repr_value))
        except sa.exc.IntegrityError:
            raise Exception("Blob '{}' already exists".format(name))

    def set_finished(self, job_id, value, repr_value, computation_time, output=None):
        assert job_id is not None
        c = self.jobs.c
        with self.conn.begin():
            cond = sa.and_(c.id == job_id, c.state == JobState.RUNNING)
            r = self.conn.execute(sa.update(self.jobs).where(cond).values(state=JobState.FINISHED, computation_time=computation_time, finished_date=sa.func.now()))
            if r.rowcount != 1:
                raise Exception("Setting a job into finished state failed")
            if value is not None:
                self.insert_blob(job_id, None, value, consts.MIME_PICKLE, repr_value)
            if output:
                self.insert_blob(job_id, "!output", output, consts.MIME_TEXT, None)

    def set_error(self, job_id, message, computation_time, output):
        assert job_id is not None
        c = self.jobs.c
        with self.conn.begin():
            cond = sa.and_(c.id == job_id, c.state.in_((JobState.RUNNING, JobState.ANNOUNCED)))
            self.conn.execute(self.announcements.delete().where(self.announcements.c.job_id == job_id))
            r = self.conn.execute(sa.update(self.jobs).where(cond).values(state=JobState.ERROR, computation_time=computation_time, finished_date=sa.func.now()))
            if r.rowcount != 1:
                raise Exception("Setting a job into finished state failed")
            if message is not None:
                self.conn.execute(sa.insert(self.blobs).values(job_id=job_id, name="!message", data=message.encode(), mime=consts.MIME_TEXT))
            if output:
                self.insert_blob(job_id, "!output", output, consts.MIME_TEXT, None)

    def get_entry_job_id_and_state(self, key):
        c = self.jobs.c
        r = self.conn.execute(sa.select([c.id, c.state]).where(sa.and_(c.key == key, c.state != JobState.ERROR))).fetchone()
        if r is None:
            return None, JobState.NONE
        else:
            return r

    def get_blob(self, job_id, name):
        c = self.blobs.c
        r = self.conn.execute(sa.select([c.data, c.mime]).where(sa.and_(c.job_id == job_id, c.name == name))).fetchone()
        if r is None:
            return None, None
        return r.data, r.mime

    def create_job_with_value(self, builder_name, key, config, value, repr_value):
        conn = self.conn
        with conn.begin() as transaction:
            r = conn.execute(self.jobs.insert().values(
                state=JobState.FINISHED,
                builder=builder_name,
                key=key,
                config=config,
                job_setup=None,
                finished_date=sa.func.now(),
            ))
            job_id = r.inserted_primary_key[0]
            assert job_id is not None
            try:
                conn.execute(self.announcements.insert().values(key=key, job_id=job_id))
            except sa.exc.IntegrityError:
                transaction.rollback()
                return False
            if value is not None:
                self.insert_blob(job_id, None, value, consts.MIME_PICKLE, repr_value)
            return True

    def announce_jobs(self, plan):
        """
            Because not all databases support partial indices, we are doing it
            rather complicated way :(
        """
        c = self.jobs.c
        conn = self.conn
        announces = []
        with conn.begin() as transaction:
            # Try to announce entries
            for pn in plan.nodes:
                r = conn.execute(self.jobs.insert().values(
                    state=JobState.ANNOUNCED,
                    builder=pn.builder_name,
                    key=pn.key,
                    config=pn.config,
                    job_setup=pn.job_setup
                ))
                job_id = r.inserted_primary_key[0]
                assert job_id is not None
                pn.job_id = job_id
                announces.append({
                    "key": pn.key,
                    "job_id": job_id,
                })
            try:
                r = conn.execute(self.announcements.insert(), announces)
            except sa.exc.IntegrityError:
                transaction.rollback()
                for pn in plan.nodes:
                    pn.job_id = None
                return False
            assert r.rowcount == len(plan.nodes)

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
        r = self.conn.execute(sa.select([c.created_date, c.finished_date, c.computation_time, c.job_setup]).where(c.id == job_id)).fetchone()
        if r is None:
            return None
        return EntryMetadata(
            created_date=r.created_date,
            finished_date=r.finished_date,
            computation_time=r.computation_time,
            job_setup=r.job_setup)

    def unannounce_jobs(self, plan):
        c = self.jobs.c
        ids = [pn.job_id for pn in plan.nodes]
        with self.conn.begin():
            cond = sa.and_(c.id.in_(ids), c.state.in_((JobState.RUNNING, JobState.ANNOUNCED)))
            self._remove_jobs(cond)

    def get_run_stats(self, builder_name):
        c = self.jobs.c
        count, avg = self.conn.execute(sa.select([sa.func.count(c.id), sa.func.avg(c.computation_time)]).where(sa.and_(c.builder == builder_name, c.computation_time != None))).fetchone()
        if avg is not None and count > 2:
            d = c.computation_time - avg
            r = self.conn.execute(sa.select([sa.func.sum(d * d)]).where(c.computation_time != None)).fetchone()
            stdev = r[0] / (count - 1)
        else:
            stdev = 0
        return {"avg": avg, "stdev": stdev, "count": count}

    def get_all_configs(self, builder_name):
        c = self.jobs.c
        return [(r.key, r.config) for r in self.conn.execute(sa.select([c.key, c.config]).where(sa.and_(c.builder == builder_name, c.state == JobState.FINISHED)))]

    def builder_summaries(self, registered_builders):
        c = self.jobs.c
        query = sa.select([c.builder, sa.func.sum(sa.func.length(c.config)).label("size")]).group_by(c.builder)
        #sa.func.length(c.config)

        result = {
            row.builder: {"name": row.builder, "n_finished": 0, "n_failed": 0, "n_in_progress": 0, "size": row.size}
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

        query = sa.select([c.builder, sa.func.sum(sa.func.length(self.blobs.c.data)).label("size")]) \
                .select_from(self.blobs.join(self.jobs)).group_by(c.builder)
        for row in self.conn.execute(query):
            result[row.builder]["size"] += row.size

        for builder in registered_builders:
            if builder.name not in result:
                result[builder.name] = {"name": builder.name, "n_finished": 0, "n_failed": 0, "n_in_progress": 0, "size": 0}
        return sorted(result.values(), key=lambda r: r["name"])

    def entry_summaries(self, builder_name):
        c = self.jobs.c
        query = sa.select([c.id, c.key, c.state, c.config, c.created_date, c.finished_date, c.computation_time, sa.func.sum(sa.func.length(c.config)).label("size")])\
                .select_from(self.jobs.join(self.blobs, isouter=True)).where(c.builder == builder_name).group_by(c.id)
        return [
            {
                "id": row.id,
                "key": row.key,
                "state": row.state.value,
                "config": row.config,
                "size": row.size,
                "comp_time": row.computation_time,
                "created": str(row.created_date),
                "finished": str(row.finished_date),
            }
            for row in self.conn.execute(query)
        ]

    def blob_summaries(self, job_id):
        def process_value(value, mime):
            if value is None:
                return None
            if mime == consts.MIME_TEXT:
                return value.decode()
            return base64.b64encode(value).decode()

        c = self.blobs.c
        query = sa.select([c.name, c.repr, c.mime,
                           sa.func.length(c.data).label("size"),
                           sa.case([(c.mime == consts.MIME_TEXT, c.data),
                                    (c.mime == "image/png", c.data)], else_=sa.null()).label("value")
                           ]).where(c.job_id == job_id)
        return [
            {
                "name": row.name,
                "repr": row.repr,
                "size": row.size,
                "value": process_value(row.value, row.mime),
                "mime": row.mime,
            }
            for row in self.conn.execute(query)
        ]

    def _get_current_jobs(self):
        # TODO: Find the real window (is it necessary?)
        c = self.jobs.c
        running = c.finished_date.is_(None)
        return sa.select([c.id]).where(sa.or_(running, c.finished_date >= (sa.select([sa.func.min(c.created_date)]).where(running))))

    def get_running_status(self):
        c = self.jobs.c
        switch = {
            JobState.RUNNING: "n_running",
            JobState.FINISHED: "n_finished",
            JobState.ANNOUNCED: "n_announced",
            JobState.ERROR: "n_failed",
        }
        counts = {name: 0 for name in switch.values()}

        for r in self.conn.execute(sa.select([c.state, sa.func.count(c.key).label("count")]).where(c.id.in_(self._get_current_jobs())).group_by(c.state)):
            counts[switch[r.state]] = r.count

        errors = [
            {
                "id": r.id,
                "builder": r.builder,
                "config": r.config,
                "finished": str(r.finished_date),
            }
            for r in self.conn.execute(sa.select([c.id, c.builder, c.config, c.finished_date]).where(c.state == JobState.ERROR).order_by(c.finished_date.desc()).limit(5))
        ]

        return {
            "counts": counts,
            "errors": errors,
        }

    def get_blob_names(self, job_id):
        c = self.blobs.c
        query = sa.select([c.name]).where(c.job_id == job_id).order_by(c.name.asc())
        return [r[0] for r in self.conn.execute(query) if r[0] is not None]

    def drop_builder(self, builder_name, drop_inputs):
        with self.conn.begin():
            c = self.jobs.c
            base_query = sa.select([c.id]).where(c.builder == builder_name)
            self.conn.execute(self.jobs.delete().where(c.id.in_(self._closure(base_query, drop_inputs))))

    def _downstream(self, base_query):
        c = self.job_deps.c
        q = base_query.cte("down_rec", recursive=True)
        return sa.select([q.union(sa.select([c.target_id]).select_from(self.job_deps).where(q.c.id == c.source_id))])

    def _upstream(self, base_query):
        c = self.job_deps.c
        q = base_query.cte("up_rec", recursive=True)
        return sa.select([q.union(sa.select([c.source_id]).select_from(self.job_deps).where(q.c.id == c.target_id))])

    def _closure(self, base_query, include_inputs):
        if include_inputs:
            base_query = self._upstream(base_query)
        return self._downstream(base_query)

    def drop_jobs_by_key(self, keys, drop_inputs):
        c = self.jobs.c
        base_query = sa.select([c.id]).where(c.key.in_(keys))
        self.conn.execute(self.jobs.delete().where(c.id.in_(self._closure(base_query, drop_inputs))))

    def export_builder(self, builder_name):
        c = self.jobs.c
        query = sa.select([c.config, c.computation_time]).where(sa.and_(builder_name == builder_name, c.state == JobState.FINISHED))
        return self.conn.execute(query)

    def upgrade_builder(self, data):
        with self.conn.begin():
            stmt = self.jobs.update().where(self.jobs.c.key == sa.bindparam('key')).values(key=sa.bindparam("new_key"), config=sa.bindparam("config"))
            self.conn.execute(stmt, data)
            stmt = self.jobs.update().where(self.announcements.key == sa.bindparam('key')).values(key=sa.bindparam("new_key"))
            self.conn.execute(stmt, data)