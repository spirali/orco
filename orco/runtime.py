import logging
import pickle
import threading
import time

from .builder import Builder, BuilderProxy
from .internals.database import Database, JobState
from .internals.executor import Executor
from .internals.key import make_key
from .internals.plan import Plan
from .internals.runner import JobRunner
from .internals.utils import make_repr
from .job import Job

logger = logging.getLogger(__name__)


def _check_unattached_job(obj, reattach):
    if not isinstance(obj, Job):
        raise Exception("'{!r}' is not an job".format(obj))
    if reattach:
        obj.detach()
    elif obj.is_attached():
        raise Exception("'{!r}' is already attached".format(obj))


class Runtime:
    """
    Core class of ORCO.

    It manages database with results and starts computations

    For SQLite:

    >>> runtime = Runtime("sqlite:///path/to/dbfile.db")

    For Postgress:

    >>> runtime = Runtime("postgresql://<USERNAME>:<PASSWORD>@<HOSTNAME>/<DATABASE>")
    """

    def __init__(
        self, db_path: str, global_builders=True, executor_name=None, n_processes=None
    ):
        self.db = Database(db_path)
        self.db.init()

        self._builders = {}
        self._lock = threading.Lock()

        self.executor = None
        self.stopped = False
        self.executor_args = {
            "name": executor_name,
            "n_processes": n_processes,
        }
        self.runners = {}

        logging.debug("Starting runtime %s (db=%s)", self, db_path)

        if global_builders:
            from .globals import _get_global_builders

            for builder in _get_global_builders():
                logging.debug("Registering global builder %s", builder.name)
                self.register_builder(builder)

    def __enter__(self):
        self._check_stopped()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.stopped:
            self.stop()

    def add_runner(self, name, runner):
        if self.executor:
            raise Exception("Runners cannot be added when the executor is started")
        assert isinstance(runner, JobRunner)
        assert isinstance(name, str)
        if name in self.runners:
            raise Exception("Runner under name '{}' is already registered".format(name))
        self.runners[name] = runner

    def stop(self):
        self._check_stopped()

        logger.debug("Stopping runtime %s", self)
        self.stop_executor()
        self.db.stop()
        self.stopped = True

    def start_executor(self):
        if self.executor:
            raise Exception("Executor is already dunning")
        logger.debug("Registering executor %s")
        executor = Executor(self, self.runners, **self.executor_args)
        executor.start()
        self.executor = executor
        return executor

    def stop_executor(self):
        if self.executor is not None:
            logger.debug("Unregistering executor %s", self.executor)
            self.executor.stop()
            self.executor = None

    def register_builder(self, builder: Builder):
        assert isinstance(builder, Builder)
        name = builder.name
        self._builders[name] = builder
        return builder.make_proxy()

    def serve(
        self, port=8550, *, debug=False, testing=False, daemon=False, host="127.0.0.1"
    ):
        from .internals.browser import init_service

        app = init_service(self)
        if testing:
            app.testing = True
            return app
        else:

            def run_app():
                if debug:
                    app.run(port=port, debug=debug, use_reloader=False)
                else:
                    from waitress import serve

                    serve(app, host=host, port=port)

            if daemon:
                t = threading.Thread(target=run_app)
                t.daemon = True
                t.start()
                return t
            else:
                run_app()

    def get_state(self, job):
        return self.db.get_active_state(job.key)

    def read(self, job, *, reattach=False):
        r = self.try_read(job, reattach)
        if r is None:
            raise Exception("No finished job for {}".format(job))
        return r

    def try_read(self, job, reattach=False):
        _check_unattached_job(job, reattach)
        job_id, state = self.db.get_active_job_id_and_state(job.key)
        if state != JobState.FINISHED:
            return None
        job.set_job_id(job_id, self.db, state)
        return job

    def read_jobs(self, job):
        return self.db.read_jobs(job.key, job.builder_name)

    def read_many(self, jobs, *, reattach=False, drop_missing=False):
        # TODO: Read from DB in one query
        results = []
        for job in jobs:
            if self.try_read(job, reattach):
                results.append(job)
            elif not drop_missing:
                results.append(None)
        return results

    def drop(self, job, drop_inputs=False):
        return self.drop_many([job], drop_inputs)

    def drop_many(self, jobs, drop_inputs=False):
        self.db.drop_jobs_by_key([job.key for job in jobs], drop_inputs)

    def archive(self, job, archive_inputs=False):
        self.archive_many([job], archive_inputs=archive_inputs)

    def archive_many(self, jobs, archive_inputs=False):
        for job in jobs:
            _check_unattached_job(job, False)
        self.db.archive_jobs_by_key([job.key for job in jobs], archive_inputs)

    def free(self, job):
        self.free_many([job])

    def free_many(self, jobs):
        for job in jobs:
            _check_unattached_job(job, False)
        self.db.free_jobs_by_key([job.key for job in jobs])

    def insert(self, job, value):
        r = self.db.create_job_with_value(
            job.builder_name, job.key, job.config, pickle.dumps(value), make_repr(value)
        )
        if not r:
            raise Exception("Job {} already exists".format(job))

    def drop_builder(self, builder_name, drop_inputs=False):
        assert isinstance(builder_name, str)
        self.db.drop_builder(builder_name, drop_inputs)

    def compute(self, job, *, reattach=False, continue_on_error=False, verbose=True):
        self._compute((job,), reattach, continue_on_error, verbose)
        return job

    def compute_many(
        self, jobs, *, reattach=False, continue_on_error=False, verbose=True
    ):
        self._compute(jobs, reattach, continue_on_error, verbose)
        return jobs

    def has_builder(self, builder_name):
        return builder_name in self._builders

    def get_builder(self, builder_name):
        return self._builders[builder_name]

    def upgrade_builder(self, builder, upgrade_fn):
        if isinstance(builder, BuilderProxy) or isinstance(builder, BuilderProxy):
            builder_name = builder.name
        else:
            builder_name = builder
        configs = self.db.get_all_configs(builder_name)
        to_update = []
        keys = set()
        for key, config in configs:
            config = upgrade_fn(config)
            new_key = make_key(builder_name, config)
            if new_key in keys:
                raise Exception(
                    "Key collision in upgrade, config={}".format(repr(config))
                )
            if new_key != key:
                to_update.append({"key": key, "new_key": new_key, "config": config})
            keys.add(new_key)
        self.db.upgrade_builder(to_update)

    def _check_stopped(self):
        if self.stopped:
            raise Exception("Runtime was already stopped")

    def _run_computation(self, plan, verbose):
        executor = self.executor
        plan.create(self)
        if plan.is_finished():
            return "finished"
        if plan.need_wait():
            print("Waiting for computation on another executor ...")
            return "wait"
        logger.debug("Announcing jobs %s at executor %s", len(plan.nodes), executor.id)
        if not self.db.announce_jobs(plan):
            return "wait"
        try:
            if plan.conflicts:
                print(
                    "Some computation was temporarily skipped as they depends on jobs computed by another executor"
                )
            if verbose:
                plan.print_report(self)
            executor.run(plan, verbose)
            if plan.is_finished():
                return "finished"
            else:
                return "next"
        except:
            self.db.unannounce_jobs(plan)
            plan.fill_job_ids(self, False)
            raise

    def _compute(self, jobs, reattach, continue_on_error, verbose):
        for job in jobs:
            _check_unattached_job(job, reattach)

        if self.executor is None:
            self.start_executor()

        plan = Plan(jobs, continue_on_error)

        while True:
            status = self._run_computation(plan, verbose)
            if status == "finished":
                break
            elif status == "next":
                continue
            elif status == "wait":
                time.sleep(1)
                continue
            else:
                assert 0
        plan.fill_job_ids(self, not continue_on_error)
        if plan.error_keys:
            print(
                "During computation, {} errors occured, see reports for details".format(
                    len(plan.error_keys)
                )
            )
