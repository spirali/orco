import collections
import inspect
import logging
import pickle
import threading
import time

from orco.internals.context import _CONTEXT

from .builder import Builder
from .job import Job
from .internals.database import Database, JobState
from .internals.executor import Executor
from .internals.plan import Plan
from .internals.key import make_key
from .internals.runner import JobRunner
from .internals.utils import make_repr

# from .internals.tasktools import collect_tasks, resolve_tasks
from .internals.utils import format_time
from .report import Report

logger = logging.getLogger(__name__)


class Runtime:
    """
    Core class of ORCO.

    It manages database with results and starts computations

    >>> runtime = Runtime("/path/to/dbfile.db")
    """

    def __init__(self, db_path: str, global_builders=True, executor_name=None, n_processes=None):
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
        if name in self._builders:
            raise Exception("Builder '{}' is already registered".format(name))
        #self.db.ensure_builder(name)
        self._builders[name] = builder
        return builder

    def serve(self, port=8550, debug=False, testing=False, nonblocking=False):
        from .internals.browser import init_service
        app = init_service(self)
        if testing:
            app.testing = True
            return app
        else:

            def run_app():
                app.run(port=port, debug=debug, use_reloader=False)

            if nonblocking:
                t = threading.Thread(target=run_app)
                t.daemon = True
                t.start()
                return t
            else:
                run_app()

    def get_state(self, job):
        return self.db.get_active_state(job.key)

    def read(self, job):
        r = self.try_read(job)
        if r is None:
            raise Exception("Not finished job for {}".format(job))
        return r

    def try_read(self, job):
        job_id, state = self.db.get_active_job_id_and_state(job.key)
        if state != JobState.FINISHED:
            return None
        job.set_job_id(job_id, self.db)
        return job

    def read_jobs(self, job):
        return self.db.read_jobs(job.key, job.builder_name)

    def read_many(self, jobs, drop_missing=False):
        # TODO: Read from DB in one query
        results = []
        for job in jobs:
            if self.try_read(job):
                results.append(job)
            elif not drop_missing:
                results.append(None)
        return results

    def drop(self, job, drop_inputs=False):
        return self.drop_many([job], drop_inputs)

    def drop_many(self, jobs, drop_inputs=False):
        self.db.drop_jobs_by_key([job.key for job in jobs], drop_inputs)

    def insert(self, job, value):
        r = self.db.create_job_with_value(job.builder_name, job.key, job.config, pickle.dumps(value), make_repr(value))
        if not r:
            raise Exception("Job {} already exists".format(job))

    def drop_builder(self, builder_name, drop_inputs=False):
        assert isinstance(builder_name, str)
        self.db.drop_builder(builder_name, drop_inputs)

    def compute(self, job, continue_on_error=False):
        self._compute((job,), continue_on_error)
        return job

    def compute_many(self, jobs, continue_on_error=False):
        self._compute(jobs, continue_on_error)
        return jobs

    def get_builder(self, builder_name):
        return self._builders[builder_name]

    def upgrade_builder(self, builder, upgrade_fn):
        if isinstance(builder, Builder):
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
                raise Exception("Key collision in upgrade, config={}".format(repr(config)))
            if new_key != key:
                to_update.append({"key": key, "new_key": new_key, "config": config})
            keys.add(new_key)
        self.db.upgrade_builder(to_update)

    def _check_stopped(self):
        if self.stopped:
            raise Exception("Runtime was already stopped")

    def _run_computation(self, plan):
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
            plan.print_report(self)
            executor.run(plan)
            if plan.is_finished():
                return "finished"
            else:
                return "next"
        except:
            self.db.unannounce_jobs(plan)
            plan.fill_job_ids(self)
            raise

    def _compute(self, jobs, continue_on_error=False):
        for job in jobs:
            if not isinstance(job, Job):
                raise Exception("'{!r}' is not an job".format(job))

        if self.executor is None:
            self.start_executor()

        plan = Plan(jobs, continue_on_error)

        while True:
            status = self._run_computation(plan)
            if status == "finished":
                break
            elif status == "next":
                continue
            elif status == "wait":
                time.sleep(1)
                continue
            else:
                assert 0
        plan.fill_job_ids(self)
        if plan.error_keys:
            print("During computation, {} errors occured, see reports for details".format(
                len(plan.error_keys)))