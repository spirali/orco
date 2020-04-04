import collections
import inspect
import logging
import pickle
import threading
import time

from orco.internals.context import _CONTEXT

from .builder import Builder
from .entry import Entry
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

    def __init__(self, db_path: str, global_builders=True):
        self.db = Database(db_path)
        self.db.init()

        self._builders = {}
        self._lock = threading.Lock()

        self.executor = None
        self.stopped = False
        self.executor_args = {}
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

    def configure_executor(self, name=None, n_processes=None, heartbeat_interval=7):
        self.executor_args = {
            "name": name,
            "n_processes": n_processes,
            "heartbeat_interval": heartbeat_interval,
        }

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

    def get_entry_state(self, entry):
        return self.db.get_entry_state(entry.builder_name, entry.key)

    def read_entry(self, entry, include_announced=False):
        r = self.try_read_entry(entry, include_announced)
        if r is None:
            raise Exception("Entry {} is not in database".format(entry))
        return r

    def try_read_entry(self, entry, include_announced=False):
        assert not include_announced  # TODO TODO
        job_id, state = self.db.get_entry_job_id_and_state(entry.builder_name, entry.key)
        if state != JobState.FINISHED:
            return False
        entry.set_job_id(job_id, self.db)
        return entry

    def read_entries(self, entries, include_announced=False, drop_missing=False):
        results = []
        for entry in entries:
            if self.try_read_entry(entry, include_announced):
                results.append(entry)
            elif not drop_missing:
                results.append(None)
        return results

    def remove(self, entry, remove_inputs=False):
        return self.remove_many([entry], remove_inputs)

    def remove_many(self, entries, remove_inputs=False):
        if remove_inputs:
            self.db.invalidate_entries_by_key(entries)
        else:
            self.db.remove_entries_by_key(entries)

    def insert(self, entry, value):
        r = self.db.create_job_with_value(entry.builder_name, entry.key, entry.config, pickle.dumps(value), make_repr(value))
        if not r:
            raise Exception("Entry {} already exists".format(entry))

    def drop_builder(self, builder_name):
        assert isinstance(builder_name, str)
        self.db.drop_builder(builder_name)

    def compute(self, entry, continue_on_error=False):
        self._compute((entry,), continue_on_error)
        return entry

    def compute_many(self, entries, continue_on_error=False):
        self._compute(entries, continue_on_error)
        return entries

    def get_reports(self, count=100):
        return self.db.get_reports(count)

    def get_builder(self, builder_name):
        return self._builders[builder_name]

    def upgrade_builder(self, builder, upgrade_fn):
        configs = self.db.get_all_configs(builder.name)
        to_update = []
        builder_name = builder.name
        keys = set()
        for config in configs:
            key = make_key(config)
            config = upgrade_fn(config)
            new_key = make_key(config)
            if new_key in keys:
                raise Exception("Key collision in upgrade, config={}".format(repr(config)))
            if new_key != key:
                to_update.append((builder_name, key, new_key, pickle.dumps(config)))
            keys.add(new_key)
        self.db.upgrade_builder(builder.name, to_update)

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
        logger.debug("Announcing entries %s at executor %s", len(plan.nodes), executor.id)
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

    def _compute(self, entries, continue_on_error=False):
        for entry in entries:
            if not isinstance(entry, Entry):
                raise Exception("'{!r}' is not an entry".format(entry))

        if self.executor is None:
            self.start_executor()

        plan = Plan(entries, continue_on_error)

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