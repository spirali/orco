import collections
import inspect
import logging
import pickle
import threading
import time

from orco.internals.context import _CONTEXT

from .builder import Builder
from .entry import Entry
from .internals.db import DB
from .internals.executor import Executor
from .internals.job import Job, JobNode
from .internals.key import make_key
from .internals.runner import JobRunner
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
        self.db = DB(db_path)
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
        self.db.ensure_builder(name)
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
        result = self.db.read_entry(entry, include_announced)
        if result is None:
            raise Exception("Entry {} is not in database".format(entry))
        return result

    def try_read_entry(self, entry, include_announced=False):
        return self.db.read_entry(entry, include_announced)

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
        entry.value = value
        builder = self._builders[entry.builder_name]
        entry = builder.make_raw_entry(builder.name, entry.key, entry.config, value, None, None)
        self.db.create_entries((entry,))

    def clear(self, builder):
        assert isinstance(builder, Builder)
        self.db.clear_builder(builder.name)

    def compute(self, entry, continue_on_error=False):
        return self._compute((entry,), continue_on_error)[0]

    def compute_many(self, entries, continue_on_error=False):
        return self._compute(entries, continue_on_error)

    def get_reports(self, count=100):
        return self.db.get_reports(count)

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

    def _builder_summaries(self):
        return self.db.builder_summaries()

    def _entry_summaries(self, builder_name):
        return self.db.entry_summaries(builder_name)

    def _executor_summaries(self):
        return self.db.executor_summaries()

    def _get_builder(self, builder_name):
        return self._builders[builder_name]

    def _create_compute_tree(self, entries, exists, errors):
        job_nodes = {}
        global_deps = set()
        conflicts = set()
        assert not hasattr(_CONTEXT, "on_entry") or _CONTEXT.on_entry is None

        def make_job_node(entry):
            if entry in exists:
                return "exists"
            if entry in conflicts or (errors and entry in errors):
                return None
            entry_key = entry.make_entry_key()
            job_node = job_nodes.get(entry_key)
            if job_node is not None:
                return job_node
            builder = self._get_builder(entry.builder_name)
            state = self.db.get_entry_state(entry.builder_name, entry.key)
            if state == "finished":
                exists.add(entry)
                return "exists"
            if state == "announced":
                conflicts.add(entry_key)
                return None
            if builder.fn is None:
                raise Exception(
                    "Computation depends on a missing configuration '{}' in a fixed builder"
                        .format(entry))

            deps = []
            try:
                _CONTEXT.on_entry = deps.append
                builder.run_with_config(entry.config, only_deps=True)
            finally:
                _CONTEXT.on_entry = None
            inputs = []
            for e in deps:
                j = make_job_node(e)
                if j is None:
                    return None
                if j == "exists":
                    continue
                inputs.append(j)
            for r in deps:
                global_deps.add((r.make_entry_key(), entry_key))
            job = Job(entry, deps, builder._create_job_setup(entry.config))
            job_node = JobNode(job, inputs)
            job_nodes[entry_key] = job_node
            return job_node

        for entry in entries:
            make_job_node(entry)

        return job_nodes, global_deps, len(conflicts)

    def _check_stopped(self):
        if self.stopped:
            raise Exception("Runtime was already stopped")

    def _print_report(self, job_nodes):
        jobs_per_builder = collections.Counter([job_node.job.entry.builder_name for job_node in job_nodes.values()])
        print("Scheduled jobs   |     # | Expected comp. time (per entry)\n"
              "-----------------+-------+--------------------------------")
        for col, count in sorted(jobs_per_builder.items()):
            stats = self.db.get_run_stats(col)
            if stats["avg"] is None:
                print("{:<17}| {:>5} | N/A".format(col, count))
            else:
                print("{:<17}| {:>5} | {:>8} +- {}".format(col, count, format_time(stats["avg"]),
                                                           format_time(stats["stdev"])))
        print("-----------------+-------+--------------------------------")

    def _run_computation(self, entries, exists, errors):
        executor = self.executor
        job_nodes, global_deps, n_conflicts = self._create_compute_tree(entries, exists, errors)
        if not job_nodes and n_conflicts == 0:
            return "finished"
        need_to_compute_entries = [job_node.job.entry for job_node in job_nodes.values()]
        if not need_to_compute_entries:
            print("Waiting for computation on another executor ...")
            return "wait"
        logger.debug("Announcing entries %s at executor %s", need_to_compute_entries, executor.id)
        if not self.db.announce_entries(
                executor.id, need_to_compute_entries, global_deps,
                Report("info", executor.id, "Computing {} job(s)".format(
                    len(need_to_compute_entries)))):
            return "wait"
        try:
            # we do not this anymore, and .run may be long
            del global_deps, need_to_compute_entries
            if n_conflicts:
                print(
                    "Some computation was temporarily skipped as they depends on jobs computed by another executor"
                )
            self._print_report(job_nodes)
            new_errors = executor.run(job_nodes, errors is not None)
            if errors is not None:
                errors.update(new_errors)
            else:
                assert not new_errors
            if n_conflicts == 0:
                return "finished"
            else:
                return "next"
        except:
            self.db.unannounce_entries(executor.id, list(job_nodes))
            raise

    def _compute(self, entries, continue_on_error=False):
        for entry in entries:
            if not isinstance(entry, Entry):
                raise Exception("'{!r}' is not an entry".format(entry))
        exists = set()
        if continue_on_error:
            errors = set()
        else:
            errors = None

        if self.executor is None:
            self.start_executor()

        while True:
            status = self._run_computation(entries, exists, errors)
            if status == "finished":
                break
            elif status == "next":
                continue
            elif status == "wait":
                time.sleep(1)
                continue
            else:
                assert 0
        if errors:
            print("During computation, {} errors occured, see reports for details".format(
                len(errors)))

        if not errors:
            for entry in entries:
                self.read_entry(entry)
            return entries
        else:
            return [self.try_read_entry(entry) if entry.make_entry_key() not in errors else None for entry in entries]
