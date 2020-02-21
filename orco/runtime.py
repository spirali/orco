import collections
import inspect
import logging
import threading
import pickle
import time

from .builder import Builder
from .entry import Entry
from .internals.db import DB
from .internals.executor import Executor
from .internals.job import Job
from .internals.rawentry import RawEntry
from .internals.runner import JobRunner
from .internals.key import make_key
from .report import Report
#from .internals.tasktools import collect_tasks, resolve_tasks
from .internals.utils import format_time
from orco.internals.context import _CONTEXT


logger = logging.getLogger(__name__)


def _default_make_raw_entry(builder_name, key, config, value, job_setup, comp_time):
    value_repr = repr(value)
    if len(value_repr) > 85:
        value_repr = value_repr[:80] + " ..."
    if config is not None:
        config = pickle.dumps(config)
    if job_setup is not None:
        job_setup = pickle.dumps(job_setup)
    return RawEntry(builder_name, key, config, pickle.dumps(value), value_repr, job_setup, comp_time)


class _BuilderDef:

    def __init__(self, name: str, main_fn, job_setup):
        self.name = name
        self.main_fn = main_fn
        self.make_raw_entry = _default_make_raw_entry
        self.job_setup = job_setup

    def create_job_setup(self, config):
        job_setup = self.job_setup
        if callable(job_setup):
            job_setup = job_setup(config)

        if job_setup is None:
            return {}
        elif isinstance(job_setup, str):
            return {"runner": job_setup}
        elif isinstance(job_setup, dict):
            return job_setup
        else:
            raise Exception("Invalid object as job_setup")


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
                self._register_builder(builder)

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

    def register_builder(self, name, build_fn=None, *, job_setup=None):
        if not isinstance(name, str) or not name:
            raise Exception("Builder name has to be non-empty string, not {!r}".format(name))
        with self._lock:
            builder = _BuilderDef(name, build_fn, job_setup)
            self._register_builder(builder)
        return Builder(name)

    def _register_builder(self, builder):
        name = builder.name
        if name in self._builders:
            raise Exception("Builder '{}' is already registered".format(name))
        self.db.ensure_builder(name)
        self._builders[name] = builder

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
        jobs = {}
        global_deps = set()
        conflicts = set()
        assert not hasattr(_CONTEXT, "on_entry") or _CONTEXT.on_entry is None

        def make_job(entry):
            if entry in exists:
                return entry
            if entry in conflicts or (errors and entry in errors):
                return None
            entry_key = entry.make_entry_key()
            job = jobs.get(entry_key)
            if job is not None:
                return job
            builder = self._get_builder(entry.builder_name)
            state = self.db.get_entry_state(entry.builder_name, entry.key)
            if state == "finished":
                exists.add(entry)
                return entry
            if state == "announced":
                conflicts.add(entry_key)
                return None

            if state is None and builder.main_fn and inspect.isgeneratorfunction(builder.main_fn):
                deps = []
                try:
                    _CONTEXT.on_entry = deps.append
                    it = builder.main_fn(entry.config)
                    next(it)
                except StopIteration:
                    raise Exception("Builder '{}' main function is generator but does not yield".format(entry.builder_name, entry))
                finally:
                    _CONTEXT.on_entry = None
                inputs = [make_job(r) for r in deps]
                if any(inp is None for inp in inputs):
                    return None
                for r in deps:
                    global_deps.add((r.make_entry_key(), entry_key))
            else:
                inputs = ()
            if state is None and builder.main_fn is None:
                raise Exception(
                    "Computation depends on a missing configuration '{}' in a fixed builder"
                    .format(entry))
            job = Job(entry, inputs, builder.create_job_setup(entry.config))
            jobs[entry_key] = job
            return job

        for entry in entries:
            make_job(entry)

        return jobs, global_deps, len(conflicts)

    def _check_stopped(self):
        if self.stopped:
            raise Exception("Runtime was already stopped")

    def _print_report(self, jobs):
        jobs_per_builder = collections.Counter([j.entry.builder_name for j in jobs.values()])
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
        jobs, global_deps, n_conflicts = self._create_compute_tree(entries, exists, errors)
        if not jobs and n_conflicts == 0:
            return "finished"
        need_to_compute_entries = [job.entry for job in jobs.values()]
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
            self._print_report(jobs)
            new_errors = executor.run(jobs, errors is not None)
            if errors is not None:
                errors.update(new_errors)
            else:
                assert not new_errors
            if n_conflicts == 0:
                return "finished"
            else:
                return "next"
        except:
            self.db.unannounce_entries(executor.id, list(jobs))
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