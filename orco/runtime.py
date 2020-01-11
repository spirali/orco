import collections
import logging
import threading
import pickle
import time

from .builder import Builder
from .internals.db import DB
from .internals.executor import Executor
from .internals.job import Job
from .internals.rawentry import RawEntry
from .internals.runner import JobRunner
from .task import make_key
from .report import Report
from .internals.tasktools import collect_tasks, resolve_tasks
from .internals.utils import format_time

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


class _Builder:

    def __init__(self, name: str, build_fn, dep_fn, job_setup):
        self.name = name
        self.build_fn = build_fn
        self.make_raw_entry = _default_make_raw_entry
        self.dep_fn = dep_fn
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

    def __init__(self, db_path: str):
        self.db = DB(db_path)
        self.db.init()

        self._builders = {}
        self._lock = threading.Lock()

        self.executor = None
        self.stopped = False
        self.executor_args = {}
        self.runners = {}

        logging.debug("Starting runtime %s (db=%s)", self, db_path)

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

    def register_builder(self, name, build_fn=None, dep_fn=None, job_setup=None):
        with self._lock:
            if name in self._builders:
                raise Exception("Builder already registered")
            self.db.ensure_builder(name)
            builder = _Builder(name, build_fn, dep_fn, job_setup)
            self._builders[name] = builder
        return Builder(name)

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

    def get_entry_state(self, task):
        return self.db.get_entry_state(task.builder_name, task.key)

    def get_entry(self, task, include_announced=False):
        entry = self.db.get_entry_no_config(task.builder_name, task.key, include_announced)
        if entry is not None:
            entry.config = task.config
        return entry

    def get_entries(self, tasks, include_announced=False, drop_missing=False):
        results = [self.get_entry(task, include_announced) for task in tasks]
        if drop_missing:
            results = [entry for entry in results if entry is not None]
        return results

    def remove(self, task, remove_inputs=False):
        return self.remove_many([task], remove_inputs)

    def remove_many(self, tasks, remove_inputs=False):
        if remove_inputs:
            self.db.invalidate_entries_by_key(tasks)
        else:
            self.db.remove_entries_by_key(tasks)

    def insert(self, task, value):
        builder = self._builders[task.builder_name]
        entry = builder.make_raw_entry(builder.name, make_key(task.config), task.config, value, None, None)
        self.db.create_entries((entry,))

    def clean(self, builder_task):
        self.db.clean_builder(builder_task.name)

    def compute(self, obj, continue_on_error=False):
        results = self._compute_tasks(collect_tasks(obj), continue_on_error)
        return resolve_tasks(obj, results)

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

    def _get_builder(self, task):
        return self._builders[task.builder_name]

    def _create_compute_tree(self, tasks, exists, errors):
        jobs = {}
        global_deps = set()
        conflicts = set()

        def make_job(task):
            if task in exists:
                return task
            if task in conflicts or (errors and task in errors):
                return None
            job = jobs.get(task)
            if job is not None:
                return job
            builder = self._get_builder(task)
            state = self.db.get_entry_state(task.builder_name, task.key)
            if state == "finished":
                exists.add(task)
                return task
            if state == "announced":
                conflicts.add(task)
                return None
            if state is None and builder.dep_fn:
                dep_value = builder.dep_fn(task.config)
                dep_tasks = collect_tasks(dep_value)
                inputs = [make_job(r) for r in dep_tasks]
                if any(inp is None for inp in inputs):
                    return None
                for r in dep_tasks:
                    global_deps.add((r, task))
            else:
                inputs = ()
                dep_value = None
            if state is None and builder.build_fn is None:
                raise Exception(
                    "Computation depends on a missing configuration '{}' in a fixed builder"
                    .format(task))
            job = Job(task, inputs, dep_value, builder.create_job_setup(task.config))
            jobs[task] = job
            return job

        for task in tasks:
            make_job(task)

        return jobs, global_deps, len(conflicts)

    def _check_stopped(self):
        if self.stopped:
            raise Exception("Runtime was already stopped")

    def _print_report(self, jobs):
        jobs_per_builder = collections.Counter([t.task.builder_name for t in jobs.values()])
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

    def _run_computation(self, tasks, exists, errors):
        executor = self.executor
        jobs, global_deps, n_conflicts = self._create_compute_tree(tasks, exists, errors)
        if not jobs and n_conflicts == 0:
            return "finished"
        need_to_compute_tasks = [job.task for job in jobs.values()]
        if not need_to_compute_tasks:
            print("Waiting for finished computation on another executor ...")
            return "wait"
        logger.debug("Announcing tasks %s at executor %s", need_to_compute_tasks, executor.id)
        if not self.db.announce_entries(
                executor.id, need_to_compute_tasks, global_deps,
                Report("info", executor.id, "Computing {} job(s)".format(
                    len(need_to_compute_tasks)))):
            return "wait"
        try:
            # we do not this anymore, and .run may be long
            del global_deps, need_to_compute_tasks
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

    def _compute_tasks(self, tasks, continue_on_error=False):
        exists = set()
        if continue_on_error:
            errors = set()
        else:
            errors = None

        if self.executor is None:
            self.start_executor()

        while True:
            status = self._run_computation(tasks, exists, errors)
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
        return {task: self.get_entry(task) for task in tasks}
