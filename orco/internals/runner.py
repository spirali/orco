import inspect
import os
import threading
import time
import traceback
from concurrent.futures.process import ProcessPoolExecutor

import cloudpickle

from orco.internals.context import _CONTEXT
from orco.internals.db import DB
# from orco.internals.tasktools import task_to_taskkey, collect_task_keys, resolve_task_keys
from orco.internals.job import Job
from orco import Builder


class JobFailure:

    def __init__(self, entry_key):
        self.entry_key = entry_key

    def message(self):
        raise NotImplementedError()

    def report_type(self):
        raise NotImplementedError()


class JobError(JobFailure):

    def __init__(self, entry_key, exception_str, traceback):
        super().__init__(entry_key)
        self.exception_str = exception_str
        self.traceback = traceback

    def message(self):
        return "Job failed: {}\n{}".format(self.exception_str, self.traceback)

    def report_type(self):
        return "error"


class JobTimeout(JobFailure):

    def __init__(self, entry_key, timeout):
        super().__init__(entry_key)
        self.timeout = timeout

    def message(self):
        return "Job timeouted after {} seconds".format(self.timeout)

    def report_type(self):
        return "timeout"


class JobRunner:

    def get_resources(self):
        raise NotImplementedError


class PoolJobRunner(JobRunner):

    def __init__(self):
        self.pool = None

    def _create_pool(self):
        raise NotImplementedError

    def start(self):
        self.pool = self._create_pool()

    def stop(self):
        self.pool = None

    def submit(self, runtime, job):
        builder = runtime._get_builder(job.entry.builder_name)
        return self.pool.submit(_run_job, runtime.db.path, builder, job)


class LocalProcessRunner(PoolJobRunner):

    def __init__(self, n_processes):
        super().__init__()
        self.n_processes = n_processes or os.cpu_count() or 1

    def _create_pool(self):
        return ProcessPoolExecutor(max_workers=self.n_processes)

    def get_resources(self):
        return "{} cpus".format(self.n_processes)


_per_process_db = None

def _run_job_timed(db, builder, job, result):
    start_time = time.time()
    deps = []

    def new_entry(e):
        deps.append(e)

    def block_new_entries(_):
        raise Exception("Builders cannot be called during computation phase")

    def after_deps():
        _CONTEXT.on_entry = block_new_entries
        dep_keys = [e.make_entry_key() for e in job.inputs]
        if set(e.make_entry_key() for e in deps) != set(dep_keys):
            raise Exception("Builder function does not consistently return dependencies")
        for entry in deps:
            db.read_entry(entry)

    try:
        _CONTEXT.on_entry = new_entry
        value = builder.run_with_config(job.entry.config, only_deps=False, after_deps=after_deps)
    finally:
        _CONTEXT.on_entry = None
    end_time = time.time()

    result.append(builder.make_raw_entry(builder.name, job.entry.make_entry_key().key, job.entry.config, value, job.job_setup, end_time - start_time))


def _run_job_int(db_path, builder, job):
    global _per_process_db
    if _per_process_db is None:
        _per_process_db = DB(db_path, threading=False)

    result = []

    timeout = job.job_setup.get("timeout")
    if timeout is not None:
        thread = threading.Thread(target=_run_job_timed, args=(_per_process_db, builder, job, result))
        thread.daemon = True
        thread.start()
        thread.join(timeout)
        if thread.is_alive():
            return JobTimeout(job.entry.make_entry_key(), timeout)
    else:
        _run_job_timed(_per_process_db, builder, job, result)
    return result[0]


def _run_job(db_path, builder, job):
    assert isinstance(builder, Builder)
    assert isinstance(job, Job)
    try:
        return _run_job_int(db_path, builder, job)
    except Exception as exception:
        return JobError(job.entry.make_entry_key(), str(exception), traceback.format_exc())
