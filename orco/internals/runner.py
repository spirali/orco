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
        self.pickle_cache = {}

    def _create_pool(self):
        raise NotImplementedError

    def start(self):
        self.pool = self._create_pool()

    def stop(self):
        self.pool = None

    def submit(self, runtime, job):
        entry = job.entry
        pickled_fns = self.pickle_cache.get(entry.builder_name)
        if pickled_fns is None:
            builder = runtime._get_builder(entry.builder_name)
            pickled_fns = cloudpickle.dumps((builder.main_fn, builder.make_raw_entry))
            self.pickle_cache[entry.builder_name] = pickled_fns
        deps = [inp.entry.make_entry_key() if isinstance(inp, Job) else inp.make_entry_key() for inp in job.inputs]
        return self.pool.submit(_run_job, runtime.db.path, pickled_fns, entry.make_entry_key(), entry.config, deps,
                                job.job_setup)


class LocalProcessRunner(PoolJobRunner):

    def __init__(self, n_processes):
        super().__init__()
        self.n_processes = n_processes or os.cpu_count() or 1

    def _create_pool(self):
        return ProcessPoolExecutor(max_workers=self.n_processes)

    def get_resources(self):
        return "{} cpus".format(self.n_processes)


_per_process_db = None


def _run_job(db_path, fns, entry_key, config, dep_keys, job_setup):
    global _per_process_db
    try:
        if _per_process_db is None:
            _per_process_db = DB(db_path, threading=False)
        main_fn, finalize_fn = cloudpickle.loads(fns)
        temp_builder = Builder(main_fn, name=entry_key.builder_name)

        result = []

        def block_new_entries(_):
            raise Exception("Builders cannot be called during computation phase")

        def run():
            start_time = time.time()
            try:
                if inspect.isgeneratorfunction(main_fn):
                    deps = []
                    _CONTEXT.on_entry = deps.append
                    it = temp_builder.run_with_config(config)
                    next(it)
                    if set(e.make_entry_key() for e in deps) != set(dep_keys):
                        raise Exception("Builder function does not consistently return dependencies")
                    for entry in deps:
                        _per_process_db.read_entry(entry)
                    _CONTEXT.on_entry = block_new_entries
                    try:
                        next(it)
                        raise Exception("Computation function yielded for the second time")
                    except StopIteration as e:
                        value = e.value
                else:
                    _CONTEXT.on_entry = block_new_entries
                    value = temp_builder.run_with_config(config)
            finally:
                _CONTEXT.on_entry = None
            end_time = time.time()
            result.append(
                finalize_fn(entry_key.builder_name, entry_key.key, None, value, job_setup,
                            end_time - start_time))

        timeout = job_setup.get("timeout")
        if timeout is not None:
            thread = threading.Thread(target=run)
            thread.daemon = True
            thread.start()
            thread.join(timeout)
            if thread.is_alive():
                return JobTimeout(entry_key, timeout)
        else:
            run()
        return result[0]
    except Exception as exception:
        return JobError(entry_key, str(exception), traceback.format_exc())
