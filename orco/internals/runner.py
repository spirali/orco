import threading
import time
import traceback
import os
from concurrent.futures.process import ProcessPoolExecutor

import cloudpickle
from orco.internals.db import DB

from orco.internals.tasktools import task_to_taskkey, collect_task_keys, resolve_task_keys


class JobFailure:

    def __init__(self, task_key):
        self.task_key = task_key

    def message(self):
        raise NotImplementedError()

    def report_type(self):
        raise NotImplementedError()


class JobError(JobFailure):

    def __init__(self, task_key, exception_str, traceback):
        super().__init__(task_key)
        self.exception_str = exception_str
        self.traceback = traceback

    def message(self):
        return "Job failed: {}\n{}".format(self.exception_str, self.traceback)

    def report_type(self):
        return "error"


class JobTimeout(JobFailure):

    def __init__(self, task_key, timeout):
        super().__init__(task_key)
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
        task = job.task
        pickled_fns = self.pickle_cache.get(task.builder_name)
        if pickled_fns is None:
            builder = runtime._get_builder(job.task)
            pickled_fns = cloudpickle.dumps((builder.build_fn, builder.make_raw_entry))
            self.pickle_cache[task.builder_name] = pickled_fns
        return self.pool.submit(_run_job, runtime.db.path, pickled_fns, task.task_key(), task.config, task_to_taskkey(job.dep_value), job.job_setup)


class LocalProcessRunner(PoolJobRunner):

    def __init__(self, n_processes):
        super().__init__()
        self.n_processes = n_processes or os.cpu_count() or 1

    def _create_pool(self):
        return ProcessPoolExecutor(max_workers=self.n_processes)

    def get_resources(self):
        return "{} cpus".format(self.n_processes)


_per_process_db = None


def _run_job(db_path, fns, task_key, config, dep_value, job_setup):
    global _per_process_db
    try:
        if _per_process_db is None:
            _per_process_db = DB(db_path, threading=False)
        build_fn, finalize_fn = cloudpickle.loads(fns)

        result = []

        def run():
            start_time = time.time()
            deps = collect_task_keys(dep_value)
            task_map = {task: _per_process_db.get_entry(task.builder_name, task.key) for task in deps}
            value = build_fn(config, resolve_task_keys(dep_value, task_map))
            end_time = time.time()
            result.append(
                finalize_fn(task_key.builder_name, task_key.key, None, value, job_setup,
                            end_time - start_time))
        timeout = job_setup.get("timeout")
        if timeout is not None:
            thread = threading.Thread(target=run)
            thread.daemon = True
            thread.start()
            thread.join(timeout)
            if thread.is_alive():
                return JobTimeout(task_key, timeout)
        else:
            run()
        return result[0]
    except Exception as exception:
        return JobError(task_key, str(exception), traceback.format_exc())


