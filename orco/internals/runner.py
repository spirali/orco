import collections
import os
import pickle
import tempfile
import threading
import time
import traceback
import sys
from concurrent.futures.process import ProcessPoolExecutor

import capturer

from .context import _CONTEXT
from .database import Database
from .database import JobState
from .utils import make_repr

JobContext = collections.namedtuple("JobContext", ["db", "job_id"])


class JobFailure:
    def __init__(self, job_id):
        self.job_id = job_id

    def message(self):
        raise NotImplementedError()

    def report_type(self):
        raise NotImplementedError()


class JobError(JobFailure):
    def __init__(self, job_id, exception_str, traceback):
        super().__init__(job_id)
        self.exception_str = exception_str
        self.traceback = traceback

    def message(self):
        return "Job failed: {}\n{}".format(self.exception_str, self.traceback)

    def report_type(self):
        return "error"


class JobTimeout(JobFailure):
    def __init__(self, job_id, timeout):
        super().__init__(job_id)
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
        self.pool.shutdown()
        self.pool = None

    def submit(self, runtime, plan_node):
        builder = runtime.get_builder(plan_node.builder_name)
        return self.pool.submit(_run_job, runtime.db.url, builder, plan_node.job_id)


class LocalProcessRunner(PoolJobRunner):
    def __init__(self, n_processes):
        super().__init__()
        self.n_processes = n_processes or os.cpu_count() or 1

    def _create_pool(self):
        pool = ProcessPoolExecutor(max_workers=self.n_processes)
        return pool

    def get_resources(self):
        return "{} cpus".format(self.n_processes)


_per_process_db = None


def _run_job_timed(db, job_id, builder, config, keys_to_job_ids, start_time, cpt):
    deps = []

    def block_new_jobs(_):
        raise Exception("Builders cannot be called during computation phase")

    def after_deps():
        _CONTEXT.on_job = block_new_jobs
        _CONTEXT.job_context = JobContext(db, job_id)
        if set(e.key for e in deps) != set(keys_to_job_ids):
            raise Exception(
                "Builder function does not consistently return dependencies"
            )
        for e in deps:
            e.set_job_id(keys_to_job_ids[e.key], db, JobState.FINISHED)

    original_cwd = os.getcwd()
    try:
        _CONTEXT.on_job = deps.append
        with tempfile.TemporaryDirectory() as tmp_dir:
            os.chdir(tmp_dir)
            value = builder.run_with_config(
                config, only_deps=False, after_deps=after_deps
            )
    finally:
        os.chdir(original_cwd)
        _CONTEXT.on_job = None
        _CONTEXT.job_context = None
        cpt.finish_capture()

    if value is None:
        value_repr = None
    else:
        value_repr = make_repr(value)
        value = pickle.dumps(value)
    _per_process_db.set_finished(
        job_id, value, value_repr, time.time() - start_time, cpt.get_bytes()
    )


def _run_job(db_path, builder_fn, job_id):
    # Workaround of the clash between jupyter & capturer
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__

    start_time = time.time()
    cpt = None
    global _per_process_db
    try:
        if _per_process_db is None:
            _per_process_db = Database(db_path)
        job_setup, config, keys_to_job_ids = _per_process_db.set_running(job_id)
        cpt = capturer.CaptureOutput(relay=job_setup.relay)
        cpt.start_capture()
        if job_setup.timeout is not None:
            thread = threading.Thread(
                target=_run_job_timed,
                args=(
                    _per_process_db,
                    job_id,
                    builder_fn,
                    config,
                    keys_to_job_ids,
                    start_time,
                    cpt,
                ),
            )
            thread.daemon = True
            thread.start()
            thread.join(job_setup.timeout)
            if thread.is_alive():
                t = JobTimeout(job_id, job_setup.timeout)
                _per_process_db.set_error(job_id, t.message(), time.time() - start_time)
                return t
        else:
            _run_job_timed(
                _per_process_db,
                job_id,
                builder_fn,
                config,
                keys_to_job_ids,
                start_time,
                cpt,
            )
        return job_id
    except Exception as exception:
        t = JobError(job_id, str(exception), traceback.format_exc())
        if _per_process_db:
            _per_process_db.set_error(
                job_id,
                t.message(),
                time.time() - start_time,
                cpt.get_bytes() if cpt else None,
            )
        return t
