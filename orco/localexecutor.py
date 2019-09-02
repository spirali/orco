import logging
import multiprocessing
import threading
import time

from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime
from collections import namedtuple

import cloudpickle
import tqdm
import traceback

from .internals.joboptions import JobOptions
from .internals.tasktools import resolve_task_keys, task_to_taskkey, collect_task_keys
from .internals.db import DB
from .internals.job import Job
from .internals.executor import Executor

from .task import TaskKey
from .report import Report

logger = logging.getLogger(__name__)


class JobFailedException(Exception):
    """Exception thrown when job in an executor failed"""
    pass


class _JobFailure:

    def __init__(self, task_key):
        self.task_key = task_key

    def message(self):
        raise NotImplementedError()

    def report_type(self):
        raise NotImplementedError()


class _JobError(_JobFailure):

    def __init__(self, task_key, exception_str, traceback):
        super().__init__(task_key)
        self.exception_str = exception_str
        self.traceback = traceback

    def message(self):
        return "Job failed: {}\n{}".format(self.exception_str, self.traceback)

    def report_type(self):
        return "error"


class _JobTimeout(_JobFailure):

    def __init__(self, task_key, timeout):
        super().__init__(task_key)
        self.timeout = timeout

    def message(self):
        return "Job timeouted after {} seconds".format(self.timeout)

    def report_type(self):
        return "timeout"


def _heartbeat(runtime, id, event, heartbeat_interval):
    while not event.is_set():
        runtime.db.update_heartbeat(id)
        time.sleep(heartbeat_interval)


class LocalExecutor(Executor):
    """
    Executor that performs computations locally.

    Executor spawns build functions in parallel. By default it spawns at most N
    build functions where N is number of cpus of the local machine. This can be
    configured via argument `n_processes` in the constructor.

    All methods of executor are considered internal. Users should use it only
    via Runtime by method `register_executor`.

    User may decreate heatbeat_interval (in seconds) if faster detection of
    hard-crash of executor is desired. But it should not be lesser than 1s
    otherwise performance issues may raises because of hammering DB. Note that
    it serves only for detecting crashes when whole Python interpreter is lost,
    it is not needed for normal exceptions.
    """

    _debug_do_not_start_heartbeat = False

    def __init__(self, heartbeat_interval=7, n_processes=None):
        super().__init__("local", "0.0", "{} cpus".format(multiprocessing.cpu_count()),
                         heartbeat_interval)
        self.heartbeat_thread = None
        self.heartbeat_stop_event = None

        self.pool = None
        self.n_processes = n_processes

    def get_stats(self):
        return self.stats

    def stop(self):
        if self.heartbeat_stop_event:
            self.heartbeat_stop_event.set()
        self.runtime.unregister_executor(self)
        self.runtime = None

    def start(self):
        assert self.runtime
        assert self.id is not None

        if not self._debug_do_not_start_heartbeat:
            self.heartbeat_stop_event = threading.Event()
            self.heartbeat_thread = threading.Thread(
                target=_heartbeat,
                args=(self.runtime, self.id, self.heartbeat_stop_event, self.heartbeat_interval))
            self.heartbeat_thread.daemon = True
            self.heartbeat_thread.start()

        self.pool = ProcessPoolExecutor(max_workers=self.n_processes)

    def _init(self, jobs):
        consumers = {}
        waiting_deps = {}
        ready = []

        for job in jobs:
            count = 0
            for inp in job.inputs:
                if isinstance(inp, Job):
                    count += 1
                    c = consumers.get(inp)
                    if c is None:
                        c = []
                        consumers[inp] = c
                    c.append(job)
            if count == 0:
                ready.append(job)
            waiting_deps[job] = count
        return consumers, waiting_deps, ready

    def run(self, all_jobs, continue_on_error):

        def process_unprocessed():
            logging.debug("Writing into db: %s", unprocessed)
            db.set_entry_values(self.id, unprocessed, self.stats, pending_reports)
            if pending_reports:
                del pending_reports[:]
            for raw_entry in unprocessed:
                task_key = TaskKey(raw_entry.builder_name, raw_entry.key)
                job = all_jobs[task_key]
                #col_progressbars[task_key[0]].update()
                for c in consumers.get(job, ()):
                    waiting_deps[c] -= 1
                    w = waiting_deps[c]
                    if w <= 0:
                        assert w == 0
                        waiting.add(submit(c))

        def submit(job):
            task = job.task
            pickled_fns = pickle_cache.get(task.builder_name)
            if pickled_fns is None:
                builder = self.runtime._get_builder(job.task)
                pickled_fns = cloudpickle.dumps((builder.build_fn, builder.make_raw_entry))
                pickle_cache[task.builder_name] = pickled_fns
            return pool.submit(_run_job, db.path, pickled_fns, job.task.task_key(), job.task.config,
                               task_to_taskkey(job.dep_value))

        self.stats = {"n_jobs": len(all_jobs), "n_completed": 0}

        pending_reports = []
        all_jobs = {task.task_key(): job for (task, job) in all_jobs.items()}
        pickle_cache = {}
        pool = self.pool
        db = self.runtime.db
        db.update_stats(self.id, self.stats)
        consumers, waiting_deps, ready = self._init(all_jobs.values())
        waiting = [submit(job) for job in ready]
        del ready

        #col_progressbars = {}
        #for i, (col, count) in enumerate(jobs_per_builder.items()):
        #    col_progressbars[col] = tqdm.tqdm(desc=col, total=count, position=i)

        progressbar = tqdm.tqdm(total=len(all_jobs))  #  , position=i+1)
        unprocessed = []
        last_write = time.time()
        errors = []
        try:
            while waiting:
                wait_result = wait(
                    waiting, return_when=FIRST_COMPLETED, timeout=1 if unprocessed else None)
                waiting = wait_result.not_done
                for f in wait_result.done:
                    self.stats["n_completed"] += 1
                    progressbar.update()
                    result = f.result()
                    if isinstance(result, _JobFailure):
                        task_key = result.task_key
                        config = db.get_config(task_key[0], task_key[1])
                        message = result.message()
                        report = Report(
                            result.report_type(),
                            self.id,
                            message,
                            builder_name=task_key[0],
                            config=config)
                        if continue_on_error:
                            errors.append(task_key)
                            pending_reports.append(report)
                        else:
                            db.insert_report(report)

                            raise JobFailedException("{} ({}/{})".format(
                                message, result.task_key[0], repr(result.task_key[1])))
                        continue
                    logger.debug("Job finished: %s/%s", result.builder_name, result.key)
                    unprocessed.append(result)
                if unprocessed and (not waiting or time.time() - last_write > 1):
                    process_unprocessed()
                    unprocessed = []
                    last_write = time.time()
            #    db.update_stats(self.id, self.stats)
            #for p in col_progressbars.values():
            #    p.close()
            db.set_entry_values(self.id, unprocessed, self.stats, pending_reports)
            return errors
        finally:
            progressbar.close()
            for f in waiting:
                f.cancel()


_per_process_db = None


def _run_job(db_path, fns, task_key, config, dep_value):
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
                finalize_fn(task_key.builder_name, task_key.key, None, value,
                            end_time - start_time))

        options = JobOptions.parse_from_config(config)
        if options.timeout:
            thread = threading.Thread(target=run)
            thread.daemon = True
            thread.start()
            thread.join(options.timeout)
            if thread.is_alive():
                return _JobTimeout(task_key, options.timeout)
        else:
            run()
        return result[0]
    except Exception as exception:
        return _JobError(task_key, str(exception), traceback.format_exc())
