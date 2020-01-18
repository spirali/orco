import logging
import threading
import time

from concurrent.futures import wait, FIRST_COMPLETED
from datetime import datetime

import tqdm
import platform

from orco.internals.runner import LocalProcessRunner, JobFailure
from orco.internals.job import Job

from orco.task import TaskKey
from orco.report import Report

logger = logging.getLogger(__name__)


class JobFailedException(Exception):
    """Exception thrown when job in an executor failed"""
    pass


def _heartbeat(runtime, id, event, heartbeat_interval):
    while not event.is_set():
        runtime.db.update_heartbeat(id)
        time.sleep(heartbeat_interval)


class Executor:
    """
    Executor that performs computations locally.

    All methods of executor are considered internal. Users should use it only
    via runtime.

    User may decrease heartbeat_interval (in seconds) if faster detection of
    hard-crash of executor is desired. But it should not be lesser than 1s
    otherwise performance issues may raises because of hammering DB. Note that
    it serves only for detecting crashes when whole Python interpreter is lost,
    it is not needed for normal exceptions.

    Executor spawns LocalProcessRunner as default. By default it spawns at most N
    build functions where N is number of cpus of the local machine. This can be
    configured via argument `n_processes` in the constructor.
    """

    _debug_do_not_start_heartbeat = False

    def __init__(self, runtime, runners=None, name=None, heartbeat_interval=7, n_processes=None):
        self.name = name or "unnamed"
        self.hostname = platform.node() or "unknown"
        self.created = None
        self.id = None
        self.runtime = runtime
        self.stats = {}
        assert heartbeat_interval >= 1
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_thread = None
        self.heartbeat_stop_event = None

        self.n_processes = n_processes

        if runners is None:
            runners = {}
        else:
            runners = runners.copy()

        self.runners = runners
        if "local" not in self.runners:
            runners["local"] = LocalProcessRunner(n_processes)

        self.resources = ",".join("{} ({})".format(name, r.get_resources()) for name, r in runners.items())

    def get_stats(self):
        return self.stats

    def stop(self):
        self.runtime.db.stop_executor(self.id)
        if self.heartbeat_stop_event:
            self.heartbeat_stop_event.set()
        for runner in self.runners.values():
            runner.stop()
        self.runtime = None

    def start(self):
        assert self.runtime
        assert self.id is None
        assert self.created is None

        self.created = datetime.now()
        self.runtime.db.register_executor(self)
        assert self.id is not None

        for runner in self.runners.values():
            runner.start()

        if not self._debug_do_not_start_heartbeat:
            self.heartbeat_stop_event = threading.Event()
            self.heartbeat_thread = threading.Thread(
                target=_heartbeat,
                args=(self.runtime, self.id, self.heartbeat_stop_event, self.heartbeat_interval))
            self.heartbeat_thread.daemon = True
            self.heartbeat_thread.start()

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

    def _submit_job(self, job):
        job_setup = job.job_setup
        if job_setup is None:
            runner_name = "local"
        else:
            runner_name = job_setup.get("runner", "local")
        runner = self.runners.get(runner_name)
        if runner is None:
            raise Exception("Task '{}' asked for runner unknown runner '{}'".format(job.task.task_key, runner_name))
        return runner.submit(self.runtime, job)

    def run(self, all_jobs, continue_on_error):

        def process_unprocessed():
            logging.debug("Writing into db: %s", unprocessed)
            db.set_entry_values(self.id, unprocessed, self.stats, pending_reports)
            if pending_reports:
                del pending_reports[:]
            for raw_entry in unprocessed:
                job = all_jobs[TaskKey(raw_entry.builder_name, raw_entry.key)]
                for c in consumers.get(job, ()):
                    waiting_deps[c] -= 1
                    w = waiting_deps[c]
                    if w <= 0:
                        assert w == 0
                        waiting.add(self._submit_job(c))

        self.stats = {"n_jobs": len(all_jobs), "n_completed": 0}

        pending_reports = []
        all_jobs = {task.task_key(): job for (task, job) in all_jobs.items()}
        db = self.runtime.db
        db.update_stats(self.id, self.stats)
        consumers, waiting_deps, ready = self._init(all_jobs.values())
        waiting = [self._submit_job(job) for job in ready]
        del ready

        progressbar = tqdm.tqdm(total=len(all_jobs))
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
                    if isinstance(result, JobFailure):
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
            db.set_entry_values(self.id, unprocessed, self.stats, pending_reports)
            return errors
        finally:
            progressbar.close()
            for f in waiting:
                f.cancel()