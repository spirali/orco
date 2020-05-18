import logging
import platform
from concurrent.futures import wait, FIRST_COMPLETED
from datetime import datetime

import tqdm

from orco.internals.runner import LocalProcessRunner, JobFailure

logger = logging.getLogger(__name__)


class JobFailedException(Exception):
    """Exception thrown when job in an executor failed"""

    pass


class Executor:
    """
    Executor that performs computations locally.

    All methods of executor are considered internal. Users should use it only
    via runtime.

    Executor spawns LocalProcessRunner as default. By default it spawns at most N
    build functions where N is number of cpus of the local machine. This can be
    configured via argument `n_processes` in the constructor.
    """

    def __init__(self, runtime, runners=None, name=None, n_processes=None):
        self.name = name or "unnamed"
        self.hostname = platform.node() or "unknown"
        self.created = None
        self.id = None
        self.runtime = runtime
        self.stats = {}
        self.n_processes = n_processes

        if runners is None:
            runners = {}
        else:
            runners = runners.copy()

        self.runners = runners
        if "local" not in self.runners:
            runners["local"] = LocalProcessRunner(n_processes)

        self.resources = ",".join(
            "{} ({})".format(name, r.get_resources()) for name, r in runners.items()
        )

    def get_stats(self):
        return self.stats

    def stop(self):
        # self.runtime.db.stop_executor(self.id)
        for runner in self.runners.values():
            runner.stop()
        self.runtime = None

    def start(self):
        assert self.runtime
        assert self.id is None
        assert self.created is None

        self.created = datetime.now()
        # self.runtime.db.register_executor(self)
        # assert self.id is not None

        for runner in self.runners.values():
            runner.start()

    def run(self, plan, verbose):
        ExecutorRun(self, plan, verbose).run()


class ExecutorRun:

    def __init__(self, executor, plan, verbose):
        self.executor = executor
        self.unprocessed = []
        self.unprocessed_exclusives = []
        self.exclusive_mode = False
        self.waiting = set()
        self.plan = plan
        self.verbose = verbose

    def start(self, plan_node):
        runner_name = plan_node.job_setup.runner_name
        runner = self.executor.runners.get(runner_name)
        if runner is None:
            raise Exception(
                "Task '{}/{}' asked for unknown runner '{}'".format(
                    plan_node.builder_name, plan_node.config, runner_name
                )
            )
        self.waiting.add(runner.submit(self.executor.runtime, plan_node))

    def init(self):
        consumers = {}
        waiting_deps = {}
        ready = []
        for plan_node in self.plan.nodes:
            if not plan_node.inputs:
                ready.append(plan_node)
            else:
                for inp in plan_node.inputs:
                    c = consumers.get(inp)
                    if c is None:
                        c = []
                        consumers[inp] = c
                    c.append(plan_node)
            waiting_deps[plan_node] = len(plan_node.inputs)
        for plan_node in ready:
            self.on_ready(plan_node)
        return consumers, waiting_deps

    def on_ready(self, plan_node):
        #self.start(plan_node)
        #return

        exclusive = plan_node.job_setup.exclusive
        if not exclusive:
            if not self.exclusive_mode:
                self.start(plan_node)
            else:
                self.unprocessed.append(plan_node)
        else:
            self.unprocessed_exclusives.append(plan_node)

    def check_waiting(self):
        if self.waiting:
            return True

        if self.unprocessed_exclusives:
            self.exclusive_mode = True
            self.start(self.unprocessed_exclusives.pop())
            return True

        if self.unprocessed:
            self.exclusive_mode = False
            for plan_node in self.unprocessed:
                self.start(plan_node)
            del self.unprocessed[:]
            return True
        return False

    def run(self):
        plan = self.plan
        nodes_by_id = {pn.job_id: pn for pn in plan.nodes}
        consumers, waiting_deps = self.init()

        if self.verbose:
            progressbar = tqdm.tqdm(total=len(plan.nodes))
        else:
            progressbar = None

        try:
            while self.check_waiting():
                wait_result = wait(
                    self.waiting,
                    return_when=FIRST_COMPLETED,
                    timeout=None,
                )
                self.waiting = wait_result.not_done
                for f in wait_result.done:
                    if progressbar:
                        progressbar.update()
                    result = f.result()
                    if isinstance(result, JobFailure):
                        pn = nodes_by_id[result.job_id]
                        message = result.message()
                        if plan.continue_on_error:
                            plan.error_keys.append(pn.key)
                        else:
                            raise JobFailedException(
                                "{} ({}/{})".format(
                                    message, pn.builder_name, repr(pn.config)
                                )
                            )
                        continue
                    pn = nodes_by_id[result]
                    logger.debug(
                        "Job %s finished: %s/%s", pn.job_id, pn.builder_name, pn.key
                    )
                    for c in consumers.get(pn, ()):
                        waiting_deps[c] -= 1
                        w = waiting_deps[c]
                        if w <= 0:
                            assert w == 0
                            self.on_ready(c)
        finally:
            if progressbar:
                progressbar.close()
            for f in self.waiting:
                f.cancel()