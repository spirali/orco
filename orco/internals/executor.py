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

    def _init(self, plan):
        consumers = {}
        waiting_deps = {}
        ready = []
        for plan_node in plan.nodes:
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
        return consumers, waiting_deps, ready

    def _submit_job(self, plan_node):
        job_setup = plan_node.job_setup
        if job_setup is None:
            runner_name = "local"
        else:
            runner_name = job_setup.runner_name
        runner = self.runners.get(runner_name)
        if runner is None:
            raise Exception(
                "Task '{}/{}' asked for unknown runner '{}'".format(
                    plan_node.builder_name, plan_node.config, runner_name
                )
            )
        return runner.submit(self.runtime, plan_node)

    def run(self, plan, verbose):
        nodes_by_id = {pn.job_id: pn for pn in plan.nodes}
        consumers, waiting_deps, ready = self._init(plan)
        waiting = [self._submit_job(pn) for pn in ready]
        del ready

        if verbose:
            progressbar = tqdm.tqdm(total=len(plan.nodes))
        else:
            progressbar = None
        unprocessed = []
        try:
            while waiting:
                wait_result = wait(
                    waiting,
                    return_when=FIRST_COMPLETED,
                    timeout=1 if unprocessed else None,
                )
                waiting = wait_result.not_done
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
                            waiting.add(self._submit_job(c))
        finally:
            if progressbar:
                progressbar.close()
            for f in waiting:
                f.cancel()
