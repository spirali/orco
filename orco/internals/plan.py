import collections

from .context import _CONTEXT
from .database import JobState
from .utils import format_time


class PlanNode:

    __slots__ = (
        "builder_name",
        "key",
        "config",
        "job_setup",
        "job_id",
        "inputs",
        "existing_dep_ids",
    )

    def __init__(self, builder_name, key, config, job_setup, inputs, existing_dep_ids):
        self.builder_name = builder_name
        self.key = key
        self.config = config
        self.job_setup = job_setup
        self.inputs = inputs
        self.existing_dep_ids = existing_dep_ids
        self.job_id = None


class Plan:
    def __init__(self, leaf_jobs, continue_on_error):
        self.leaf_jobs = leaf_jobs
        self.existing_jobs = {}
        self.continue_on_error = continue_on_error
        if continue_on_error:
            self.error_keys = set()
        else:
            self.error_keys = None
        self._nodes = None
        self.conflicts = None

    def is_finished(self):
        return not self.nodes and not self.conflicts

    def need_wait(self):
        return not self.nodes and self.conflicts

    @property
    def nodes(self):
        return self._nodes.values()

    def _create_for_testing(self):
        self._nodes = {}
        for job in self.leaf_jobs:
            plan_node = PlanNode(job.builder_name, job.key, job.config, "XXX", [], [])
            self._nodes[job.key] = plan_node

    def create(self, runtime):
        conflicts = set()
        self.conflicts = conflicts
        nodes = {}
        self._nodes = nodes
        existing_jobs = self.existing_jobs
        error_keys = self.error_keys

        assert not hasattr(_CONTEXT, "on_job") or _CONTEXT.on_job is None

        def traverse(job):
            key = job.key
            job_id = existing_jobs.get(key)
            if job_id:
                return job_id
            if key in conflicts or (error_keys and key in error_keys):
                return None
            plan_node = nodes.get(key)
            if plan_node is not None:
                return plan_node
            builder = runtime.get_builder(job.builder_name)
            job_id, state = runtime.db.get_active_job_id_and_state(job.key)
            if state == JobState.FINISHED:
                assert isinstance(job_id, int)
                existing_jobs[key] = job_id
                return job_id
            elif state == JobState.ANNOUNCED or state == JobState.RUNNING:
                conflicts.add(key)
                return None
            elif state == JobState.FREED:
                raise Exception(
                    "Computation depends on a job in freed state ({}). "
                    "You need to drop or archive the job to run the computation".format(
                        job
                    )
                )
            assert job_id is None

            if builder.fn is None:
                raise Exception(
                    "Computation depends on a missing configuration '{}' in a fixed builder".format(
                        job
                    )
                )

            deps = []
            try:
                _CONTEXT.on_job = deps.append
                builder.run_with_config(job.config, only_deps=True)
            finally:
                _CONTEXT.on_job = None
            unfinished_inputs = []
            existing_ids = []
            for e in deps:
                j = traverse(e)
                if j is None:
                    return None
                if isinstance(j, int):
                    existing_ids.append(j)
                    continue
                unfinished_inputs.append(j)
            plan_node = PlanNode(
                job.builder_name,
                job.key,
                job.config,
                builder._create_job_setup(job.config),
                unfinished_inputs,
                existing_ids,
            )
            nodes[key] = plan_node
            return plan_node

        for job in self.leaf_jobs:
            traverse(job)

    def _testing_fill_job_ids(self, runtime):
        db = runtime.db
        for job in self.leaf_jobs:
            key = job.key
            job_id = self.existing_jobs.get(key)
            if job_id:
                job.set_job_id(job_id, db, JobState.FINISHED)

            node = self._nodes.get(key)
            if node:
                job.set_job_id(node.job_id, db, None)

    def fill_job_ids(self, runtime, set_finish):
        """
            if set_finish is True then all leaf nodes is set as finished
            if set_finish is False then state of leaf nodes is read from db
        """
        read_jobs = []
        db = runtime.db
        nodes = self._nodes
        for job in self.leaf_jobs:
            key = job.key
            job_id = self.existing_jobs.get(key)
            if job_id:
                job.set_job_id(job_id, db, JobState.FINISHED)

            node = nodes.get(key)
            if node:
                if set_finish:
                    job.set_job_id(node.job_id, db, JobState.FINISHED)
                else:
                    read_jobs.append(job)

        if read_jobs:
            job_ids = [nodes[job.key].job_id for job in read_jobs]
            state_map = db.get_states(job_ids)
            for job in read_jobs:
                job_id = nodes[job.key].job_id
                state = state_map.get(job_id)
                if state:
                    assert state == JobState.FINISHED or state == JobState.ERROR
                    job.set_job_id(job_id, db, state)

    def print_report(self, runtime):
        jobs_per_builder = collections.Counter([pn.builder_name for pn in self.nodes])
        print(
            "Scheduled jobs   |     # | Expected comp. time (per job)\n"
            "-----------------+-------+--------------------------------"
        )
        for col, count in sorted(jobs_per_builder.items()):
            stats = runtime.db.get_run_stats(col)
            if stats["avg"] is None:
                print("{:<17}| {:>5} | N/A".format(col, count))
            else:
                print(
                    "{:<17}| {:>5} | {:>8} +- {}".format(
                        col,
                        count,
                        format_time(stats["avg"]),
                        format_time(stats["stdev"]),
                    )
                )
        print("-----------------+-------+--------------------------------")
