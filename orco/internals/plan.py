from .context import _CONTEXT
from .database import JobState
import collections
from .utils import format_time
from ..entry import EntryKey


class PlanNode:

    __slots__ = ("builder_name", "key", "config", "job_setup", "job_id", "inputs", "existing_dep_ids")

    def __init__(self, builder_name, key, config, job_setup, inputs, existing_dep_ids):
        self.builder_name = builder_name
        self.key = key
        self.config = config
        self.job_setup = job_setup
        self.inputs = inputs
        self.existing_dep_ids = existing_dep_ids
        self.job_id = None

    def make_entry_key(self):
        return EntryKey(self.builder_name, self.key)


class Plan:

    def __init__(self, leaf_entries, continue_on_error):
        self.leaf_entries = leaf_entries
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

    def create(self, runtime):
        conflicts = set()
        self.conflicts = conflicts
        nodes = {}
        self._nodes = nodes
        existing_jobs = self.existing_jobs
        error_keys = self.error_keys

        assert not hasattr(_CONTEXT, "on_entry") or _CONTEXT.on_entry is None

        def traverse(entry):
            entry_key = entry.make_entry_key()
            job_id = existing_jobs.get(entry_key)
            if job_id:
                return job_id
            if entry_key in conflicts or (error_keys and entry_key in error_keys):
                return None
            job_node = nodes.get(entry_key)
            if job_node is not None:
                return job_node
            builder = runtime.get_builder(entry.builder_name)
            job_id, state = runtime.db.get_entry_job_id_and_state(entry.builder_name, entry.key)
            if state == JobState.FINISHED:
                assert isinstance(job_id, int)
                existing_jobs[entry_key] = job_id
                return job_id
            if state == JobState.ANNOUNCED or state == JobState.RUNNING:
                conflicts.add(entry_key)
                return None
            assert job_id is None

            if builder.fn is None:
                raise Exception(
                    "Computation depends on a missing configuration '{}' in a fixed builder"
                        .format(entry))

            deps = []
            try:
                _CONTEXT.on_entry = deps.append
                builder.run_with_config(entry.config, only_deps=True)
            finally:
                _CONTEXT.on_entry = None
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
            plan_node = PlanNode(entry.builder_name,
                                 entry.key,
                                 entry.config,
                                 builder._create_job_setup(entry.config),
                                 unfinished_inputs,
                                 existing_ids)
            nodes[entry_key] = plan_node
            return plan_node

        for entry in self.leaf_entries:
            traverse(entry)

    def fill_job_ids(self, runtime):
        for entry in self.leaf_entries:
            key = entry.make_entry_key()
            node = self._nodes.get(key)
            if node:
                job_id = node.job_id
            else:
                job_id = self.existing_jobs.get(key)
            if job_id:
                entry.set_job_id(job_id, runtime.db)

    def print_report(self, runtime):
        jobs_per_builder = collections.Counter([pn.builder_name for pn in self.nodes])
        print("Scheduled jobs   |     # | Expected comp. time (per entry)\n"
              "-----------------+-------+--------------------------------")
        for col, count in sorted(jobs_per_builder.items()):
            stats = runtime.db.get_run_stats(col)
            if stats["avg"] is None:
                print("{:<17}| {:>5} | N/A".format(col, count))
            else:
                print("{:<17}| {:>5} | {:>8} +- {}".format(col, count, format_time(stats["avg"]),
                                                           format_time(stats["stdev"])))
        print("-----------------+-------+--------------------------------")
