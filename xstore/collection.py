
from collections import namedtuple

from .obj import Obj
from .entry import Entry
from .task import Task
from .ref import Ref


class Collection:

    def __init__(self, runtime, name: str, build_fn, dep_fn):
        self.runtime = runtime
        self.name = name
        self.build_fn = build_fn
        self.dep_fn = dep_fn

    def ref(self, config):
        return Ref(self, config)

    def compute(self, config):
        return self.compute_many([config])[0]

    def get_entry(self, config):
        return self.runtime.db.get_entry_by_config(self, config)

    def has_entry(self, config):
        return self.runtime.db.has_entry_by_key(self, self.make_key(config))

    def remove(self, config):
        return self.runtime.db.remove_entry_by_key(self, self.make_key(config))

    def remove_many(self, configs):
        return self.runtime.db.remove_entries(
            ((self.name, self.make_key(config)) for config in configs))

    def compute_many(self, configs):
        #if isinstance(configs, Obj):
        #    configs = (configs,)

        def make_task(ref):
            ref_key = ref.ref_key()
            task = tasks.get(ref_key)
            if task is not None:
                return task
            collection = ref.collection
            is_computed = collection.has_entry(ref.config)
            if not is_computed and self.dep_fn:
                deps = self.dep_fn(ref.config)
                for r in deps:
                    assert isinstance(r, Ref)
                inputs = [make_task(r) for r in self.dep_fn(ref.config)]
            else:
                inputs = None
            task = Task(ref, inputs, is_computed)
            tasks[ref_key] = task
            return task

        tasks = {}
        requested_tasks = [make_task(self.ref(config)) for config in configs]
        return self.runtime.executor.run(requested_tasks)

    def make_key(self, config):
        return str(config)  # TODO: Improve this