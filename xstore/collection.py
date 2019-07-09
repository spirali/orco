
from collections import namedtuple
from datetime import datetime

from .obj import Obj
from .entry import Entry
from .executor import Task

Ref = namedtuple("Ref", ["collection", "config"])


class CollectionConfig:

    def __init__(self, name, build_fn, dep_fn):
        self.name = name
        self.dep_fn = dep_fn
        self.build_fn = build_fn


def entry_builder(collection, config):
    value = collection.build_fn(config)

    entry = Entry(collection, config, value, datetime.now())
    collection.runtime.db.create_entry(entry)
    return entry


class Collection:

    def __init__(self, runtime, name: str, build_fn, dep_fn):
        self.runtime = runtime
        self.name = name
        self.dep_fn = dep_fn
        self.build_fn = build_fn

    def ref(self, config):
        assert isinstance(config, Obj)
        return Ref(self, config)

    def compute_one(self, config):
        return self.compute([config])[0]

    def compute(self, configs):
        #if isinstance(configs, Obj):
        #    configs = (configs,)

        tasks = []
        result = []
        indices = []
        for config in configs:
            entry = self.runtime.db.get_entry_by_config(self, config)
            if entry is not None:
                result.append(entry)
            else:
                tasks.append(Task(entry_builder, (self, config), ()))
                indices.append(len(result))
                result.append(None)

        if tasks:
            output = self.runtime.executor.run(tasks)
            for i, o in zip(indices, output):
                result[i] = o
        return result

    def make_key(self, config):
        return str(config)  # TODO: Improve this