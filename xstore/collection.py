
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
    value = collection.collection_config.build_fn(config)

    entry = Entry(collection, config, value, datetime.now())
    # TODO: Save into DB!!
    return entry


class Collection:

    def __init__(self, runtime, collection_config: CollectionConfig):
        self.runtime = runtime
        self.collection_config = collection_config

    def ref(self, config):
        assert isinstance(config, Obj)
        return Ref(self, config)

    def compute(self, configs):
        if isinstance(configs, Obj):
            configs = (configs,)

        build_fn = self.collection_config.build_fn

        tasks = []
        for config in configs:
            # TODO: Check if not in DB!!
            tasks.append(Task(entry_builder, (self, config), ()))

        return self.runtime.executor.run(tasks)