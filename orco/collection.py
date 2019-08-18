from collections import namedtuple
from datetime import datetime
import pickle

from .entry import Entry, RawEntry
from .task import Task
from .ref import Ref


def default_make_raw_entry(collection_name, key, config, value, comp_time):
    value_repr = repr(value)
    if len(value_repr) > 85:
        value_repr = value_repr[:80] + " ..."
    if config is not None:
        config = pickle.dumps(config)
    return RawEntry(collection_name, key, config, pickle.dumps(value), value_repr, comp_time)


class Collection:

    def __init__(self, name: str, build_fn, dep_fn):
        self.name = name
        self.build_fn = build_fn
        self.make_raw_entry = default_make_raw_entry
        self.dep_fn = dep_fn


class CollectionRef:

    def __init__(self, name):
        self.name = name

    def ref(self, config):
        return Ref(self.name, config)

    def refs(self, configs):
        name = self.name
        return [Ref(name, config) for config in configs]
