import collections

# Value is serialized
RawEntry = collections.namedtuple(
    "RawEntry", ["collection_name", "key", "config", "value", "value_repr", "comp_time"])