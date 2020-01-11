import collections

# Value is serialized
RawEntry = collections.namedtuple(
    "RawEntry", ["builder_name", "key", "config", "value", "value_repr", "job_setup", "comp_time"])