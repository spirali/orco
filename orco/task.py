from collections import Iterable, namedtuple


class Task:
    """
    Task is a pair Builder + a configuration

    Public interface for creating tasks are methods "task" and "tasks" on Builder

    >>> builder.task(config)
    """

    __slots__ = ["builder_name", "config", "key"]

    def __init__(self, builder_name, config):
        self.builder_name = builder_name
        self.config = config
        self.key = make_key(config)

    def __eq__(self, other):
        if not isinstance(other, Task):
            return False
        if self.key != other.key:
            return False
        return self.builder_name == other.builder_name

    def __hash__(self):
        return hash((self.builder_name, self.key))

    def __repr__(self):
        return "<{}/{}>".format(self.builder_name, repr(self.config))

    def task_key(self):
        return TaskKey(self.builder_name, self.key)


def _make_key_helper(obj, stream):
    if isinstance(obj, str) or isinstance(obj, int) or isinstance(obj, float):
        stream.append(repr(obj))
    elif isinstance(obj, list) or isinstance(obj, tuple):
        stream.append("[")
        for value in obj:
            _make_key_helper(value, stream)
            stream.append(",")
        stream.append("]")
    elif isinstance(obj, dict):
        stream.append("{")
        for key, value in sorted(obj.items()):
            if not isinstance(key, str):
                raise Exception("Invalid key in config: '{}', type: {}".format(
                    repr(key), type(key)))
            if key.startswith("_"):
                continue
            stream.append(repr(key))
            stream.append(":")
            _make_key_helper(value, stream)
            stream.append(",")
        stream.append("}")
    else:
        raise Exception("Invalid item in config: '{}', type: {}".format(repr(obj), type(obj)))


def make_key(config):
    stream = []
    _make_key_helper(config, stream)
    return "".join(stream)


TaskKey = namedtuple("TaskKey", ("builder_name", "key"))