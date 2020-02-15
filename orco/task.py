from collections import Iterable, namedtuple


class Task:
    """
    Task is a pair Builder + a configuration

    Public interface for creating tasks are methods "task" and "tasks" on Builder
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
        return EntryKey(self.builder_name, self.key)


