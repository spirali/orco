from .task import Task


class Builder:
    """
    Builder - a factory for a task (a pair of builder and config)
    """

    def __init__(self, name):
        self.name = name

    def task(self, config):
        """Create a task"""
        return Task(self.name, config)

    def tasks(self, configs):
        """Create more tasks at once"""
        name = self.name
        return [Task(name, config) for config in configs]
