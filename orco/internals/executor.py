from datetime import datetime
from .job import Job


class Executor:

    def __init__(self, executor_type, version, resources, heartbeat_interval):
        self.executor_type = executor_type
        self.version = version
        self.created = datetime.now()
        self.id = None
        self.runtime = None
        self.resources = resources
        self.stats = {}
        assert heartbeat_interval >= 1
        self.heartbeat_interval = heartbeat_interval

    def get_stats(self):
        raise NotImplementedError

    def run(self, jobs: [Job]):
        raise NotImplementedError

    def start(self):
        pass

    def stop(self):
        pass
