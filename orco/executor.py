from typing import Union, Iterable

from .collection import Ref, Collection, Entry
from .task import Task
from datetime import datetime
import multiprocessing
import threading
import time


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

    def run(self, tasks: [Task]):
        raise NotImplementedError

    def start(self):
        pass

    def stop(self):
        pass


def heartbeat(runtime, id, event, heartbeat_interval):
    while not event.is_set():
        runtime.update_heartbeat(id)
        time.sleep(heartbeat_interval)

def gather_announcements(tasks):
    result = set()


class LocalExecutor(Executor):

    _debug_do_not_start_heartbeat = False

    def __init__(self, heartbeat_interval=5):
        super().__init__("local", "0.0", "{} cpus".format(multiprocessing.cpu_count()), heartbeat_interval)
        self.heartbeat_thread = None
        self.heartbeat_stop_event = None

    def get_stats(self):
        return {}

    def stop(self):
        if self.heartbeat_stop_event:
            self.heartbeat_stop_event.set()
        self.runtime.unregister_executor(self)
        self.runtime = None

    def start(self):
        if not self._debug_do_not_start_heartbeat:
            self.heartbeat_stop_event = threading.Event()
            self.heartbeat_thread = threading.Thread(target=heartbeat,
                                                    args=(self.runtime, self.id,
                                                        self.heartbeat_stop_event, self.heartbeat_interval))
            self.heartbeat_thread.daemon = True
            self.heartbeat_thread.start()

    def run_task(self, task, input_entries):
        ref = task.ref
        if task.is_computed:
            entry = ref.collection.get_entry(ref.config)
            assert entry is not None
            return entry

        collection = ref.collection
        if collection.dep_fn is None:
            value = collection.build_fn(ref.config)
        else:
            value = collection.build_fn(ref.config, input_entries)
        entry = Entry(collection, ref.config, value, datetime.now())
        collection.runtime.db.set_entry_value(self.id, entry)
        self.stats["n_completed"] += 1
        collection.runtime.db.update_stats(self.id, self.stats)
        return entry

    def run(self, all_tasks, required_tasks: [Task]):

        def run_helper(task):
            entry = cache.get(task)
            if entry is not None:
                return entry
            if task.inputs:
                inputs = [run_helper(task) for task in task.inputs]
            else:
                inputs = None
            entry = self.run_task(task, inputs)
            cache[task] = entry
            return entry

        n_tasks = sum(1 for t in all_tasks.values() if not t.is_computed)
        self.stats = {
            "n_tasks": n_tasks,
            "n_completed": 0
        }
        cache = {}
        return [run_helper(task) for task in required_tasks]