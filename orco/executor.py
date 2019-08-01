import multiprocessing
import threading
import time
import cloudpickle
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime


from .collection import Entry
from .task import Task
from .db import DB


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


def compute_task(args):
    build_fn, config, has_input, input_entries = args

    if has_input:
        return build_fn(config, input_entries)
    else:
        return build_fn(config)


class LocalExecutor(Executor):

    _debug_do_not_start_heartbeat = False

    def __init__(self, heartbeat_interval=5, n_processes=None):
        super().__init__("local", "0.0", "{} cpus".format(multiprocessing.cpu_count()),
                         heartbeat_interval)
        self.heartbeat_thread = None
        self.heartbeat_stop_event = None

        self.pool = None
        self.n_processes = n_processes

    def get_stats(self):
        return self.stats

    def stop(self):
        if self.heartbeat_stop_event:
            self.heartbeat_stop_event.set()
        self.runtime.unregister_executor(self)
        self.runtime = None

    def start(self):
        assert self.runtime
        assert self.id is not None

        if not self._debug_do_not_start_heartbeat:
            self.heartbeat_stop_event = threading.Event()
            self.heartbeat_thread = threading.Thread(target=heartbeat,
                                                     args=(self.runtime, self.id,
                                                           self.heartbeat_stop_event,
                                                           self.heartbeat_interval))
            self.heartbeat_thread.daemon = True
            self.heartbeat_thread.start()

    def _init(self, tasks):
        consumers = {}
        waiting_deps = {}
        ready = []

        for task in tasks:
            count = 0
            if task.inputs is not None:
                for inp in task.inputs:
                    if isinstance(inp, Task):
                        count += 1
                        c = consumers.get(inp)
                        if c is None:
                            c = set()
                            consumers[inp] = c
                        c.add(task)
            if count == 0:
                ready.append(task)
            waiting_deps[task] = count
        return consumers, waiting_deps, ready

    def run(self, all_tasks, required_tasks: [Task]):
        def submit(task):
            collection = task.ref.collection
            if task.inputs is not None:
                inputs = [t.ref.ref_key() if isinstance(t, Task) else t.ref_key() for t in task.inputs]
            else:
                inputs = None
            return pool.submit(_run_task,
                               self.id,
                               db.path,
                               cloudpickle.dumps(collection.build_fn),
                               task.ref.ref_key(),
                               task.ref.config,
                               inputs)
        self.stats = {
            "n_tasks": len(all_tasks),
            "n_completed": 0
        }
        db = self.runtime.db
        db.update_stats(self.id, self.stats)
        consumers, waiting_deps, ready = self._init(all_tasks.values())
        pool = self.pool
        if pool is None:
            pool = ProcessPoolExecutor(max_workers=self.n_processes)
            self.pool = pool

        waiting = [submit(task) for task in ready]
        del ready

        while waiting:
            wait_result = wait(waiting, None, return_when=FIRST_COMPLETED)
            waiting = wait_result.not_done
            for f in wait_result.done:
                self.stats["n_completed"] += 1
                task = all_tasks[f.result()]
                for c in consumers.get(task, ()):
                    waiting_deps[c] -= 1
                    w = waiting_deps[c]
                    if w <= 0:
                        assert w == 0
                        waiting.add(submit(c))
            db.update_stats(self.id, self.stats)
        return [self.runtime.get_entry(task.ref if isinstance(task, Task) else task) for task in required_tasks]


_per_process_db = None


def _run_task(executor_id, db_path, build_fn, ref_key, config, deps):
    global _per_process_db
    if _per_process_db is None:
        _per_process_db = DB(db_path, threading=False)
    build_fn = cloudpickle.loads(build_fn)
    if deps is not None:
        value_deps = [_per_process_db.get_entry(*ref) for ref in deps]
        value = build_fn(config, value_deps)
    else:
        value = build_fn(config)
    entry = Entry(config, value, datetime.now())
    _per_process_db.set_entry_value(executor_id, ref_key[0], ref_key[1], entry)
    return ref_key