import collections
import logging
import threading
import pickle
import time

from .collection import CollectionRef
from .internals.db import DB
from .internals.executor import Executor
from .internals.task import Task
from .internals.rawentry import RawEntry
from .ref import make_key
from .report import Report
from .internals.reftools import collect_refs, resolve_refs
from .internals.utils import format_time

logger = logging.getLogger(__name__)


def _default_make_raw_entry(collection_name, key, config, value, comp_time):
    value_repr = repr(value)
    if len(value_repr) > 85:
        value_repr = value_repr[:80] + " ..."
    if config is not None:
        config = pickle.dumps(config)
    return RawEntry(collection_name, key, config, pickle.dumps(value), value_repr, comp_time)


class _Collection:

    def __init__(self, name: str, build_fn, dep_fn):
        self.name = name
        self.build_fn = build_fn
        self.make_raw_entry = _default_make_raw_entry
        self.dep_fn = dep_fn


class Runtime:
    """
    Core class of ORCO.

    It manages database with results and starts computations

    >>> runtime = Runtime("/path/to/dbfile.db")
    """

    def __init__(self, db_path, executor: Executor = None):
        self.db = DB(db_path)
        self.db.init()

        self._executor = executor
        self._collections = {}
        self._lock = threading.Lock()

        self.executors = []
        self.stopped = False

        logging.debug("Starting runtime %s (db=%s)", self, db_path)

        if executor:
            self.register_executor(executor)

    def __enter__(self):
        self._check_stopped()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.stopped:
            self.stop()

    def stop(self):
        self._check_stopped()

        logger.debug("Stopping runtime %s", self)
        for executor in self.executors:
            logger.debug("Stopping executor %s", executor)
            executor.stop()
        self.stopped = True

    def register_executor(self, executor):
        logger.debug("Registering executor %s", executor)
        executor.runtime = self
        self.db.register_executor(executor)
        executor.start()
        self.executors.append(executor)

    def unregister_executor(self, executor):
        logger.debug("Unregistering executor %s", executor)
        self.executors.remove(executor)
        self.db.stop_executor(executor.id)

    def register_collection(self, name, build_fn=None, dep_fn=None):
        with self._lock:
            if name in self._collections:
                raise Exception("Collection already registered")
            self.db.ensure_collection(name)
            collection = _Collection(name, build_fn=build_fn, dep_fn=dep_fn)
            self._collections[name] = collection
        return CollectionRef(name)

    def serve(self, port=8550, debug=False, testing=False, nonblocking=False):
        from .internals.browser import init_service
        app = init_service(self)
        if testing:
            app.testing = True
            return app
        else:

            def run_app():
                app.run(port=port, debug=debug, use_reloader=False)

            if nonblocking:
                t = threading.Thread(target=run_app)
                t.daemon = True
                t.start()
                return t
            else:
                run_app()

    def get_entry_state(self, ref):
        return self.db.get_entry_state(ref.collection_name, ref.key)

    def get_entry(self, ref, include_announced=False):
        entry = self.db.get_entry_no_config(ref.collection_name, ref.key, include_announced)
        if entry is not None:
            entry.config = ref.config
        return entry

    def get_entries(self, refs, include_announced=False, drop_missing=False):
        results = [self.get_entry(ref, include_announced) for ref in refs]
        if drop_missing:
            results = [entry for entry in results if entry is not None]
        return results

    def remove(self, ref, remove_inputs=False):
        return self.remove_many([ref], remove_inputs)

    def remove_many(self, refs, remove_inputs=False):
        if remove_inputs:
            self.db.invalidate_entries_by_key(refs)
        else:
            self.db.remove_entries_by_key(refs)

    def insert(self, ref, value):
        collection = self._collections[ref.collection_name]
        entry = collection.make_raw_entry(collection.name, make_key(ref.config), ref.config, value,
                                          None)
        self.db.create_entries((entry, ))

    def clean(self, collection_ref):
        self.db.clean_collection(collection_ref.name)

    def compute(self, obj, executor=None, continue_on_error=False):
        results = self._compute_refs(collect_refs(obj), executor, continue_on_error)
        return resolve_refs(obj, results)

    def get_reports(self, count=100):
        return self.db.get_reports(count)

    def upgrade_collection(self, collection, upgrade_fn):
        configs = self.db.get_all_configs(collection.name)
        to_update = []
        collection_name = collection.name
        keys = set()
        for config in configs:
            key = make_key(config)
            config = upgrade_fn(config)
            new_key = make_key(config)
            if new_key in keys:
                raise Exception("Key collision in upgrade, config={}".format(repr(config)))
            if new_key != key:
                to_update.append((collection_name, key, new_key, pickle.dumps(config)))
            keys.add(new_key)
        self.db.upgrade_collection(collection.name, to_update)

    def _collection_summaries(self):
        return self.db.collection_summaries()

    def _entry_summaries(self, collection_name):
        return self.db.entry_summaries(collection_name)

    def _executor_summaries(self):
        return self.db.executor_summaries()

    def _get_collection(self, ref):
        return self._collections[ref.collection_name]

    def _create_compute_tree(self, refs, exists, errors):
        tasks = {}
        global_deps = set()
        conflicts = set()

        def make_task(ref):
            if ref in exists:
                return ref
            if ref in conflicts or (errors and ref in errors):
                return None
            task = tasks.get(ref)
            if task is not None:
                return task
            collection = self._get_collection(ref)
            state = self.db.get_entry_state(ref.collection_name, ref.key)
            if state == "finished":
                exists.add(ref)
                return ref
            if state == "announced":
                conflicts.add(ref)
                return None
            if state is None and collection.dep_fn:
                dep_value = collection.dep_fn(ref.config)
                dep_refs = collect_refs(dep_value)
                inputs = [make_task(r) for r in dep_refs]
                if any(inp is None for inp in inputs):
                    return None
                for r in dep_refs:
                    global_deps.add((r, ref))
            else:
                inputs = ()
                dep_value = None
            if state is None and collection.build_fn is None:
                raise Exception(
                    "Computation depends on a missing configuration '{}' in a fixed collection"
                    .format(ref))
            task = Task(ref, inputs, dep_value)
            tasks[ref] = task
            return task

        for ref in refs:
            make_task(ref)

        return tasks, global_deps, len(conflicts)

    def _check_stopped(self):
        if self.stopped:
            raise Exception("Runtime was already stopped")

    def _print_report(self, tasks):
        tasks_per_collection = collections.Counter([t.ref.collection_name for t in tasks.values()])
        print("Scheduled tasks  |     # | Expected comp. time (per entry)\n"
              "-----------------+-------+--------------------------------")
        for col, count in sorted(tasks_per_collection.items()):
            stats = self.db.get_run_stats(col)
            if stats["avg"] is None:
                print("{:<17}| {:>5} | N/A".format(col, count))
            else:
                print("{:<17}| {:>5} | {:>8} +- {}".format(col, count, format_time(stats["avg"]),
                                                           format_time(stats["stdev"])))
        print("-----------------+-------+--------------------------------")

    def _run_computation(self, refs, executor, exists, errors):
        tasks, global_deps, n_conflicts = self._create_compute_tree(refs, exists, errors)
        if not tasks and n_conflicts == 0:
            return "finished"
        need_to_compute_refs = [task.ref for task in tasks.values()]
        if not need_to_compute_refs:
            print("Waiting for finished computation on another executor ...")
            return "wait"
        logger.debug("Announcing refs %s at executor %s", need_to_compute_refs, executor.id)
        if not self.db.announce_entries(
                executor.id, need_to_compute_refs, global_deps,
                Report("info", executor.id, "Computing {} task(s)".format(
                    len(need_to_compute_refs)))):
            return "wait"
        try:
            # we do not this anymore, and .run may be long
            del global_deps, need_to_compute_refs
            if n_conflicts:
                print(
                    "Some computation was temporarily skipped as they depends on tasks computed by another executor"
                )
            self._print_report(tasks)
            new_errors = executor.run(tasks, errors is not None)
            if errors is not None:
                errors.update(new_errors)
            else:
                assert not new_errors
            if n_conflicts == 0:
                return "finished"
            else:
                return "next"
        except:
            self.db.unannounce_entries(executor.id, list(tasks))
            raise

    def _compute_refs(self, refs, executor=None, continue_on_error=False):
        exists = set()
        if continue_on_error:
            errors = set()
        else:
            errors = None
        if executor is None:
            if len(self.executors) == 0:
                raise Exception("No executors registered")
            executor = self.executors[0]

        while True:
            status = self._run_computation(refs, executor, exists, errors)
            if status == "finished":
                break
            elif status == "next":
                continue
            elif status == "wait":
                time.sleep(1)
                continue
            else:
                assert 0
        if errors:
            print("During computation, {} errors occured, see reports for details".format(
                len(errors)))
        return {ref: self.get_entry(ref) for ref in refs}
