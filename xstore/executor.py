from typing import Union, Iterable

from .collection import Ref, Collection, Entry
from .task import Task
from datetime import datetime


class Executor:

    def run(self, tasks: [Task]):
        raise NotImplementedError


class LocalExecutor(Executor):

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
        collection.runtime.db.create_entry(entry)
        return entry

    def run(self, tasks: [Task]):

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

        cache = {}
        return [run_helper(task) for task in tasks]