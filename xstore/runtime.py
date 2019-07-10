from .db import DB
from .collection import Collection
from .executor import Executor, LocalExecutor

import cloudpickle


class Runtime:

    def __init__(self, db_path, executor: Executor=None):
        self.db = DB(db_path)

        if executor is None:
            executor = LocalExecutor()
        self.executor = executor

    def collection(self, name, build_fn=None, dep_fn=None):
        self.db.ensure_collection(name)
        return Collection(self, name, build_fn=build_fn, dep_fn=dep_fn)