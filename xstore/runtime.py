from .db import DB
from .collection import CollectionConfig, Collection
from .executor import Executor, LocalExecutor

import cloudpickle


class Runtime:

    def __init__(self, db_path, executor: Executor=None):
        self.db = DB(db_path)

        if executor is None:
            executor = LocalExecutor()
        self.executor = executor

    def new_collection(self, name, build_fn=None, dep_fn=None):
        cc = CollectionConfig(name, build_fn, dep_fn)
        serialized_cc = cloudpickle.dumps(cc)
        self.db.new_collection(name, serialized_cc)
        return Collection(self, cc)