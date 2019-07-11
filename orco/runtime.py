from .db import DB
from .collection import Collection
from .executor import Executor, LocalExecutor


import cloudpickle
import argparse
import threading


class Runtime:

    def __init__(self, db_path, executor: Executor=None):
        self.db = DB(db_path)

        if executor is None:
            executor = LocalExecutor()
        self._executor = executor
        self._collections = {}
        self._lock = threading.Lock()


    def register_collection(self, name, build_fn=None, dep_fn=None):
        with self._lock:
            if name in self._collections:
                raise Exception("Collection already registered")
            self.db.ensure_collection(name)
            collection = Collection(self, name, build_fn=build_fn, dep_fn=dep_fn)
            self._collections[name] = collection
            return collection

    @property
    def collections(self):
        with self._lock:
            return self._collections.copy()

    def collection_summaries(self):
        return self.db.collection_summaries()

    def entry_summaries(self, collection_name):
        return self.db.entry_summaries(self.collections[collection_name])

    def serve(self, port=8550, debug=False, testing=False):
        from .rest import init_service
        app = init_service(self)
        if testing:
            app.testing = True
            return app
        else:
            app.run(port=port, debug=debug, use_reloader=False)

    def _command_serve(self, args):
        self.serve()

    def _parse_args(self):
        parser = argparse.ArgumentParser("orco")
        sp = parser.add_subparsers(title="command")
        p = sp.add_parser("serve")
        p.set_default(func=self._command_serve)
        return parser.parse_args()

    def main():
        self._parse_args()