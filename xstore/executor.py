

class Task:

    def __init__(self, compute_fn, args, deps: ["Task"]):
        self.compute_fn = compute_fn
        self.args = args
        self.deps = deps

    def run(self):
        return self.compute_fn(*self.args)


class Executor:

    def run(self, tasks: [Task]):
        raise NotImplementedError


class LocalExecutor(Executor):

    def run(self, tasks: [Task]):
        # TODO: sort by deps
        return [task.run() for task in tasks]