from orco.internals.runner import PoolJobRunner
from concurrent.futures import Future


class NaivePool():

    def __init__(self, events):
        self.events = events

    def submit(self, fn, *args, **kwargs):
        self.events.append("run")
        f = Future()
        f.set_result(fn(*args, **kwargs))
        return f


class NaiveRunner(PoolJobRunner):

    def __init__(self):
        super().__init__()
        self.events = []

    def _create_pool(self):
        return NaivePool(self.events)


def test_runner_selection(env):
    runtime = env.test_runtime()

    testing_runner = NaiveRunner()
    runtime.add_runner("tr", testing_runner)

    b1 = runtime.register_builder("col1", lambda c, d: c, job_setup="tr")
    b2 = runtime.register_builder("col2", lambda c, d: c, lambda c: [b1.task(c)])

    r = runtime.compute(b2.task(10))
    assert r.value == 10
    assert r.job_setup == {}
    r = runtime.get_entry(b1.task(10))
    assert r.job_setup == {"runner": "tr"}
    assert len(testing_runner.events) == 1