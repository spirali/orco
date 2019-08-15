import pytest

from orco import LocalExecutor


def test_runtime_stop(env):
    with env.test_runtime() as runtime:
        executor = LocalExecutor(heartbeat_interval=1)
        runtime.register_executor(executor)
        assert not runtime.stopped

    runtime = env.test_runtime()
    with runtime:
        executor = LocalExecutor(heartbeat_interval=1)
        runtime.register_executor(executor)
        assert not runtime.stopped

    with pytest.raises(Exception):
        with runtime:
            pass


def test_reports(env):

    def adder(config, deps):
        return config["a"] + config["b"]

    executor = LocalExecutor()
    runtime = env.test_runtime()
    runtime.register_executor(executor)
    collection = runtime.register_collection("col1", adder)
    entry = runtime.compute(collection.ref({"a": 10, "b": 30}))

    reports = runtime.get_reports()
    assert len(reports) == 1