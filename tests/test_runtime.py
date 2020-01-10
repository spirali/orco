import pytest


def test_runtime_stop(env):
    with env.test_runtime() as runtime:
        runtime.configure_executor(heartbeat_interval=1)
        assert not runtime.stopped

    runtime = env.test_runtime()
    with runtime:
        runtime.configure_executor(heartbeat_interval=1)
        assert not runtime.stopped

    with pytest.raises(Exception):
        with runtime:
            pass


def test_reports(env):

    def adder(config, deps):
        return config["a"] + config["b"]

    runtime = env.test_runtime()
    builder = runtime.register_builder("col1", adder)
    entry = runtime.compute(builder.task({"a": 10, "b": 30}))

    reports = runtime.get_reports()
    assert len(reports) == 1