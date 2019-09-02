from orco import LocalExecutor
from orco.export import export_builder_to_pandas


def test_builder_to_pandas(env):
    runtime = env.test_runtime()
    runtime.register_executor(LocalExecutor())

    col1 = runtime.register_builder("col1", lambda c, d: c * 2)
    runtime.compute(col1.tasks([1, 2, 3, 4]))
    frame = export_builder_to_pandas(runtime, col1.name)
    assert len(frame) == 4
    assert sorted(frame["config"]) == [1, 2, 3, 4]
    assert sorted(frame["value"]) == [2, 4, 6, 8]

    assert frame[frame["config"] == 1]["value"].iloc[0] == 2