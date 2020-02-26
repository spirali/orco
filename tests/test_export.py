from orco import Builder
from orco.export import export_builder_to_pandas


def test_builder_to_pandas(env):
    runtime = env.test_runtime()

    col1 = runtime.register_builder(Builder(lambda c: c * 2, "col1"))
    runtime.compute_many([col1(x) for x in [1, 2, 3, 4]])
    frame = export_builder_to_pandas(runtime, col1.name)
    assert len(frame) == 4
    assert sorted(frame["config"]) == [1, 2, 3, 4]
    assert sorted(frame["value"]) == [2, 4, 6, 8]

    assert frame[frame["config"] == 1]["value"].iloc[0] == 2
