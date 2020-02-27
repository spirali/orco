from orco import Builder
from orco.export import export_builder_to_pandas
import pandas as pd


def test_builder_to_pandas(env):
    runtime = env.test_runtime()

    col1 = runtime.register_builder(Builder(lambda c: c * 2, "col1"))
    runtime.compute_many([col1(x) for x in [1, 2, 3, 4]])
    frame = export_builder_to_pandas(runtime, col1.name)
    assert len(frame) == 4
    assert sorted(frame["arg.c"]) == [1, 2, 3, 4]
    assert sorted(frame["value"]) == [2, 4, 6, 8]

    assert frame[frame["arg.c"] == 1]["value"].iloc[0] == 2


def test_builder_to_pandas_kwargs(env):
    runtime = env.test_runtime()

    def foo(a=1, **kwargs):
        return a + sum(kwargs.values())

    foob = runtime.register_builder(Builder(foo))
    runtime.compute(foob(b=2))
    runtime.compute(foob(a=3))
    runtime.compute(foob(42, b=1, c=8))
    frame = export_builder_to_pandas(runtime, foob.name)
    frame.sort_values("arg.a")
    frame.fillna(value=999, inplace=True)

    assert len(frame) == 3
    assert list(frame["arg.a"]) == [1, 3, 42]
    assert list(frame["arg.b"]) == [2, 999, 1]
    assert list(frame["arg.c"]) == [999, 999, 8]
    assert list(frame["value"]) == [3, 3, 51]
