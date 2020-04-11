from orco import Builder
from orco.ext.pandas import export_builder
from orco.ext.pandas import unpack_frame


def test_unpack_frame():
    import pandas as pd

    frame = pd.DataFrame(
        [
            {"config": {"a": 5, "b": 3}, "value": 10},
            {"config": {"a": 7, "b": 6}, "value": 1},
            {"config": {"a": 8, "b": 4, "c": False}, "value": 2},
        ]
    )
    unpacked = unpack_frame(frame)

    assert set(unpacked.columns) == {"a", "b", "c", "value"}
    assert unpacked[(unpacked["a"] == 5) & (unpacked["b"] == 3)]["value"].iloc[0] == 10
    assert pd.isna(unpacked[(unpacked["a"] == 5) & (unpacked["b"] == 3)]["c"]).all()
    assert unpacked[(unpacked["a"] == 8) & (unpacked["b"] == 4)]["c"].iloc[0] is False


def test_builder_to_pandas(env):
    runtime = env.test_runtime()

    col1 = runtime.register_builder(Builder(lambda c: c * 2, "col1"))
    runtime.compute_many([col1(x) for x in [1, 2, 3, 4]])
    frame = export_builder(runtime, col1.name)
    assert len(frame) == 4
    assert sorted(frame["arg.c"]) == [1, 2, 3, 4]


def test_builder_to_pandas_kwargs(env):
    runtime = env.test_runtime()

    def foo(a=1, **kwargs):
        return a + sum(kwargs.values())

    foob = runtime.register_builder(Builder(foo))
    runtime.compute(foob(b=2))
    runtime.compute(foob(a=3))
    runtime.compute(foob(42, b=1, c=8))
    frame = export_builder(runtime, foob.name)
    frame.sort_values("arg.a")
    frame.fillna(value=999, inplace=True)

    assert len(frame) == 3
    assert list(frame["arg.a"]) == [1, 3, 42]
    assert list(frame["arg.b"]) == [2, 999, 1]
    assert list(frame["arg.c"]) == [999, 999, 8]
