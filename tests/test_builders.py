import pickle
import random
import os

import pytest

from orco import Builder, builder, JobState

def adder(a, b):
    return a + b


def test_builder_args(env):
    @builder()
    def f(a, *ars, e=2, **kwrs):
        return (a, ars, e, kwrs)

    @builder()
    def g(a, *ars, e=2, **kwrs):
        d = f(a, *ars, e=e, **kwrs)
        yield
        return d.value

    runtime = env.test_runtime()
    assert runtime.compute(f(1)).value == (1, (), 2, {})
    assert runtime.compute(f(1, 2, 3, 4, f=3)).value == (1, (2, 3, 4), 2, {'f': 3})
    assert runtime.compute(f(e=3, a=42)).value == (42, (), 3, {})

    assert runtime.compute(g(1, 2, 3, 4, f=3)).value == (1, (2, 3, 4), 2, {'f': 3})


def test_pickle_builder():
    @builder()
    def f(x):
        return x + 1

    bf = f
    print(f)
    s = pickle.dumps(bf)
    f2 = pickle.loads(s)
    assert f2.run_with_config({'x': 42}) == 43
    assert f2.run_with_args((44, ), {}) == 45


def test_builder_init(env):

    @builder(name="foo")
    def bar(conf):
        "bleurghsff"
        return conf

    assert bar.name == "foo"
    assert bar.__name__ == "bar"
    assert "bleurghsff" in bar.__doc__

    @builder()
    def baz(conf):
        return conf
    assert baz.name == "baz"
    assert baz.__name__ == "baz"

    with pytest.raises(ValueError, match=".*Provide at leas one of fn and name.*"):
        Builder(None)
    with pytest.raises(ValueError, match=".*is not a valid name for Builder.*"):
        Builder(lambda cfg: None)

    runtime = env.test_runtime()
    runtime.register_builder(Builder(None, "b1"))

    assert "b1" in runtime._builders
    assert "foo" in runtime._builders
    assert "bar" not in runtime._builders
    assert "baz" in runtime._builders


def test_reopen_builder(env):
    runtime = env.test_runtime()
    runtime.register_builder(Builder(adder, "col1"))

    with pytest.raises(Exception):
        runtime.register_builder(Builder(adder, "col1"))


def test_fixed_builder(env):
    runtime = env.test_runtime()

    fix1 = runtime.register_builder(Builder(None, "fix1"))

    def b1(v):
        f = fix1(x=v)
        yield
        return f.value * 10

    col2 = runtime.register_builder(Builder(b1, "col2"))

    runtime.insert(fix1(x="a"), 11)

    assert runtime.compute(col2("a")).value == 110
    assert runtime.compute(fix1(x="a")).value == 11

    with pytest.raises(Exception, match=".* fixed builder.*"):
        assert runtime.compute(col2("b"))
    with pytest.raises(Exception, match=".* fixed builder.*"):
        assert runtime.compute(col2("b"))


def test_builder_upgrade(env):
    runtime = env.test_runtime(n_processes=1)

    def creator(a):
        return a * 10

    def adder2(a, b, **kwargs):
        ae = col1(a)
        be = col1(b)
        yield
        return ae.value + be.value

    def upgrade(config):
        config["c"] = config["a"] + config["b"]
        return config

    def upgrade_confict(config):
        del config["a"]
        return config

    col1 = runtime.register_builder(Builder(creator, "col1"))
    col2 = runtime.register_builder(Builder(adder2, "col2"))

    runtime.compute(col1(123))
    runtime.compute_many([col2(**c) for c in [{"a": 10, "b": 12}, {"a": 14, "b": 11}, {"a": 17, "b": 12}]])

    assert runtime.read_entry(col2(a=10, b=12)).value == 220

    with pytest.raises(Exception, match=".* collision.*"):
        runtime.upgrade_builder(col2, upgrade_confict)

    assert runtime.read_entry(col2(a=10, b=12)).value == 220

    runtime.upgrade_builder(col2, upgrade)

    assert runtime.try_read_entry(col2(a=10, b=12)) is None
    assert runtime.read_entry(col2(a=10, b=12, c=22)).value == 220
    assert runtime.try_read_entry(col2(a=14, b=11)) is None
    assert runtime.read_entry(col2(a=14, b=11, c=25)).value == 250


def test_builder_compute(env):
    runtime = env.test_runtime(n_processes=1)

    counter = env.file_storage("counter", 0)

    def adder2(a, b):
        counter.write(counter.read() + 1)
        return a + b

    bld = runtime.register_builder(Builder(adder2, "col1"))

    entry = runtime.compute(bld(10, 30))
    assert entry.config["a"] == 10
    assert entry.config["b"] == 30
    assert entry.value == 40
    assert entry.metadata().computation_time >= 0
    assert counter.read() == 1

    result = runtime.compute_many([bld(b=30, a=10)])
    assert len(result) == 1
    entry = result[0]
    assert entry.config["a"] == 10
    assert entry.config["b"] == 30
    assert entry.value == 40
    assert entry.metadata().computation_time >= 0
    assert counter.read() == 1


def test_builder_deps(env):
    runtime = env.test_runtime(n_processes=1)

    counter_file = env.file_storage("counter", [0, 0])

    def builder1(config):
        counter = counter_file.read()
        counter[0] += 1
        counter_file.write(counter)
        return config * 10

    def builder2(config):
        deps = [col1(x) for x in range(config)]
        yield
        counter = counter_file.read()
        counter[1] += 1
        counter_file.write(counter)
        return sum(e.value for e in deps)

    col1 = runtime.register_builder(Builder(builder1, "col1"))
    col2 = runtime.register_builder(Builder(builder2, "col2"))

    e = runtime.compute(col2(5))
    counter = counter_file.read()
    assert counter == [5, 1]
    assert e.value == 100

    e = runtime.compute(col2(4))

    counter = counter_file.read()
    assert counter == [5, 2]
    assert e.value == 60

    runtime.drop_many([col1(0), col1(3)])

    e = runtime.compute(col2(6))
    counter = counter_file.read()
    assert counter == [8, 3]
    assert e.value == 150

    e = runtime.compute(col2(6))
    counter = counter_file.read()
    assert counter == [8, 3]
    assert e.value == 150

    runtime.drop(col2(6))
    e = runtime.compute(col2(5))
    counter = counter_file.read()
    assert counter == [8, 4]
    assert e.value == 100

    e = runtime.compute(col2(6))
    counter = counter_file.read()
    assert counter == [8, 5]
    assert e.value == 150


def test_builder_double_task(env):
    runtime = env.test_runtime()

    def b2(config):
        tasks = [col1(10), col1(10), col1(10)]
        yield
        return sum(x.value for x in tasks)

    col1 = runtime.register_builder(Builder(lambda c: c * 10, "col1"))
    col2 = runtime.register_builder(Builder(b2, "col2"))
    assert runtime.compute(col2("abc")).value == 300


def test_builder_stored_deps(env):
    runtime = env.test_runtime()

    col1 = runtime.register_builder(Builder(lambda c: c * 10, "col1"))

    def b2(start, end, step):
        data = [col1(i) for i in range(start, end, step)]
        yield
        return sum(d.value for d in data)

    col2 = runtime.register_builder(Builder(b2, "col2"))

    def b3(end):
        a = col2(0, end, 2)
        b = col2(0, end, 3)
        yield
        return a.value + b.value

    col3 = runtime.register_builder(Builder(b3, "col3"))
    assert runtime.compute(col3(10)).value == 380

    cc2_2 = {"end": 10, "start": 0, "step": 2}
    cc2_3 = {"end": 10, "start": 0, "step": 3}
    c2_2 = col2(**cc2_2)
    c2_3 = col2(**cc2_3)

    assert runtime.get_entry_state(col3(10)) == JobState.FINISHED
    assert runtime.get_entry_state(c2_2) == JobState.FINISHED
    assert runtime.get_entry_state(c2_3) == JobState.FINISHED
    assert runtime.get_entry_state(col1(0)) == JobState.FINISHED
    assert runtime.get_entry_state(col1(2)) == JobState.FINISHED

    runtime.drop(col1(6))
    assert runtime.get_entry_state(col3(10)) == JobState.NONE
    assert runtime.get_entry_state(c2_2) == JobState.NONE
    assert runtime.get_entry_state(c2_3) == JobState.NONE
    assert runtime.get_entry_state(col1(0)) == JobState.FINISHED
    assert runtime.get_entry_state(col1(6)) == JobState.NONE
    assert runtime.get_entry_state(col1(2)) == JobState.FINISHED

    runtime.drop(col1(0))
    assert runtime.get_entry_state(col3(10)) == JobState.NONE
    assert runtime.get_entry_state(c2_2) == JobState.NONE
    assert runtime.get_entry_state(c2_3) == JobState.NONE
    assert runtime.get_entry_state(col1(0)) == JobState.NONE
    assert runtime.get_entry_state(col1(6)) == JobState.NONE
    assert runtime.get_entry_state(col1(2)) == JobState.FINISHED

    assert runtime.compute(col3(10)).value == 380

    assert runtime.get_entry_state(col3(10)) == JobState.FINISHED
    assert runtime.get_entry_state(c2_2) == JobState.FINISHED
    assert runtime.get_entry_state(c2_3) == JobState.FINISHED
    assert runtime.get_entry_state(col1(0)) == JobState.FINISHED
    assert runtime.get_entry_state(col1(2)) == JobState.FINISHED

    runtime.drop(col1(2))

    assert runtime.get_entry_state(col3(10)) == JobState.NONE
    assert runtime.get_entry_state(c2_2) == JobState.NONE
    assert runtime.get_entry_state(c2_3) == JobState.FINISHED
    assert runtime.get_entry_state(col1(0)) == JobState.FINISHED
    assert runtime.get_entry_state(col1(6)) == JobState.FINISHED
    assert runtime.get_entry_state(col1(2)) == JobState.NONE

    runtime.drop(col1(2))


def test_builder_drop(env):
    runtime = env.test_runtime()

    col1 = runtime.register_builder(Builder(lambda c: c, "col1"))

    def b2(c):
        d = col1(c)
        yield
        return d.value

    col2 = runtime.register_builder(Builder(b2, "col2"))

    runtime.compute(col2(1))
    runtime.drop_builder("col1")
    assert runtime.get_entry_state(col1(1)) == JobState.NONE
    assert runtime.get_entry_state(col2(1)) == JobState.NONE
    assert runtime.get_entry_state(col2(2)) == JobState.NONE

    runtime.compute(col2(1))
    runtime.drop_builder("col2")
    assert runtime.get_entry_state(col1(1)) == JobState.FINISHED
    assert runtime.get_entry_state(col2(1)) == JobState.NONE
    assert runtime.get_entry_state(col2(2)) == JobState.NONE

    runtime.compute(col2(1))
    runtime.drop_builder("col2", drop_inputs=True)
    assert runtime.get_entry_state(col1(1)) == JobState.NONE
    assert runtime.get_entry_state(col2(1)) == JobState.NONE
    assert runtime.get_entry_state(col2(2)) == JobState.NONE


def test_builder_remove_inputs(env):
    runtime = env.test_runtime()

    col1 = runtime.register_builder(Builder(lambda c: c, "col1"))

    def b1(c):
        d = col1(c)
        yield
        return d.value

    def b2(c):
        d = col2(c)
        yield
        return d.value

    col2 = runtime.register_builder(Builder(b1, "col2"))
    col3 = runtime.register_builder(Builder(b1, "col3"))
    col4 = runtime.register_builder(Builder(b2, "col4"))

    runtime.compute(col4(1))
    runtime.compute(col3(1))
    runtime.drop(col2(1), drop_inputs=True)
    assert runtime.get_entry_state(col1(1)) == JobState.NONE
    assert runtime.get_entry_state(col2(1)) == JobState.NONE
    assert runtime.get_entry_state(col3(1)) == JobState.NONE
    assert runtime.get_entry_state(col4(1)) == JobState.NONE


def test_builder_computed(env):
    runtime = env.test_runtime(n_processes=1)

    def build_fn(x):
        return x * 10

    builder = runtime.register_builder(Builder(build_fn, "col1"))
    tasks = [builder(b) for b in [2, 3, 4, 0, 5]]
    assert len(tasks) == 5
    assert runtime.read_entries(tasks) == [None] * len(tasks)
    assert runtime.read_entries(tasks, drop_missing=True) == []

    runtime.compute_many(tasks)
    assert [e.value for e in runtime.read_entries(tasks)] == [20, 30, 40, 0, 50]
    assert [e.value if e else "missing" for e in runtime.read_entries(tasks + [builder(123)])
            ] == [20, 30, 40, 0, 50, "missing"]
    assert [
               e.value if e else "missing"
               for e in runtime.read_entries(tasks + [builder(123)], drop_missing=True)
           ] == [20, 30, 40, 0, 50]


def test_builder_error_in_deps(env):
    def builder_fn(c):
        if c != 0:
            raise Exception("MyError")
        yield
        return 123

    runtime = env.test_runtime()
    bld = runtime.register_builder(Builder(builder_fn, "col1"))
    with pytest.raises(Exception, match="MyError"):
        runtime.compute(bld(1))


def test_builder_double_yield_error(env):
    def builder_fn(c):
        yield
        yield
        return 123

    runtime = env.test_runtime()
    bld = runtime.register_builder(Builder(builder_fn, "col1"))
    with pytest.raises(Exception, match="yielded"):
        runtime.compute(bld(1))


def test_builder_ref_in_compute(env):
    def builder_fn():
        yield
        col0(123)
        return 123

    runtime = env.test_runtime()
    col0 = runtime.register_builder(Builder(lambda c: 123, "col0"))
    bld = runtime.register_builder(Builder(builder_fn, "col1"))
    with pytest.raises(Exception, match="computation phase"):
        runtime.compute(bld())


def test_builder_inconsistent_deps(env):
    def builder_fn():
        c = col0(random.random())
        print("C", c)
        yield
        return c.value + 1

    runtime = env.test_runtime()
    col0 = runtime.register_builder(Builder(lambda c: 123, "col0"))
    bld = runtime.register_builder(Builder(builder_fn, "col1"))
    
    with pytest.raises(Exception, match="dependencies"):
        runtime.compute(bld())


def test_builder_workdir(env):

    @builder()
    def bb(x):
        with open("test", "x"):
            pass
        return os.getcwd()

    runtime = env.test_runtime()
    a, b = runtime.compute_many([bb(1), bb(2)])
    assert a.value is not None and b.value is not None
    assert not os.path.isdir(a.value)
    assert not os.path.isdir(b.value)
    assert a.value != b.value