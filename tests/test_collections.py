import pytest

from orco import LocalExecutor


def adder(config):
    return config["a"] + config["b"]


def test_reopen_collection(env):
    runtime = env.test_runtime()
    runtime.register_collection("col1", adder)

    with pytest.raises(Exception):
        runtime.register_collection("col1", adder)


def test_fixed_collection(env):
    runtime = env.test_runtime()
    runtime.register_executor(LocalExecutor())

    fix1 = runtime.register_collection("fix1")
    col2 = runtime.register_collection("col2", lambda c, d: d[0].value * 10, lambda c: [fix1.ref(c)])

    runtime.insert(fix1.ref("a"), 11)

    assert runtime.compute(col2.ref("a")).value == 110
    assert runtime.compute(fix1.ref("a")).value == 11

    with pytest.raises(Exception, match=".* fixed collection.*"):
        assert runtime.compute(col2.ref("b"))
    with pytest.raises(Exception, match=".* fixed collection.*"):
        assert runtime.compute(col2.ref("b"))


def test_collection_upgrade(env):
    runtime = env.test_runtime()
    runtime.register_executor(LocalExecutor(n_processes=1))

    def creator(config, deps):
        return config * 10

    def adder(config, deps):
        return deps["a"].value + deps["b"].value

    def adder_deps(config):
        return {"a": col1.ref(config["a"]), "b": col1.ref(config["b"])}

    def upgrade(config):
        config["c"] = config["a"] + config["b"]
        return config

    def upgrade_confict(config):
        del config["a"]
        return config

    col1 = runtime.register_collection("col1", creator)
    col2 = runtime.register_collection("col2", adder, adder_deps)

    runtime.compute(col1.ref(123))
    runtime.compute(col2.refs([{"a": 10, "b": 12},
                               {"a": 14, "b": 11},
                               {"a": 17, "b": 12}]))

    assert runtime.get_entry(col2.ref({"a": 10, "b": 12})).value == 220

    with pytest.raises(Exception, match=".* collision.*"):
        runtime.upgrade_collection(col2, upgrade_confict)

    assert runtime.get_entry(col2.ref({"a": 10, "b": 12})).value == 220

    runtime.upgrade_collection(col2, upgrade)

    assert runtime.get_entry(col2.ref({"a": 10, "b": 12})) is None
    assert runtime.get_entry(col2.ref({"a": 10, "b": 12, "c": 22})).value == 220
    assert runtime.get_entry(col2.ref({"a": 14, "b": 11})) is None
    assert runtime.get_entry(col2.ref({"a": 14, "b": 11, "c": 25})).value == 250


def test_collection_compute(env):
    runtime = env.test_runtime()
    runtime.register_executor(LocalExecutor(n_processes=1))

    counter = env.file_storage("counter", 0)

    def adder(config, deps):
        assert not deps
        counter.write(counter.read() + 1)
        return config["a"] + config["b"]

    collection = runtime.register_collection("col1", adder)

    entry = runtime.compute(collection.ref({"a": 10, "b": 30}))
    assert entry.config["a"] == 10
    assert entry.config["b"] == 30
    assert entry.value == 40
    assert entry.comp_time >= 0
    assert counter.read() == 1

    result = runtime.compute([collection.ref({"a": 10, "b": 30})])
    assert len(result) == 1
    entry = result[0]
    assert entry.config["a"] == 10
    assert entry.config["b"] == 30
    assert entry.value == 40
    assert entry.comp_time >= 0
    assert counter.read() == 1


def test_collection_deps(env):
    runtime = env.test_runtime()
    runtime.register_executor(LocalExecutor(n_processes=1))

    counter_file = env.file_storage("counter", [0, 0])

    def builder1(config, input):
        counter = counter_file.read()
        counter[0] += 1
        counter_file.write(counter)
        return config * 10

    def builder2(config, deps):
        counter = counter_file.read()
        counter[1] += 1
        counter_file.write(counter)
        return sum(e.value for e in deps)

    def make_deps(config):
        return [col1.ref(x) for x in range(config)]

    col1 = runtime.register_collection("col1", builder1)
    col2 = runtime.register_collection("col2", builder2, make_deps)

    e = runtime.compute(col2.ref(5))
    counter = counter_file.read()
    assert counter == [5, 1]
    assert e.value == 100

    e = runtime.compute(col2.ref(4))

    counter = counter_file.read()
    assert counter == [5, 2]
    assert e.value == 60

    runtime.remove_many(col1.refs([0, 3]))

    e = runtime.compute(col2.ref(6))
    counter = counter_file.read()
    assert counter == [8, 3]
    assert e.value == 150

    e = runtime.compute(col2.ref(6))
    counter = counter_file.read()
    assert counter == [8, 3]
    assert e.value == 150

    runtime.remove(col2.ref(6))
    e = runtime.compute(col2.ref(5))
    counter = counter_file.read()
    assert counter == [8, 4]
    assert e.value == 100

    e = runtime.compute(col2.ref(6))
    counter = counter_file.read()
    assert counter == [8, 5]
    assert e.value == 150


def test_collection_deps_complex(env):
    runtime = env.test_runtime()
    runtime.register_executor(LocalExecutor(n_processes=1))

    def builder1(config, input):
        return config * 10

    def builder2(config, deps):
        return sum(e.value for e in deps[0].values()) + deps[1]

    def make_deps(config):
        return [{
            "a": col1.ref(1),
            "b": col1.ref(1),
            "c": col1.ref(3),
            "d": col1.ref(4)
        }, 5]

    col1 = runtime.register_collection("col1", builder1)
    col2 = runtime.register_collection("col2", builder2, make_deps)

    e = runtime.compute(col2.ref(1))
    assert e.value == 95


def test_collection_double_ref(env):
    runtime = env.test_runtime()
    runtime.register_executor(LocalExecutor())

    col1 = runtime.register_collection("col1", lambda c, d: c * 10)
    col2 = runtime.register_collection("col2",
                                       (lambda c, d: sum(x.value for x in d)),
                                       (lambda c: [col1.ref(10), col1.ref(10), col1.ref(10)]))
    assert runtime.compute(col2.ref("abc")).value == 300


def test_collection_stored_deps(env):
    runtime = env.test_runtime()
    runtime.register_executor(LocalExecutor())

    col1 = runtime.register_collection("col1", lambda c, d: c * 10)
    col2 = runtime.register_collection("col2",
                                       (lambda c, d: sum(x.value for x in d)),
                                       lambda c: [col1.ref(i) for i in range(c["start"], c["end"], c["step"])])
    col3 = runtime.register_collection("col3",
                                       (lambda c, d: sum(x.value for x in d)),
                                       lambda c: [col2.ref({"start": 0, "end": c, "step": 2}), col2.ref({"start": 0, "end": c, "step": 3})])
    assert runtime.compute(col3.ref(10)).value == 380

    cc2_2 = {"end": 10, "start": 0, "step": 2}
    cc2_3 = {"end": 10, "start": 0, "step": 3}
    c2_2 = col2.ref(cc2_2)
    c2_3 = col2.ref(cc2_3)

    assert runtime.get_entry_state(col3.ref(10)) == "finished"
    assert runtime.get_entry_state(c2_2) == "finished"
    assert runtime.get_entry_state(c2_3) == "finished"
    assert runtime.get_entry_state(col1.ref(0)) == "finished"
    assert runtime.get_entry_state(col1.ref(2)) == "finished"

    assert set(runtime.db.get_recursive_consumers(col1.name, "2")) == {
        ("col1", "2"), ('col2', "{'end':10,'start':0,'step':2,}"), ('col3', "10")
    }

    assert set(runtime.db.get_recursive_consumers(col1.name, "6")) == {
        ("col1", "6"), ('col2', c2_2.key), ("col2", c2_3.key), ('col3', "10")
    }

    assert set(runtime.db.get_recursive_consumers(col1.name, "9")) == {
            ("col1", "9"), ("col2", c2_3.key), ('col3', "10")
    }

    assert set(runtime.db.get_recursive_consumers(col2.name, c2_3.key)) == {
        ("col2", c2_3.key),  ('col3', "10")
    }

    assert set(runtime.db.get_recursive_consumers(col3.name, col3.ref(10).key)) == {
        ('col3', '10')
    }

    runtime.remove(col1.ref(6))
    assert runtime.get_entry_state(col3.ref(10)) is None
    assert runtime.get_entry_state(c2_2) is None
    assert runtime.get_entry_state(c2_3) is None
    assert runtime.get_entry_state(col1.ref(0)) == "finished"
    assert runtime.get_entry_state(col1.ref(6)) is None
    assert runtime.get_entry_state(col1.ref(2)) == "finished"

    runtime.remove(col1.ref(0))
    assert runtime.get_entry_state(col3.ref(10)) is None
    assert runtime.get_entry_state(c2_2) is None
    assert runtime.get_entry_state(c2_3) is None
    assert runtime.get_entry_state(col1.ref(0)) is None
    assert runtime.get_entry_state(col1.ref(6)) is None
    assert runtime.get_entry_state(col1.ref(2)) == "finished"

    assert runtime.compute(col3.ref(10)).value == 380

    assert runtime.get_entry_state(col3.ref(10)) == "finished"
    assert runtime.get_entry_state(c2_2) == "finished"
    assert runtime.get_entry_state(c2_3) == "finished"
    assert runtime.get_entry_state(col1.ref(0)) == "finished"
    assert runtime.get_entry_state(col1.ref(2)) == "finished"

    runtime.remove(col1.ref(2))

    assert runtime.get_entry_state(col3.ref(10)) is None
    assert runtime.get_entry_state(c2_2) is None
    assert runtime.get_entry_state(c2_3) == "finished"
    assert runtime.get_entry_state(col1.ref(0)) == "finished"
    assert runtime.get_entry_state(col1.ref(6)) == "finished"
    assert runtime.get_entry_state(col1.ref(2)) is None

    runtime.remove(col1.ref(2))


def test_collection_clean(env):
    runtime = env.test_runtime()
    runtime.register_executor(LocalExecutor())

    col1 = runtime.register_collection("col1", lambda c, d: c)
    col2 = runtime.register_collection("col2", lambda c, d: c, lambda c: [col1.ref(c)])

    runtime.compute(col2.ref(1))
    runtime.clean(col1)
    assert runtime.get_entry_state(col1.ref(1)) is None
    assert runtime.get_entry_state(col2.ref(1)) is None
    assert runtime.get_entry_state(col2.ref(2)) is None


def test_collection_remove_inputs(env):
    runtime = env.test_runtime()
    runtime.register_executor(LocalExecutor())

    col1 = runtime.register_collection("col1", lambda c, d: c)
    col2 = runtime.register_collection("col2", lambda c, d: c, lambda c: [col1.ref(c)])
    col3 = runtime.register_collection("col3", lambda c, d: c, lambda c: [col1.ref(c)])
    col4 = runtime.register_collection("col4", lambda c, d: c, lambda c: [col2.ref(c)])

    runtime.compute(col4.ref(1))
    runtime.compute(col3.ref(1))
    runtime.remove(col2.ref(1), remove_inputs=True)
    assert runtime.get_entry_state(col1.ref(1)) is None
    assert runtime.get_entry_state(col2.ref(1)) is None
    assert runtime.get_entry_state(col3.ref(1)) is None
    assert runtime.get_entry_state(col4.ref(1)) is None


def test_collection_computed(env):
    runtime = env.test_runtime()
    runtime.register_executor(LocalExecutor(n_processes=1))

    def build_fn(x, deps):
        return x * 10

    collection = runtime.register_collection("col1", build_fn)
    refs = collection.refs([2, 3, 4, 0, 5])
    assert len(refs) == 5
    assert runtime.get_entries(refs) == [None] * len(refs)
    assert runtime.get_entries(refs, drop_missing=True) == []

    runtime.compute(refs)
    assert [e.value for e in runtime.get_entries(refs)] == [20, 30, 40, 0, 50]
    assert [e.value if e else "missing" for e in runtime.get_entries(refs + [collection.ref(123)])] == [20, 30, 40, 0, 50, "missing"]
    assert [e.value if e else "missing" for e in runtime.get_entries(refs + [collection.ref(123)], drop_missing=True)] == [20, 30, 40, 0, 50]