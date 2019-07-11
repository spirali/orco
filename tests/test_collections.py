
from orco import Runtime, Obj

def adder(config):
    return config["a"] + config["b"]


def test_reopen_collection():
    runtime = Runtime(":memory:")
    runtime.collection("col1", adder)
    runtime.collection("col1", adder)


def test_collection_compute():
    runtime = Runtime(":memory:")
    counter = [0]
    def adder(config):
        counter[0] += 1
        return config["a"] + config["b"]

    collection = runtime.collection("col1", adder)

    entry = collection.compute({"a": 10, "b": 30})
    assert entry.config["a"] == 10
    assert entry.config["b"] == 30
    assert entry.value == 40
    assert counter[0] == 1

    result = collection.compute_many([{"a": 10, "b": 30}])
    assert len(result) == 1
    entry = result[0]
    assert entry.config["a"] == 10
    assert entry.config["b"] == 30
    assert entry.value == 40
    assert counter[0] == 1


def test_collection_deps():
    runtime = Runtime(":memory:")
    counter = [0, 0]

    def builder1(config):
        counter[0] += 1
        return config * 10

    def builder2(config, deps):
        counter[1] += 1
        return sum(e.value for e in deps)

    def make_deps(config):
        return [col1.ref(x) for x in range(config)]

    col1 = runtime.collection("col1", builder1)
    col2 = runtime.collection("col2", builder2, make_deps)

    e = col2.compute(5)

    assert counter == [5, 1]
    assert e.value == 100

    e = col2.compute(4)

    assert counter == [5, 2]
    assert e.value == 60

    col1.remove_many([0, 3])

    e = col2.compute(6)
    assert counter == [8, 3]
    assert e.value == 150

    e = col2.compute(6)
    assert counter == [8, 3]
    assert e.value == 150

    col2.remove(6)

    e = col2.compute(5)
    assert counter == [8, 3]
    assert e.value == 100

    e = col2.compute(6)
    assert counter == [8, 4]
    assert e.value == 150