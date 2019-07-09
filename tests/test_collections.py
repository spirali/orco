
from xstore import Runtime, Obj

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

    result = collection.compute([{"a": 10, "b": 30}])
    assert len(result) == 1
    entry = result[0]
    assert entry.config["a"] == 10
    assert entry.config["b"] == 30
    assert entry.value == 40
    assert counter[0] == 1

    result = collection.compute([{"a": 10, "b": 30}])
    assert len(result) == 1
    entry = result[0]
    assert entry.config["a"] == 10
    assert entry.config["b"] == 30
    assert entry.value == 40
    assert counter[0] == 1