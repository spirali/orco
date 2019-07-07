
from xstore import Runtime, Obj

def adder(config):
    return config.a + config.b

def test_new_collection():
    runtime = Runtime(":memory:")
    collection = runtime.new_collection("col1", adder)

    result = collection.compute([Obj(a=10, b=30)])
    assert len(result) == 1
    entry = result[0]
    assert entry.config.a == 10
    assert entry.config.b == 30
    assert entry.value == 40