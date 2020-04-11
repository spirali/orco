import itertools
import os
import random
import threading
import time

from orco import Runtime, builder, Builder, attach_object

url = "sqlite:///test.db"
if os.path.isfile("test.db"):
    os.unlink("test.db")

@builder()
def failer(config):
    raise Exception("Here!")


@builder()
def sleeper(c):
    time.sleep(c)


@builder()
def bedroom(sleepers):
    [sleeper(x) for x in sleepers]
    yield
    return None

@builder()
def state_demo(x):
    if x > 0:
        state_demo(x - 1)
    yield
    attach_object("data1", "Hello!")

rt = Runtime(url)


try:
    rt.compute(failer(config="fail1"))

except Exception as e:
    print(e)
print("Failer failed (and it is ok")

rt.compute(bedroom(sleepers=[0.1]))
def thread_fn():
    rt = Runtime(url)
    try:
        rt.compute(bedroom(list(range(10))))
    finally:
        rt.stop()


t = threading.Thread(target=thread_fn)
t.start()

time.sleep(0.5)

c = rt.register_builder(Builder(None, name="hello"))
rt.insert(c(cfg="e1"), "ABC")
rt.insert(c(cfg="e2"), "A" * (7 * 1024 * 1024 + 200000))

c = Builder(None, name="estee")
rt.register_builder(c)
graphs = ["crossv", "fastcrossv", "gridcat"]
models = ["simple", "maxmin"]
scheduler = [
    "blevel", "random", {
        "name": "camp",
        "iterations": 1000
    }, {
        "name": "camp",
        "iterations": 2000
    }
]
for g, m, s in itertools.product(graphs, models, scheduler):
    rt.insert(c(graph=g, model=m, scheduler=s), random.randint(1, 30000))

c = rt.register_builder(Builder(None, name="builder_long_name"))

rt.compute(state_demo(4))
rt.free_many([state_demo(2), state_demo(3)])
rt.archive([state_demo(3)])

print("SERVE")
print("=" * 80)
rt.serve()
