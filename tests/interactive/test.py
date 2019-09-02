from orco import Runtime, LocalExecutor

import itertools
import random
import time
import threading
import os

if os.path.isfile("test.db"):
    os.unlink("test.db")

rt = Runtime("test.db")

executor = LocalExecutor(heartbeat_interval=1, n_processes=1)
rt.register_executor(executor)

executor2 = LocalExecutor(heartbeat_interval=1)
executor2._debug_do_not_start_heartbeat = True
rt.register_executor(executor2)

executor3 = LocalExecutor(heartbeat_interval=1)
rt.register_executor(executor3)
executor3.stop()

c_sleepers = rt.register_builder("sleepers", lambda c, d: time.sleep(c))
c_bedrooms = rt.register_builder("bedrooms", lambda c, d: None,
                                    lambda c: [c_sleepers.task(x) for x in c["sleepers"]])


def failer(config, deps):
    raise Exception("Here!")


c_failers = rt.register_builder("failers", failer)
try:
    rt.compute(c_failers.task({"type": "fail1"}))
except Exception as e:
    print(e)
print("Failer failed (and it is ok")

rt.compute(c_bedrooms.task({"sleepers": [0.1]}))
t = threading.Thread(target=(lambda: rt.compute(c_bedrooms.task({"sleepers": list(range(10))}))))
t.start()

time.sleep(0.5)  # To solve a problem with ProcessPool, fix waits for Python3.7

c = rt.register_builder("hello")
rt.insert(c.task("e1"), "ABC")
rt.insert(c.task("e2"), "A" * (7 * 1024 * 1024 + 200000))

c = rt.register_builder("estee")
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
    rt.insert(c.task({"graph": g, "model": m, "scheduler": s}), random.randint(1, 30000))

c = rt.register_builder("builder with space in name")

rt.serve()
