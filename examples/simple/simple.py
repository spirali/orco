
from orco import Runtime, run_cli
import time, random


runtime = Runtime("mydb.db")


def do_preprocessing(config):
    time.sleep(0.3)  # Simulate computation
    return random.randint(0, 10)


def make_experiment(config, deps):
    time.sleep(config["difficulty"])  # Simulate computation
    return sum(entry.value for entry in deps) + config.get("value", 0)


def make_experiment_deps(config):
    d = config["difficulty"]
    return [preprocessing.ref(d - 2), preprocessing.ref(d - 1)]

preprocessing = runtime.register_collection(
    "preprocessing", build_fn=do_preprocessing)
experiments = runtime.register_collection(
    "experiments", build_fn=make_experiment, dep_fn=make_experiment_deps)

run_cli(runtime)
