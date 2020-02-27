import random
import time

import orco


@orco.builder()
def do_something(config):
    time.sleep(0.3)  # Simulate computation
    return random.randint(0, 10)


@orco.builder()
def make_experiment(config):
    data = [do_something(x) for x in range(config["difficulty"])]
    yield
    time.sleep(config["difficulty"])  # Simulate computation
    return sum(entry.value for entry in data)


orco.run_cli()
