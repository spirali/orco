import random
import time

import orco


@orco.builder()
def do_something(_unused):
    time.sleep(0.3)  # Simulate computation
    return random.randint(0, 10)


@orco.builder()
def make_experiment(difficulty):
    data = [do_something(x) for x in range(difficulty)]
    yield
    time.sleep(difficulty)  # Simulate computation
    return sum(entry.value for entry in data)


orco.run_cli()
