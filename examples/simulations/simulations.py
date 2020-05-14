import orco
import time, random

@orco.builder()
def simulation(p):
    # Fake a computation
    result = random.randint(0, p)
    time.sleep(1)
    return result


# Run experiment, sum resulting values as demonstration of a postprocessing
# of simulation
@orco.builder()
def experiment(start, end):
    sims = [simulation(p) for p in range(start, end)]
    yield
    return sum([s.value for s in sims])


orco.start_runtime("sqlite:///my.db")
orco.compute(experiment(0, 10))
orco.compute(experiment(7, 15))