# Getting started

## Example 1: Adder

In this example we will define a simple computation that adds two numbers. After
you run the computation, its results will be stored in a database to avoid
recomputation in the future.

The database with computation results is represented by a `Runtime`, which
receives a path to a database file where the results will be stored.

```python
from orco import Runtime, LocalExecutor

# Create a new runtime
# If the database file does not exist, it will be created
runtime = Runtime("./mydb")
```

After you have a database, you have to create an executor that will be used for running
the actual computations.
```python
# By default, the executor will use all local cores to run the experiments
runtime.register_executor(LocalExecutor())
```

Now you can start defining your computations. Computations in ORCO are stored in **collection**s.
A collection stores the results of a single type of computation (preparing a dataset,
training a neural network, benchmarking or compiling a program, ...). To define a collection,
you have to specify three things:
- Name
- **Build function**, which produces a result from an input
- **Dependency function**, which specifies other computations on which the build function depends (if any)

Let's define a trivial collection that will store results of a computation which simply adds two numbers
from its input. It does not depend on any other collections, so the dependency function does not have
to be specified.
```python
# You can ignore `inputs` for now, it is explained in the Advanced usage guide
def build_fn(config, inputs):
    return config["a"] + config["b"]

# Create a collection with a build function
add = runtime.register_collection("add", build_fn=build_fn)
```

You can think of a collection as a persistent cache on top of its build function. If you give an input
to the collection, it will either return an already computed result stored in the database or invoke
the build function if the result was not computed for the specified input yet.

Inputs for collections in ORCO are called **configuration**s. A configuration has to be a Python value
that is easily serializable (i.e. JSON-like - numbers, strings, booleans, lists, tuples or dictionaries).
The configuration should contain everything that is necessary to produce a result of the collection.

In our simple case, let's represent the configurations for our `add` collection as
a dictionary containing two numbers "a" and "b". Here we create two different configurations:
```python
config_1 = { "a": 1, "b": 2 }
config_2 = { "a": 3, "b": 4 }
```

We have now defined a database for storing computation results, an executor for running the computations,
a collection that defines a simple `add` computation and two configurations for `add` that we want to compute.

To compute a result, we have to give a **reference** to the runtime. Reference is an object specifying
what type of computation you want to perform (the collection) and with what input you want to compute it
(the configuration). Giving a reference to the runtime will produce the desired result. The returned
object will always be an instance of the `Entry` class, which contains both the input configuration
(`config`) and mainly the result value (`field`).

```python
# Compute the result of `add` with the input `config_1`
result = runtime.compute(add.ref(config_1))
print(result.value)  # prints: 3
```

Because this was the first time we asked for this specific computation, the build function was invoked
with the given configuration and its return value was stored into the database.
When we run the same computation again, the result will be provided directly from the database.
```python
result = runtime.compute(add.ref(config_1)) # build_fn is not called again here
```

So far we have computed only one configuration, usually you want to compute many of them
```python
result = runtime.compute(add.refs([{"a": 1, "b": 2},
                                   {"a": 2, "b": 3},
                                   {"a": 4, "b": 5}]))
print([r.value for r in result])  # prints: [3, 5, 9]

# add.refs([A, B, C]) is just shortcut for [add.ref(A), add.ref(B), add.ref(C)]
```

This concludes basic ORCO usage. You define a collection with a build function and ask the collection
to give you results for your desired configurations.

The whole code example is listed here:

```python
from orco import Runtime, LocalExecutor

# Create a runtime environment for ORCO
# All data will be stored in file on provided path
# If the file does not exists, it will be created
runtime = Runtime("./mydb")


# Registering executor for running tasks
# By default, it will use all local cores
runtime.register_executor(LocalExecutor())


# Build function for our collection
# The 'inputs' parameter is explained in the example below.
def build_fn(config, inputs):
    return config["a"] + config["b"]


# Create a collection and register build_fn
add = runtime.register_collection("add", build_fn=build_fn)


# Invoke computations, collection.ref(...) creates a "reference into a collection",
# basically a pair (collection, config)
# When reference is provided, compute returns instance of Entry that
# contains attribute 'value' with the result of build function
result = runtime.compute(add.ref({"a": 1, "b": 2}))
print(result.value)  # prints: 3

# Invoke more compututations at once
result = runtime.compute([add.ref({"a": 1, "b": 2}),
                          add.ref({"a": 2, "b": 3}),
                          add.ref({"a": 4, "b": 5})])
print([r.value for r in result])  # prints: [3, 5, 9]
```

## Example 2: Dependencies

ORCO allows you to define dependencies between computations. When a computation
`A` depends on a computation `B`, `A` will be executed only after `B` has been
completed and the result of `B` will be passed as an additional input to `A`.

Besides the `build function`, a collection might have a `dependency function`,
which receives a single configuration and returns the dependencies that must be
completed before this configuration can be executed. The build function will
then receive the result value of the specified dependencies in its `inputs`
parameter.

In the example, assume that we have an expensive simulation. For the sake of
simplicity, we will parametrize it by one parameter. Configurations for it will
look like `{"p": <PARAMETER-OF-SIMULATION>}`.

We define collection of simulations as follows:

```python
from orco import Runtime, LocalExecutor
import time

runtime = Runtime("./mydb")
runtime.register_executor(LocalExecutor())

def simulation_run(config, inputs):
    # Fake a computation
    result = random.randint(0, config["p"])
    time.sleep(1)
    return result

sims = runtime.register_collection("simulations", build_fn=simulation_run)
```

Now we define an "experiment" that includes a range of simulations that has to
be performed together with some post-processing. Configurations for experiments
will be as follows `{"from": <P-START>, "upto": <P-END>}` where `<P-START>`
and `<P-END>` defines a range in which we want to run simulations.

Because we want share simulations between experiments, experiment itself will
not perform the computation, but establish an dependency on "sims" collection.

```python
# Create dependencies for experiment
def experiment_deps(config):
    # Create refereces into "sims" with configurations:
    # {p: config["from"]}, {p: config["from"] + 1} .. {p: config["upto"]}
    return sims.refs([{"p": p} for p in range(config["from"], config["upto"])])

# Run experiment, sum resulting values as demonstration of a postprocessing
# of simulation
def experiment_run(config, inputs):
    return [s.value for s in inputs]

experiments = runtime.register_collection(
    "exeperiments", build_fn=experiment_run, dep_fn=experiment_deps)
```

Now if we run:

```python
# Run experiment with with simulation between [0, 10).
runtime.compute(experiments.ref({"from": 0, "upto": 10}))
```

The output will be as follows:

```
Scheduled tasks  |     # | Expected comp. time (per entry)
-----------------+-------+--------------------------------
exeperiments     |     1 | N/A
simulations      |    10 | N/A
-----------------+-------+--------------------------------
100%|██████████████████████████████████| 11/11 [00:03<00:00,  3.63it/s]
```

We see, that system performs one computation from collection "experiments" and ten from "simulations".
Simulations are automatically scheduled as result of experiment dependency.
Since our DB is empty, we have no prior information about previous run, the expected computation is not available (third column in the output).


```python
# Run experiment with with simulation between [7, 15).
runtime.compute(experiments.ref({"from": 7, "upto": 15}))
```

The output will be following:

```
Scheduled tasks  |     # | Expected comp. time (per entry)
-----------------+-------+--------------------------------
exeperiments     |     1 |      1ms +- 0ms
simulations      |     5 |     1.0s +- 1ms
-----------------+-------+--------------------------------
100%|███████████████████████████████████| 6/6 [00:02<00:00,  2.95it/s]
```

ORCO schedules five simulations, because simulations for parameters 7, 8, and 9 are already computed. We need to only compute five simulations in range 10-14.
Now we also see expected computation time for entries because we already have some results in the DB.

## ORCO browser

ORCO contains a web browser of computations and their results stored in the database.
It can be started by running:

```python
runtime.serve()
```

`serve` starts a local HTTP server (by default on port 8550) that allows inspecting
stored data in the database and observing tasks running in executors. It is completely safe to run computation(s) simultaneously with the server.

![Screenshot of ORCO browser](./imgs/browser-collection.png)

(The `serve` method is blocking. If you want to start the browser and run computations in the same script,
you can call `serve(nonblocking=True)` before calling `compute` on the `Runtime`.)

Continue to [CLI interface](cli.md)

