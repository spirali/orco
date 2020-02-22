# Getting started

## Example 1: Adder

In this example we will define a simple computation that adds two numbers. After
you run the computation, its results will be stored in a database to avoid
recomputation in the future.

The database with computation results is represented by a `Runtime`, which
receives a path to a database file where the results will be stored.
In this chapter, we use a default 'job runner' that executes task locally.
It is spawned automatically on demand by the runtime, so no further action is needed.


```python
import orco

# Create a new runtime
# If the database file does not exist, it will be created
runtime = orco.Runtime("./mydb")
```

Now you can start defining your computations. Computations in ORCO are stored in **builder**s.
A builder stores the results of a single type of computation (preparing a dataset,
training a neural network, benchmarking or compiling a program, ...). 

Let's define a trivial builder that will returns of a computation which simply adds two numbers.

```python
# You can ignore `inputs` for now, it is explained in the Advanced usage guide
import orco

@orco.builder()
def adder(config):
    return config["a"] + config["b"]

runtime = orco.Runtime("./mydb")
```

Builder is basically a function that takes a **configuration** and returns may return any pickable Python object.
A configuration has to be a Python value
that is easily serializable (i.e. JSON-like - numbers, strings, booleans, lists, tuples or dictionaries).
The configuration should contain everything that is necessary to produce a result of the builder.

The builder in our example expects a directory with keys "a" and "b" as a configuration. 
Here we create two different configurations:

```python
config_1 = { "a": 1, "b": 2 }
config_2 = { "a": 3, "b": 4 }
```

We have now defined a database for storing computation results,
a builder that defines a simple `adder` computation 
and two configurations for `adder` that we want to compute.

To invoke a computation we need to create an **entry**. 
Entry is created by calling a builder. Calling a builder is a very lightweight operation that just creates a reference
to a computation without invoking a builder's function.
To really invoke a computation we need to call ``runtime.compute(entry)``.
After the computation is over, the entry will contain a result in the attribute ``value``.

```python
# Compute the result of `add` with the input `config_1`
result = runtime.compute(adder(config_1))
print(result.value)  # prints: 3
```

A builder establish a persistent cache for each builder that is stored in the database.
If you give a configuration to the builder,
it will either return an already computed result stored in the database or invoke
the builder's function if the result was not computed for the specified input yet.

Because this was the first time we asked for this specific computation, the build function was invoked
with the given configuration and its return value was stored into the database.
When we run the same computation again, the result will be provided directly from the database.
```python
result = runtime.compute(adder(config_1))  # build_fn is not called again here
```

So far we have computed only one configuration, usually you want to compute many of them
```python
result = runtime.compute_many([adder({"a": 1, "b": 2}),
                               adder({"a": 2, "b": 3}),
                               adder({"a": 4, "b": 5})])
print([r.value for r in result])  # prints: [3, 5, 9]]
```

Computing more instances at once allows to paralelize the computation and reduce an overhead of starting a computation.

This concludes basic ORCO usage. You define a builder with a build function and ask the builder
to give you results for your desired configurations.

The whole code example is listed here:

```python
import orco

# Register builder
@orco.builder()
def adder(config):
    return config["a"] + config["b"]


# Create a runtime environment for ORCO
# All data will be stored in file on provided path
# If the file does not exists, it will be created
runtime = orco.Runtime("./mydb")


# When a task is provided, compute returns instance of Entry that
# contains attribute 'value' with the result of the build function
result = runtime.compute(adder({"a": 1, "b": 2}))
print(result.value)  # prints: 3

result = runtime.compute_many([adder({"a": 1, "b": 2}),
                               adder({"a": 2, "b": 3}),
                               adder({"a": 4, "b": 5})])
print([r.value for r in result])  # prints: [3, 5, 9]
```


## Example 2: Dependencies

ORCO allows you to define dependencies between computations. When a computation
`A` depends on a computation `B`, `A` will be executed only after `B` has been
completed and the result of `B` will be passed as an additional input to `A`.

In the example, assume that we have an expensive simulation. For the sake of
simplicity, we will parametrize it by one parameter. Configurations for it will
look like `{"p": <PARAMETER-OF-SIMULATION>}`.

We define builder of simulations as follows:

```python
import orco
import time
import random

@orco.builder()
def simulation(config, inputs):
    # Fake a computation
    result = random.randint(0, config["p"])
    time.sleep(1)
    return result
```

Now we define an "experiment" that includes a range of simulations that has to
be performed together with some post-processing. Configurations for experiments
will be as follows `{"from": <P-START>, "upto": <P-END>}` where `<P-START>`
and `<P-END>` defines a range in which we want to run simulations.

Because we want share simulations between experiments, experiment itself will
not perform the computation, but establish an dependency on "simulation" builder.

```python


# Run experiment, sum resulting values as demonstration of a postprocessing
# of simulation
@orco.builder()
def experiment(config):
    sims = [simulation({"p": p}) for p in range(config["from"], config["upto"])]
    yield
    return sum([s.value for s in sims])
```

When dependencies are used, there are two phases:

* Inputs gathering (before ``yield``)
* Computation (after ``yeild``)

You call other builders in the first phase. They are gathered as dependencies for the computation.
This phase should be quick without any heavy computation. This phase is possibly invoked more than once.

The second phase is computation phase where the main part of the computation should
happen. It may freely used entries created in the first phase. It is not allowed to create new entries by calling 
builders (an exception is thrown in such case).
 
Now if we run:

```python

runtime = orco.Runtime("my.db")

# Run experiment with with simulation between [0, 10).
runtime.compute(experiment({"from": 0, "upto": 10}))
```

The output will be as follows:

```
Scheduled jobs   |     # | Expected comp. time (per entry)
-----------------+-------+--------------------------------
exeperiments     |     1 | N/A
simulations      |    10 | N/A
-----------------+-------+--------------------------------
100%|██████████████████████████████████| 11/11 [00:03<00:00,  3.63it/s]
```

We see, that system performs one computation from builder "experiment" and ten from "simulation".
Simulations are automatically scheduled as result of experiment dependency.
Since our DB is empty, we have no prior information about previous run, the expected computation is not available (third column in the output).


```python
# Run experiment with with simulation between [7, 15).
runtime.compute(experiment({"from": 7, "upto": 15}))
```

The output will be following:

```
Scheduled jobs   |     # | Expected comp. time (per entry)
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
stored data in the database and observing jobs running in executors. It is completely safe to run computation(s) simultaneously with the server.

![Screenshot of ORCO browser](./imgs/browser-builder.png)

(The `serve` method is blocking. If you want to start the browser and run computations in the same script,
you can call `serve(nonblocking=True)` before calling `compute` on the `Runtime`.)

Continue to [CLI interface](cli.md)
