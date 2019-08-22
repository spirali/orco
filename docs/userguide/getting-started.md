# Getting started

In this example we will define a simple computation that adds two numbers. After you run the computation,
its results will be stored in a database to avoid recomputation in the future.

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
config_a = { "a": 1, "b": 2 }
config_b = { "a": 3, "b": 4 }
``` 

We have now defined a database for storing computation results, an executor for running the computations,
a collection that defines a simple `add` computation and two configurations for `add` that we want to compute.

To compute a result, we have to give a **reference** to the runtime. Reference is an object specifying
what type of computation you want to perform (the collection) and with what input you want to compute it
(the configuration). Giving a reference to the runtime will produce the desired result. The returned
object will always be an instance of the `Entry` class, which contains both the input configuration
(`config`) and mainly the result value (`field`).

```python
# Compute the result of `add` with the input `config_a`
result = runtime.compute(add.ref(config_a))
print(result.value)  # prints: 3
```

Because this was the first time we asked for this specific computation, the build function was invoked
with the given configuration and its return value was stored into the database.
When we run the same computation again, the result will be provided directly from the database.
```python
result = runtime.compute(add.ref(config_a)) # build_fn is not called again here
```

So far we have computed only one configuration, usually you want to compute many of them:
```python
result = runtime.compute([add.ref({"a": 1, "b": 2}),
                          add.ref({"a": 2, "b": 3}),
                          add.ref({"a": 4, "b": 5})])
print([r.value for r in result])  # prints: [3, 5, 9]
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
# The 'inputs' parameter is explained in the Advanced usage guide
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
