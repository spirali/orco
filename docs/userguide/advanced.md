# Advanced usage

In this guide we show additional functionality offered by ORCO.

- [Advanced usage](#advanced-usage)
  - [Attaching data](#attaching-data)
  - [Removing job](#removing-jobs)
  - [States of jobs](#states-of-jobs)
  - [Upgrading builders](#upgrading-builders)
  - [Retrieving jobs without computation](#retrieving-jobs-without-computation)
  - [Fixed builders](#fixed-builders)
  - [Exporting results](#exporting-results)
  - [Configuration equivalence](#configuration-equivalence)
  - [Capturing output](#capturing-output)
  - [JobSetup](#jobsetup)
  - [Configuration generators](#configuration-generators)


## Attaching data

TODO

## Removing jobs

Generally removing a data while maintaining consistency may be a little bit tricky. Let us start with a demonstration where is the problem.

### Why naive remove does not work?

Let us first show why naive removing does not work. Consider the following example with three builders, one for generating a random
coin tosses, one is counting heads in the sample and one is counting tails in the sample.

```python
@builder()
def sample_of_coin_tosses(sample_size):
    return [random.randint(1, 2) for _ in range(sample_size)]

@builder()
def heads_count_in_sample(sample_size):
    sample = sample_of_coin_tosses(sample_size)
    yield
    return [t.value for t in sample].count(1)

@builder()
def tails_count_in_sample(sample_size):
    sample = sample_of_coin_tosses(sample_size)
    yield
    return [t.value for t in sample].count(2)
```

Let us assume that we do the following computation:

```python
a = runtime.compute(heads_count_in_sample(10))
b = runtime.compute(tails_count_in_sample(10))
```

Obviously the ``a.value + b.value`` should be 10.

But assume that we have a method ``naive_remove(job)`` that just removes stored result job from the database. Let us consider the following scenario:

```python
# ! this is only a demonstration of a problem, naive remove is not implemented in ORCO !
runtime.naive_remove(sample_of_coin_tosses(10))
runtime.naive_remove(heads_count_in_sample(10))
```

After that only the result for number of tails remains.
If we now call again the the code above:

```python
a = runtime.compute(heads_count_in_sample(10))
b = runtime.compute(tails_count_in_sample(10))
```

The first line causes a computation and it induces also
to computing a new sample of coin tosses.
However, the second result is just loaded from the database as it already exists.
But ``a`` and ``b`` where computed on different samples!
So now you can get inconsistent results. For example, ``a.value + b.value`` does not hold.

This example is artificial, but this situation easily occur everywhere where you cannot reproduce you result exactly: in training neural network models, sampling a data sets, using some randomized heuristics, or simply just by library upgrade.
When you cannot achieve the same result every time, naive remove does not work and causes inconsistencies.

### Removing operation in ORCO

There are three types of removing jobs from database in ORCO: *drop*, *archive*, and *free*.

The simplest one is *drop*, it simply removes specified job from the database together with **all** jobs that recursively depends on the job. Removing dependant jobs prevents the inconsistency in the section above.

```python

@builder()
def step1(x):
    ...

@builder()
def step2(x):
    a = step1(x)
    yield
    ...

# This computes step2(10) together with step1(10) as its dependancy
runtime.compute(step2(10))

# This drops step1(10) and all its consumers,
# that is step2(10) in this case
runtime.drop(step1(10))
```

Another option is *archive*, that sets a job and **all** its recursive
dependancies into state "arhived". Such a job remains in database but it is
ignored by ``compute``; builder's function create a new job (in a normal
non-archived state). It also mean no new computation may depend on an archived
job result. Therefore the effect on computation is similar to *drop*, but you
can still reach archived data (e.g. in browser).

[Note that archive operation is reversible. Archived data may be put again back
to active state, if it does not causes conflicts with the current active data.
The restoring operation is not yet implemented.
]

```python
# This computes step2(10) together with step1(10) as its dependancy
runtime.compute(step2(10))

# This set to archived state step1(10) and all its dependants,
# that is step2(10) in this case
runtime.archive(step1(10))

# Computes new step2(10), the archived job is ignored
runtime.compute(step2(10))
```

Sometimes we want to free a data from the database but still use jobs that
depends on this job for further computation. It is possible with *free*
operation. Free removes all computed data by a job. However, to prevent the
scenario from the section above, *free* operation leaves job metadata in the
database and ORCO rejects any computation that would recompute the job. If you
need recomputation of freed job, you have to archive or drop it.


```python
# This computes step2(10) together with step1(10) as its dependancy
runtime.compute(step2(10))


# Sets step1(10) into "freed" state and removes. Job step2(10) remains intact
runtime.free(step1(10))

# This does not trigger a computation, it just load step2(10) from DB.
runtime.compute(step2(10))

# This throws an error because the computation needs recomputing a freed object
runtime.compute(step1(10))
```

### Bulk removing

All methods have "_many" variant for dropping/archiving/freeing more jobs at once

```python
runtime.free_many([step1(20), step2(30)])
runtime.archive_many([step1(30), step2(40)])
runtime.drop_many([step1(50), step2(60)])
```

## States of jobs

<img src="imgs/states.png">

Each job is in one of the following state:

- *Announced* - The job was scheduled to computation.
- *Running* - The job is currently executed.
- *Finished* - Job is finished and its result are available
- *Failed* - An error occured during job's computation
- *Freed* - Job's data was freed by calling `.free()` method. It cannot be used for computation any more while it also blocks recreaction of the task (details in section about freeing objects).
- *Archived/Achived free* - Job was archived by calling ``.archive()``. It was removed from the set of active jobs. Its data are preserved but it cannot be used only for new computations.

For each builder and configuration, there is at most one job that is in *active* state (Announced, Running, Finished, Freed).
This is invariant that helps to avoid recomputing already computed data, prevent simultanous computation of the same computation from different processes, and helps with consistency of results in case of data removal.
And there may be unlimited number of jobs in non-active states.

Most methods like ``.compute()`` or ``.read()`` looks only for jobs in active states and ignore others. There are two exceptions: If you want to get all jobs under a given configuration, there is a method ``.read_jobs()``, e.g.:

```python

@my_builder(x):
    ...

runtime.read_jobs(my_builder(10))
```

The second exception is ``.drop(..)`` that removes jobs in all states.

## Upgrading builders

Sometimes you have already computed some results, only to find out that you need
to introduce a new parameter to your configurations. By default, this would
invalidate the old results, because ORCO would observe a new configuration and
it wouldn't know that it should be equivalent with the already computed results.

For such situations, there is an `upgrade_builder` function which will allow you
to change the configurations of already computed results in the database
"in-place". Using this function, you can change already computed configurations
without recomputing them again.

```python

@orco.builder()
def my_builder(config):
    ...

# Introduce a "version" key to all computed configurations of a builder
def uprage_config(config):
    config.setdefault("version", 1)
    return config

runtime.upgrade_builder(my_builder, upgrade_config)
```

Note that the upgrade is atomic, i.e. if an exception happens during the upgrade, the changes made so far
will be rolled back and nothing will be upgraded.


## Retrieving jobs without computation

If you want to query the database for already computed jobs, without starting
the computation for configurations that are not yet computed, you can use the
`read` method on a `Runtime`. If a job exists in the finished stated, then it attaches job to the database entry. If there is not such job, an exception is thrown.

The method `try_read` works similarly, but return `None` is not in the database.


## Fixed builders

Builders do not need to have an associated build function. Such builders are called *fixed*
and are created by passing `None` for the builder function.
You can insert values into them method using the `insert` function, which receives a configuration
and its result value:

```python

my_builder = runtime.register_builder(Builder(None, "my_builder"))

# Insert two values
runtime.insert(my_builder({"something": 1}), 123)
runtime.insert(my_builder("abc"), "xyz")

```

When a reference into a fixed builder occurs in a computation, the value for
the given configuration has to be present in the database, otherwise the computation is
cancelled (because the runtime doesn't know how to produce a result value from a configuration
without the build function).

> Values can be inserted also for builders with a build function, but usually you want to run
> `compute` rather than `insert` for this.

## Exporting results

A builder can be easily exported into a Pandas `DataFrame`:

```python
from orco.ext.pandas import export_builder_to_pandas

# Exporting builder with name "builder1"
df = export_builder_to_pandas(runtime, "builder1")
```

## Configuration equivalence

A configuration may be composed of dictionaries, lists, tuples, integers, floats, strings and booleans.
To check if a configuration has already been computed, it is serialized into a string `key` and queried
against the database.

In general, two configurations are equivalent if they are equivalent in Python (using the `==` operator),
with some exceptions:

* Lists and tuples are not distinguished, i.e. `[1,2,3]` and `(1, 2, 3)` are considered
  equivalent.
* Dictionary keys have to be strings, otherwise `{1: "a"}` and `{"1": "a"}` would be considered
  equivalent.
* Dictionary keys starting with an double underscore are ignored:
  `{"iterations": 100, "__note": "Foo!"}` and `{"iterations": 100}` are equivalent.


## Capturing output

TODO

## JobSetup

TODO

## Configuration generators

ORCO comes with a simple configuration builder for situations when you want to build
non-trivial matrices of parameter combinations (e.g. cartesian products with some "holes").
It enables you to build complex Python objects from a simple declarative description.

The builder is used via the `build_config` function. It expects a dictionary containing
JSON-like objects (numbers, strings, bools, lists, tuples and dictionaries are allowed). By default,
it will simply return the input dictionary:

```python
from orco.cfggen import build_config

configurations = build_config({
    "batch_size": 128,
    "learning_rate": 0.1
})
# configurations == { "batch_size": 128, "learning_rate": 0.1 }
```

To build more complex combinations, the builder supports special operators
(similar to [MongoDB operators](https://docs.mongodb.com/manual/reference/operator/query/)).
An operator is a dictionary with a single key starting with `$`. The value of the
key contains parameters for the operator. The operator will return a Python object after it is
evaluated by the builder. The operators can be arbitrarily nested.

The following operators are supported:
* `$range: int | list[int]`:
Evaluates to a list of numbers, similarly to the built-in `range` function.
```python
build_config({"$range": 3})         # evaluates to [0, 1, 2]
build_config({"$range": [1, 7, 2]}) # evaluates to [1, 3, 5]
```
* `$ref: str`:
Evaluates to the value of a top level key in the input dictionary. It is useful when you want
to use a specific key multiple things, but you want to define it only once.
```python
build_config({
    "graphs": ["a", "b", "c"],
    "bfs": {
        "algorithm": "bfs",
        "graphs": {"$ref": "graphs"}
    }
})
"""
evaluates to {
    "graphs": ["a", "b", "c"],
    "bfs": {
        "algorithm": "bfs",
        "graphs": ["a", "b", "c"]
    }
}
"""
```
* `$+: list[iterable]`:
Evaluates to a list with the concatenation of its input parameters (i.e. behaves like
`list(itertools.chain.from_iterable(params))`).

In this example you can see how it can be combined with a nested `$ref` operator.
```python
build_config({
    "small_graphs": ["a", "b"],
    "large_graphs": ["c", "d"],
    "graphs": {"$+": [{"$ref": "small_graphs"}, {"$ref": "large_graphs"}]}
})
"""
evaluates to {
    "small_graphs": ["a", "b"],
    "large_graphs": ["c", "d"],
    "graphs": ["a", "b", "c", "d"]
}
```
`$zip: list[iterable]`:
Evaluates to a list of zipped values from the input parameters, i.e. behaves like `list(zip(*params))`.
```python
build_config({
    "a": {"$zip": [[1, 2], ["a", "b"]]}
})
"""
evaluates to {
    "a": [(1, "a"), (2, "b")],
}
```
`$product: dict[str, iterable] | list[iterable]`:
Evaluates to a list containing a cartesian product of the input parameters.
If the input parameter is a list of iterables, it behaves like `list(itertools.product(*params))`:
```python
build_config({
    "$product": [[1, 2], ["a", "b"]]
})
# evaluates to [(1, "a"), (1, "b"), (2, "a"), (2, "b")]
```
If the input parameter is a dictionary, it will evaluate to a list of dictionaries with the same
keys, with the values forming the cartesian product:
```python
build_config({
    "$product": {
        "batch_size": [32, 64],
        "learning_rate": [0.01, 0.1]
    }
})
"""
evaluates to [
    {"batch_size": 32, "learning_rate": 0.01},
    {"batch_size": 32, "learning_rate": 0.1},
    {"batch_size": 64, "learning_rate": 0.01},
    {"batch_size": 64, "learning_rate": 0.1}
]
"""
```

When you nest other operators inside a `$product`, each element of the inner operator will be
evaluated as a single element of the outer product, like this:
```python
build_config({
    "$product": {
        "a": {
            "$product": {
                "x": [3, 4],
                "y": [5, 6]
            }
        },
        "b": [1, 2]
    }
})
"""
evaluates to [
    {'a': {'x': 3, 'y': 5}, 'b': 1},
    {'a': {'x': 3, 'y': 5}, 'b': 2},
    {'a': {'x': 3, 'y': 6}, 'b': 1},
    {'a': {'x': 3, 'y': 6}, 'b': 2},
    {'a': {'x': 4, 'y': 5}, 'b': 1},
    {'a': {'x': 4, 'y': 5}, 'b': 2},
    {'a': {'x': 4, 'y': 6}, 'b': 1},
    {'a': {'x': 4, 'y': 6}, 'b': 2}
]
"""
```

If you instead want to materialize the inner operator first and use its final value as a single
element of the product, simply wrap the nested operator in a list:

```python
build_config({
    "$product": {
        "a": [{
            "$product": {
                "x": [3, 4],
                "y": [5, 6]
            }
        }],
        "b": [1, 2]
    }
})
"""
evaluates to [
    {'a': [{'x': 3, 'y': 5}, {'x': 3, 'y': 6}, {'x': 4, 'y': 5}, {'x': 4, 'y': 6}], 'b': 1},
    {'a': [{'x': 3, 'y': 5}, {'x': 3, 'y': 6}, {'x': 4, 'y': 5}, {'x': 4, 'y': 6}], 'b': 2}
]
"""
```

There is also a `build_config_from_file` function, which parses configurations from a path to a JSON file
containing the configuration description.


Continue to [Best practices](best-practices.md)