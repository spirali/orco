# Advanced usage

In this guide we show additional functionality offered by ORCO.

* [Removing computed results](#removing-entries)
* [Upgrading collections](#upgrading-collections)
* [Generating configurations](#configuration-generators)
* [Collection references](#collection-reference)
* [Retrieving results without computation](#retrieving-entries-without-computation)
* [Fixed collections](#fixed-collections)
* [Export to Pandas](#exporting-results)
* [How are configurations compared](#configuration-equivalence)
* [Using references in configurations](#references-in-configurations)

## Removing entries

You can remove already computed entries using `runtime.remove(<reference>)` or
`runtime.remove_many(<list of refences>)`. ORCO maintains an invariant that all dependencies used to
compute a value are stored in the database. Therefore, if you remove an entry from a collection,
all its "downstream" dependencies will also be removed.

Let's have the following example with four players who were used to generate
games G1, G2, and G3 and a tournament T1 that used G1, G2 and G3 and T2 that
used just G2:

```

players:   A    B    C    D
            \  / \  / \  /
             \/   \/   \/
games:       G1   G2   G3
               \  /___/ \
                \//      \
tournaments:    T1       T2
```

If we remove G3, then also T1 and T2 will be removed because they depended on it.
If we remove player B then G1, G2 and T1 will be removed. If we remove T1 then only T1 will removed because
nothing depends on it.

All entries of a collection can be removed using `runtime.clear(<collection-refence>)`.
The invariant is also maintained in this case, so any entries that depended on this collection will
be removed too.

## Upgrading collections

Sometimes you have already computed some results, only to find out that you need to introduce a new
key to your configurations. By default, this would invalidate the old results, because ORCO would
observe a new configuration and it wouldn't know that it should be equivalent with the already computed
results.

For such situations, there is an `upgrade_collection` function which will allow you to change the
configurations of already computed results in the database "in-place". Using this function, you can
change already computed configurations without recomputing them again.

```python
collection = runtime.register_collection(...)

# Introduce a "version" key to all computed configurations of a collection
def uprage_config(config):
    config.setdefault("version", 1)
    return config

runtime.upgrade_collection(collection, upgrade_config)
```

Note that the upgrade is atomic, i.e. if an exception happens during the upgrade, the changes made so far
will be rolled back and nothing will be upgraded.

## Configuration generators

ORCO comes with a simple configuration builder for situations when you want to build
non-trivial matrices of parameter combinations (e.g. cartesian products with some "holes").
It enables you to build complex Python objects from a simple declarative description.

The builder is used via the `build_config` function. It expects a dictionary containing
JSON-like objects (numbers, strings, bools, lists, tuples and dictionaries are allowed). By default,
it will simply return the input dictionary:
```python
from orco.cfgbuild import build_config

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

## Collection reference

Sometimes you do not have access to the `collection` object, but you still want to create
a `reference` to it (for example if you create these references in a different module than where
the collection is defined).

You can use the `CollectionRef` class to create references to a collection simply by knowing
the name of the collection:

```python
from orco import CollectionRef

col1 = CollectionRef("col1")
col1.ref(config)
```

The only limitation is that the collection with the specified name has to be registered in
the runtime by the point that you try to `compute` the created references.

## Retrieving entries without computation

If you want to query the database for already computed entries, without starting the computation
for configurations that are not yet computed, you can use the `get_entry` function on a `Runtime`.

If no computed entry is stored in the database for a given configuration, `None` will be returned.

```python
# Returns None if config is not in the collection
entry = runtime.get_entry(collection.ref(config))
```

## Fixed collections

Collections do not need to have an associated build function. Such collections are called *fixed*.
You can insert values into them method using the `insert` function, which receives a configuration
and its result value:

```python
# Register a fixed collection
collection = runtime.register_collection("my_collection")

# Insert two values
runtime.insert(collection.ref({"something": 1}), 123)
runtime.insert(collection.ref("abc"), "xyz")
```

When a reference into a fixed collection occurs in a computation, the value for
the given configuration has to be present in the database, otherwise the computation is
cancelled (because the runtime doesn't know how to produce a result value from a configuration
without the build function).

> Values can be inserted also for collections with a build function, but usually you want to run
> `compute` rather than `insert` for this.

## Exporting results

A collection can be easily exported into a Pandas `DataFrame`:

```python
from orco.export import export_collection_to_pandas

# Exporting collection with name "collection1"
df = export_collection_to_pandas(runtime, "collection1")
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
* Dictionary keys starting with an underscore are ignored:
  `{"iterations": 100, "_note": "Foo!"}` and `{"iterations": 100}` are equivalent.

## References in configurations

Even though you usually only use `references` as parameters for `compute` and as a return value
from a `dependency function`, you can in fact pass an arbitrary JSON-like Python object.

The `compute` function computes all `references` in the object and evaluates them to an `entry`.
All `references` contained in the return value of `dependency function` are evaluated to a computed
`entry` and the final value is provided as the `inputs` parameter to a `build function`.

Example with `compute`:
```python
# Result is a single entry
runtime.compute(add.ref({"a": 1, "b": 2}))

# Result is a list of two entries
runtime.compute([add.ref({"a": 1, "b": 2}), add.ref({"a": 4, "b": 5})])

# Result is a dictionary {"x": <Entry>, "y": <Entry>}
runtime.compute({"x": add.ref({"a": 1, "b": 2}),
                 "y": add.ref({"a": 4, "b": 5})})
```

Example with a `dependency function`:
```python
# In this example, we assume that 'c' and 'd' are collections

# Dependency function
def dep_fn(config):
    return {"abc": [c.ref(...), d.ref(...), c.ref(...)], "xyz": c.ref(...)}

# Build function, that is used for
def build_fn(config, inputs):
    # When this function is called, its inputs will look like:
    # {"abc": [<Entry from 'c'>, <Entry from 'd'>, <Entry from 'c'>], "xyz": <Entry from 'c'>}
```

Continue to [Best practices](best-practices.md)