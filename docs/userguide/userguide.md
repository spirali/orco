
![Screenshot of ORCO browser](./imgs/orco.svg)

# ORCO

ORCO (Organized Computing) is a Python package for running computational
experiments and storing results.

ORCO ensures that already computed parts of the computation is not repeated and
every computation is run exactly even more instances of ORCO is concurently
running.

## Installation

TODO

## Getting started

First, we need to explain five basic terms:

- **configuration** is an JSON-like object that describes and fully identifies a
  computation. Ideally it should fully  descibe the computation. Example a good
  configuration for a machine learning experiment is
  `{"layers": [1024, 1024], "input": "mnist", "iterations": 800, "learning-rate": 0.01}`.

- **build function** is function that takes a configuration and creates a value

- **entry** is structure that combines configuration and its computed value (+
  some metadata)

- **collection** represents a set of entries. It may also have associated a
  build function creating new elements in collection.

- **reference** is "pointer" into a collection. Basically it is a pair
  (collection, configuration).


Let us have now a simple example for adding two numbers. We will use
configurations of a form: `{"a": <A>, "b": <B>}`, where `<A>` and `<B>` are
operands for addition, e.g.: `{"a": 2, "b": 5}` is a configuration that
produces `7` as result.

```python
from orco import Runtime, LocalExecutor

# Create a runtime environment for ORCO.
# All data will be stored in file on provided path.
# If file does not exists, it is created
runtime = Runtime("./mydb")


# Registering executor for running tasks.
# By default, it will use all local cores.
runtime.register_executor(LocalExecutor())


# Build function for our configurations
# ('inputs' is explained later)
def build_fn(config, inputs):
    return config["a"] + config["b"]


# Create a collection and register build_fn
add = runtime.register_collection("add", build_fn=build_fn)


# Invoke computations, collection.ref(...) creates a "reference into a collection",
# basically a pair (collection, config)
# When reference is provided, compute returns instance of Entry that
# contains attribute 'value' with the result of build function.
result = runtime.compute(add.ref({"a": 1, "b": 2}))
print(result.value)  # prints: 3

# Invoke more compututations at once
result = runtime.compute([add.ref({"a": 1, "b": 2}),
                          add.ref({"a": 2, "b": 3}),
                          add.ref({"a": 4, "b": 5})])
print([r.value for r in result])  # prints: [3, 5, 9]
```

**Important 1:** In the second call of `runtime.compute(..)`, the build function
is not called for configuration `{"a": 1, "b": 2}` as it was already computed in
first call of `.compute(..)` and stored in DB.

**Important 2:** If you run the script for the second time, the build function
will not be invokend not a single once, since all results are already in DB.

**Important 3:** You can run more scripts over the same DB, it is guaranteed
that every configuration will be computed exactly once


## ORCO Browser

ORCO contains a browser of stored data in DB. It can be started by running:

```python
runtime.serve()
```

It starts a local HTTP server (by default on port 8550) that allows inspect
stored data in DB and tasks running in the executor. It is completely safe to
run computatin(s) simultanously with the server.

![Screenshot of ORCO browser](./imgs/browser-collection.png)

## Command-line Interface

ORCO contains a simple command line interface over collections. It can be used
ass follows:

```python
from orco import Runtime, run_cli

runtime = Runtime("./mydb")

def build_fn(config, inputs):
    return config["a"] + config["b"]


runtime.register_collection("add", build_fn=build_fn)

run_cli(runtime)  # Start CLI interface
                  # (note that we did not register LocalExecutor, it is managed by run_cli)
```

Let us assume that the script above is saved as `adder_cli.py`, then we can
start computations by executing:

```
$ python3 adder_cli.py compute <COLLECTION_NAME> <CONFIG>
```

For our example, it can be:

```
$ python3 adder_cli.py compute add '{"a": 3, "b": 2}'
```

You can also start server for ORCO browser by following command:

```
$ python3 adder_cli.py serve
```

Help for other commands may be obtained by:

```
$ python3 adder_cli.py --help
```

## Dependencies

This section demonstrates a computation where computations may have
dependencies, i.e. running a computation needs results from other computations.

Beside "build function", a user may provide a "dependency function" that for a
given config returns dependencies that have to be computed before building this
configuration. When build function is called, then values of dependencies are
provided into build function through the second argument. When entry with
dependency function is computed, ORCO automatically computes (or loads from DB)
all necessary dependencies.

### Example: Tournament

As a demonstration, we use an example of training and evaluating AI players
(e.g. AlphaZero players) for a two player game. Training a player takes time, so
we do not want recompute already trained player. Also playes between players for
evaluating their strength can take some time, so we do not want to repeat them
as they are already computed.

We will use three collections:

* **players** contains trained players, configurations are just names of players
* **plays** contains results of a play between two players, configurations have
  a form `{"player1": <PLAYER1>, "player2": <PLAYER2>}`.
* **tournaments** defines tournament between a set of players. Configurations
  have a form `{"players": [<PLAYER-1>, <PLAYER-2>, ...]}`.

For the sake of simplicity, we replace real Machine Learning functions by a
dummy proxies.

```python
from orco import Runtime, run_cli
import random
import itertools

runtime = Runtime("mydb.db")


# Build function for "players"
def train_player(config, inputs):
    # We will simulate trained players by dictionary with key "strength"
    return {"strength": random.randint(0, 10)}


# Dependancy function for "plays"
# To play a game we need, we need both players
def game_deps(config):
    return [players.ref(config["player1"]), players.ref(config["player2"])]


# Build a function for "plays"
# Because of "game_deps", in the "inputs" we find two players
def play_game(config, inputs):
    # Simulation of playing a game between two players,
    # They just throw k-sided dices, where k is trength of the player
    # The difference of throw is the result

    # 'inputs' is a list of two instances of Entry, hence we use .value argument
    # to obtain resulting value of predecessing computation
    r1 = random.randint(0, inputs[0].value["strength"] * 2)
    r2 = random.randint(0, inputs[1].value["strength"] * 2)
    return r1 - r2


# Dependancy function "tournaments"
# For evaluating tournament, we need to know results of plays between
# each pair of players.
def tournament_deps(config):
    return [
        plays.ref({
            "player1": p1,
            "player2": p2
        }) for (p1, p2) in itertools.product(config["players"], config["players"])
    ]


# Build function for a tournament, return score for each player
def play_tournament(config, inputs):
    score = {}
    for play in inputs:
        player1 = play.config["player1"]
        player2 = play.config["player2"]
        score.setdefault(player1, 0)
        score.setdefault(player2, 0)
        score[player1] += play.value
        score[player2] -= play.value
    return score


players = runtime.register_collection("players", build_fn=train_player)
plays = runtime.register_collection("plays", build_fn=play_game, dep_fn=game_deps)
tournaments = runtime.register_collection(
    "tournaments", build_fn=play_tournament, dep_fn=tournament_deps)

run_cli(runtime)

```

Let us assume that script is saved as `tournament.py` then we can run:

```
$ python3 tournament.py compute tournaments '{"players": ["A", "B", "C"]}}'
```

When the line is executed, than players "A", "B", and "C" are trained, and
playes between each pair of players are played.

If we now execute the following command:

```
$ python3 tournament.py compute tournaments '{"players": ["A", "B", "C", "D"]}}'
```

ORCO will train only player "D" and perform plays where "D" is involved, other
players and plays are loaded from DB.


## Complex objects as dependencies

In the example above, we have returned a list of references as a result of a
dependency function, and we obtain a list of entries in the second argument of
build function.

ORCO allows to provide any object composed of dictionaries, lists, tuples, and
references as result of dependency function. The input argument in build
function will have the same structure, but all references will be replaces by
entries.

Example:

```python
# In this example, we assume that 'c' and 'd' are collections

# Dependancy function
def dep_fn(config):
    return {"abc": [c.ref(...), d.ref(...), c.ref(...)], "xyz": c.ref(...)}

# Build function, that is used for
def build_fn(config, inputs)
    # When this function is called, it inputs will look like:
    # {"abc": [<Entry from 'c'>, <Entry from 'd'>, <Entry from 'c'>], "xyz": <Entry from 'c'>}
```


## Complex objects in runtime.compute()

The same mechanism as for results of dependency function works also for
`runtime.compute(...)` method. It means that an JSON-like object with references
may be provided into `compute()` method and result is an object with the same
shape, except we references are replaced by entries.

Assume that we gain have the first example with adder. Then we can use following
code:

```python
# Result is a single entry
runtime.compute(add.ref({"a": 1, "b": 2}))

# Result is a list of two entries
runtime.compute([add.ref({"a": 1, "b": 2}), add.ref({"a": 4, "b": 5})])

# Result is a dictionary {"x": <Entry>, "y": <Entry>}
runtime.compute({"x": add.ref({"a": 1, "b": 2}),
                 "y": add.ref({"a": 4, "b": 5})})
```


## Removing entries

Entries can be removed collections via `runtime.remove(<reference>)` or
`runtime.remove_many(<list of refences>)`. ORCO wants to have inspectable computations,
it maintains invariant that all dependencies used to compute a value is fully in DB.
Therefore if you remove an entry from a collection, all its "downstream" dependencies is also removed.

Lets have the following example with three players, that was used to generate
plays P1, P2, and P3 and a tournaments T1 that used P1, P2, and P3 and T2 that
used just P2:

```

players:   A    B    C
          / \  / \  / \
          \--\/---\/--\\
plays:       P1   P2   P3
               \  /___/ \
                \//      \
tournaments:    T1       T2
```

If we remove P3 then also T1 and T2 will be removed. If we remove player B then
P1, P2, and T1 will be removed. If we remove T1 then only T1 will removed.

Entries can be also removed by `runtime.clear(<collection-refence>)` that
removes all entries in collection. Since invariant is still maitained, it also
remove all dependent values.


## Updating collection

Sometimes it is necessary to update configuration without recomputing values.
Usually it happens when a new key is introduced into configuration. For such
situations, there is "upgrade" of collections that allows this.

```python
collection = runtime.register_collection(...)

# Introduce key "version" into configuration
def uprage_config(config):
    config.setdefault("version", 1)
    return config

runtime.upgrade_collection(collection, upgrade_config)
```


## Configuration generators

ORCO comes with a simple configuration builder for situation when a set of
regular shaped configuration is necessary (for example for dependency functions,
or as argument for computation).

```python
from orco.cfgbuild import build_config

configurations = build_config({
    "$product": {
        "train_iterations": [100, 200, 300],
        "batch_size": [128, 256],
        "architecture": ["model1", "model2"]
    }
})
```

When we run this, `configurations` will contains:

```
[{'train_iterations': 100, 'batch_size': 128, 'architecture': 'model1'},
 {'train_iterations': 100, 'batch_size': 128, 'architecture': 'model2'},
 {'train_iterations': 100, 'batch_size': 256, 'architecture': 'model1'},
 {'train_iterations': 100, 'batch_size': 256, 'architecture': 'model2'},
 {'train_iterations': 200, 'batch_size': 128, 'architecture': 'model1'},
 {'train_iterations': 200, 'batch_size': 128, 'architecture': 'model2'},
 {'train_iterations': 200, 'batch_size': 256, 'architecture': 'model1'},
 {'train_iterations': 200, 'batch_size': 256, 'architecture': 'model2'},
 {'train_iterations': 300, 'batch_size': 128, 'architecture': 'model1'},
 {'train_iterations': 300, 'batch_size': 128, 'architecture': 'model2'},
 {'train_iterations': 300, 'batch_size': 256, 'architecture': 'model1'},
 {'train_iterations': 300, 'batch_size': 256, 'architecture': 'model2'}]
```

TODO: Document other operations other than $product.


## Collection reference

So far, we have created references on an object returned by
`register_collection(..)`, i.e.

```python
col1 = runtime.register("col1", ...)
col1.ref(config)
```

Sometimes, it can be troublesome to get access to this variable, e.g. in a build
function defined in different module. However, you can always create reference
to a collection just by instantiating, `CollectionRef`:

```python
from orco import CollectionRef

col1 = CollectionRef("col1")
col1.ref(config)
```

The only limitation is, that before the first usage of such a reference for
computation; collection with the fiven name has to be registered in the runtime
where the computation is invoked.


## Retrieving entries without computation

Entries can be also retreived without computation. Method `.get_entry` on
returns entry for given reference. If there is no stored entry in DB, then
`None` is returned; computation is not started.

```python
# Returns None if config is not in the collection
entry = runtime.get_entry(collection.ref(config))
```

## Fixed collection

Collection does not have to have an associated build function. In such case, we
called this collection as *fixed*.

New values can be inserted via method `.insert(..)` as follows:

```python

# Register fixed collection
collection = runtime.register_collection("my_collection")

# Insert two values
runtime.insert(collection.ref({"something": 1}), 123)
runtime.insert(collection.ref("abc"), "xyz")
```

When a reference into fixed collection occurs in the computation, the value for
the given configuration has to be loaded from DB otherwise, the computation is
cancelled.

(Note: Values can be inserted also for collection with build function, but
usually you want to run `.compute(..)` rather than `.insert(..)`)


## Exporting results

A collection can be exported into pandas dataframe as follows:

```python
from orco.export import export_collection_to_pandas

# Exporting collection with name "collection1"
df = export_collection_to_pandas(runtime, "collection1")
```

## Configuration equivalence

Configuration may be composed of dictionaries, lists, tuples, integers, floats
and strings. Two configurations are equivalent if they are equivalent in Python
except two exceptions:

* Lists and tuples are not distinguished, i.e. `[1,2,3]` and `(1, 2, 3)` are
  equivalent.
* Dictionary keys starting with underscore is ignored:
  `{"iterations": 100, "_note": "Foo!"}` and `{"iterations": 100}` are equivalent.


## Best practises

### Versioning

Sometimes you may want recomputing something without losing old results. You can
do manage it by introductiong versions as a simple key in configuration, e.g.
assume that we have configurations like `{"param-a": 1, "param-b": 0.3}`, when
we add version, we will have configurations like `{"param-a": 1, "param-b": 0.3,
version: 1}`.

In case of collections with dependency function functions, we need to add
propagating of version.

```python
# Original version
def make_deps(config):
    return [col1.ref(...), col2.ref(...)]

# Version propagating
def make_deps(config):
    return [col1.ref(..., config["version"]), col2.ref(..., config["version"])]
```

Since our original data (that we do not want lose) did not have version, we need
to add version to them. It can be dome by upgrading collection (see section
about upgrading).

### Replications

Let us assume that we have non-deterministic build function (e.g. a sampling
function) and we need to run more samples with the same configuration. In ORCO,
each configuration may have exatly one result, hence a simple option is to store
a list of results into configuration. But it would fix a number of samples for a
configuration and prevents later reusage of results.

We can solve it by introducing new key in the configuration to distinguish each
sample. We will `replication` in this example.

```python
# Build function for sampler
def run_sampler(config, inputs):
    # In the configration we ignore 'replication' key
    return doSomethingNondeterministic(config)

samples = runtime.register_collection("samples", build_fn=run_sampler)

# This is a dependency function that asks for 20 replicated entries from "samples"
def dep_fn1(config):
    return samples.refs(
        [{<sample-configuration...>, "replication": i}
         for i in range(20)])
```