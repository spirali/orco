# Command-line Interface

ORCO contains a simple command line interface designed for running computations.

To use it, you have to pass your created runtime It can be used
as follows:

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

Continue to [Advanced usage](advanced.md)