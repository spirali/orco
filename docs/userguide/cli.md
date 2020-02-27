# Command-line Interface

ORCO contains a simple command line interface designed for running computations.

To use it, you have to pass your created runtime It can be used
as follows:

```python
import orco

@orco.builder()
def adder(config):
    return config["a"] + config["b"]


orco.run_cli()  # Start CLI interface
```

In case you want to create your own `Runtime`, you can pass it to `run_cli`, otherwise,
a default `Runtime` is created for you with the db specified by `--db` parameter (or in-memory if none is specified).

Let us assume that the script above is saved as `adder_cli.py`, then we can
start computations by executing:

```sh
$ python3 adder_cli.py compute <BUILDER_NAME> <CONFIG>
```

For our example, it can be:

```sh
$ python3 adder_cli.py compute adder '{"a": 3, "b": 2}'
```

You can also start server for ORCO browser by following command:

```sh
$ python3 adder_cli.py serve
```

Help for other commands may be obtained by:

```sh
$ python3 adder_cli.py --help
```

Continue to [Advanced usage](advanced.md)