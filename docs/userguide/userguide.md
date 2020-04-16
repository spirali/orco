
![Screenshot of ORCO browser](./imgs/orco.png)

# ORCO

ORCO (Organized Computing) is a Python package for defining, executing and
persisting computations in a straightforward fashion.

ORCO prevents problems like accidentally overwriting data, computing already
computed data, non-invalidating old computations when the input data are
changed, overwriting data in simultaneous computations.

ORCO combines an execution engine for running computations with a database that
stores their results and allows you to query them. It forces you to be very
explicit in specifying how is your computation defined and what are its
dependencies. This makes the code a bit verbose, but facilitates reproducibility and consistency of your experiments.

## Installation

### Requirements

ORCO needs Python 3.5+.

### Installation via pip

```
pip3 install orco
```

## User guide
* [Getting started](getting-started.md)
* [CLI interface](cli.md)
* [Advanced usage](advanced.md)
* [Best practices](best-practices.md)
* [Extensions](extensions.md)
