Here we show some best practices that you should take in mind when using ORCO.


## Versioning

If you need systematically work with older versions and "archive" operation is not enough, we suggest to introduce a `version` key in configurations.


```python
# Original version
@orco.builder()
def my_builder(x):
    ...

# Builder with version
@orco.builder()
def my_builder(x, version):
    ...
```

Now when you want to recompute the builder, just bump the version
in your input configurations and the results will be computed from scratch.
Version argument also allows to simple load any older version.

In the case of builders with dependencies, you may need to propagate the version to all upstream builders.

```python
# Original version
@orco.builder()
def another_builder(x):
    dep = my_builder(x + 1)
    yield
    ...

# Version propagating
@orco.builder()
def another_builder(x, version):
    dep = my_builder(x + 1, version)
    # Propagating own version to the dependencies
    yield
    ...
    ...
```

If you already have some results computed without a `version` key, you can
[upgrade your builder](advanced.md#upgrading-builders) to add a `version` key
with some default value to the existing results.

## Computing multiple samples

If your computations are non-deterministic, you may want to execute them
multiple times to gather multiple samples so that you can (for example) average
the results. An example of this might be a benchmark that you want to run
multiple times (each time with the same inputs).

A simply (but non-ideal) is to run the computation multiple times in your build
function and return a list of results, but that would fix the number of samples
that you have collected. This makes adding new results later more complicated
than necessary.

A better solution might be to produce just one sample from your build function,
but introduce a key in your input configurations that will distinguish each
sample. For example you can add a parameter named `sample` and compute N
configurations differing only by the value of `sample` (which could for example
go from `0 ` to `N-1`). The value of the `sample` key itself doesn't have to be
used by the build function, it serves only to distinguish the input
configurations from each other.

In the following example, we compute 20 samples with the same input parameters.

```python
# Build function for sampler
orco.builder()
def sampler(param1, param2, sample):
    return doSomethingNondeterministic(param1, param2)

results = orco.compute_many(
    [sampler(10, 12, sample=i) for i in range(20)])
```

Later, when we find out that we need more samples, we can easily increase the
sample count. Only the new samples will be computed (the first 20 ones are
already stored in the database).

```python
# only the 10 new samples will be computed
results = orco.compute_many(
    [sampler(10, 12, sample=i) for i in range(30)])
```

Continue to [Extensions](extensions.md)