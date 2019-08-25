Here we show some best practices that you should take in mind when using ORCO.

## Versioning

Often you find yourself in a situation when you want to rerun already finished computations with
the exact same input configurations, because something has changed (for example a
binary program used to execute your computations or the build function itself). To force a recomputation,
you would have to manually delete the old results. To avoid losing the old results, we suggest to
introduce a `version` key in your configurations.

Let's assume that we have the following configurations: `{"param-a": 1, "param-b": 0.3}`. To make it
versioned, just add some key that will store the version like this: `{"param-a": 1, "param-b": 0.3,
version: 1}`.

Now when you want to run the computations again because of some external change, just bump the version
in your input configurations and the results will be computed from scratch. Later you can get the old results
from the database by using an older value of the `version` key.

In the case of collections with dependencies, you may need to propagate the version to all upstream
collections.

```python
# Original version
def make_deps(config):
    return col1.ref(config["dep_parameters"])

# Version propagating
def make_deps(config):
    return col1.ref({**config["dep_parameters"], "version": config["version"]})
```

If you already have some results computed without a `version` key, you can
[upgrade your collection](advanced.md#upgrading-collections) to add a `version` key with some default
value to the existing results.

## Computing multiple samples

If your computations are non-deterministic, you may want to execute them multiple times to gather multiple
samples so that you can (for example) average the results. An example of this
might be a benchmark that you want to run multiple times (each time with the same inputs).

You could simply run the computation multiple times in your build function and return a list of results,
but that would fix the number of samples that you have collected. If you wanted to add more later, you would
have to change either your input configurations or the build function, both of which would result in recomputing
all of the samples from scratch.

A better solution might be to produce just one sample from your build function, but introduce a key
in your input configurations that will distinguish each sample. For example you can add a key named
`sample` and compute N configurations differing only by the value of `sample` (which could
for example go from `0 ` to `N-1`). The value of the `sample` key itself doesn't have to be used
by the build function, it serves only to distinguish the input configurations from each other.

In the following example, we compute 20 samples with the same input parameters.

```python
# Build function for sampler
def run_sampler(config, inputs):
    # In the configration we ignore the 'sample' key
    return doSomethingNondeterministic(config)

samples = runtime.register_collection("samples", build_fn=run_sampler)

config = {...}
results = runtime.compute(samples.refs(
        [{**config, "sample": i}
         for i in range(20)])
```

Later, when we find out that we need more samples, we can easily increase the sample count.
Only the new samples will be computed (the first 20 ones are already stored in the database).

```python
# only the 10 new samples will be computed
results = runtime.compute(samples.refs(
        [{**config, "sample": i}
         for i in range(30)])
```