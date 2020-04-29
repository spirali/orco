![Screenshot of ORCO browser](./docs/userguide/imgs/orco.png)

# ORCO

ORCO (Organized Computing) is a Python package for defining, executing and persisting
computations in a straightforward fashion.

If you often find yourself in a situation where you need to:
 - run many computations with various combinations of input parameters
 - easily distribute the computation among many cores
 - persist the results to disk in a structured way
 - define dependencies between computations and pipeline them together
 - avoid recomputing already executed computations when you add new inputs
 - observe the status of the computations while they are running

then ORCO is the right tool for you.

ORCO prevents problems like accidentally overwriting data, computing already
computed data, non-invalidating old computations when the input data are
changed, overwriting data in simultaneous computations.

ORCO combines an execution engine for running computations with a database that
stores their results and allows you to query them. It forces you to be very
explicit in specifying how is your computation defined and what are its
dependencies. This makes the code a bit verbose, but facilitates reproducibility
of your experiments.

* [User guide](https://spirali.github.io/orco.pages/userguide/userguide)
* [API reference](https://spirali.github.io/orco.pages/api/)

## Example

### Defining computation

```python
import orco

@orco.builder()
def train(n_layers, layer_size):
    import tensorflow as tf

    model = ...
    # skipped TF code
    # creating a model with n_layers with layer_size

    # TF method, prints on stdout
    # ORCO captures stdout and save it with result
    model.summary()

    test_accuracy = ...
    train_accuracy = ...
    # save model into directory 'model', function runs in own directory
    model.save("model")

    orco.attach_directory("model")
    orco.attach_object("train_acc", train_accuracy)
    orco.attach_object("test_acc", test_accuracy)


@orco.builder()
def summary(configurations):
    import seaborn as sns
    from orco.ext.pyplot import attach_figure

    # Defining dependencies by calling builders
    results = [train(**cfg) for cfg in configurations]

    yield  # All dependencies are computed after this point
           # We can now read the results

    # Create pyplot chart
    values = [r.get_object("test_acc") for r in results]
    names = [f"#l={c['n_layers']} s={r.c['layer_size']}"
             for c in configurations]
    sns.barplot(x=names, y=values)

    attach_figure("chart", tight_layout=True)
```


### Running computation

```python
# DB as file
orco.start_runtime("sqlite:///my.db")
# OR
# orco.start_runtime("postgresql://USER:PASS@HOST/DB")

# Start computation and store result in database
job = orco.compute(train(3, 32))


# Get results
accuracy = job.get_object("train_accuracy")
job.extract_tar("model", "./target_path")  # model is a directory

# Compute summary, this needs 3 'train' computations
# train(3, 32) is skipped as it is already computed
orco.compute(summary([
    {"n_layers": 3, "layer_size": 32},
    {"n_layers": 4, "layer_size": 64},
    {"n_layers": 5, "layer_size": 64},
]))

```


### Browser

<img src='docs/imgs/browser2.png'/>

## License

[MIT](LICENSE)

