from orco import Runtime

# Create a runtime environment for ORCO.
# All data will be stored in file on provided path.
# If file does not exists, it is created
runtime = Runtime("./mydb")


# Build function for our configurations
def build_fn(config, inputs):
    return config["a"] + config["b"]


# Create a builder and register build_fn
add = runtime.register_builder("add", build_fn=build_fn)


# Invoke computations, builder.ref(...) creates a "reference into a builder",
# basically a pair (builder, config)
# When reference is provided, compute returns instance of Entry that
# contains attribute 'value' with the result of build function.
result = runtime.compute(add.task({"a": 1, "b": 2}))
print(result.value)  # prints: 3

# Invoke more compututations at once
result = runtime.compute([add.task({"a": 1, "b": 2}),
                          add.task({"a": 2, "b": 3}),
                          add.task({"a": 4, "b": 5})])
print([r.value for r in result])  # prints: [3, 5, 9]

runtime.serve()
