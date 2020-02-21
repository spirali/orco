import orco


# Build function for our configurations
@orco.builder()
def add(config):
    return config["a"] + config["b"]


# Create a runtime environment for ORCO.
# All data will be stored in file on provided path.
# If file does not exists, it is created
runtime = orco.Runtime("./mydb")


# Invoke computations, builder.ref(...) creates a "reference into a builder",
# basically a pair (builder, config)
# When reference is provided, compute returns instance of Entry that
# contains attribute 'value' with the result of build function.
result = runtime.compute(add({"a": 1, "b": 2}))
print(result.value)  # prints: 3

# Invoke more compututations at once
result = runtime.compute_many([add({"a": 1, "b": 2}),
                               add({"a": 2, "b": 3}),
                               add({"a": 4, "b": 5})])
print([r.value for r in result])  # prints: [3, 5, 9]

runtime.serve()
