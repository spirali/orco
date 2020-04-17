import orco


# Build function for our configurations
@orco.builder()
def add(a, b):
    return a + b


# Create a global runtime environment for ORCO.
# All data will be stored in file on provided path.
# If file does not exists, it is created
orco.start_runtime("sqlite:///my.db")

# Invoke computations, builder.ref(...) creates a "reference into a builder",
# basically a pair (builder, config)
# When reference is provided, compute returns instance of Entry that
# contains attribute 'value' with the result of build function.
job = orco.compute(add(1, 2))
print(job.value)  # prints: 3

# Invoke more compututations at once
result = orco.compute_many([add(1, 2),
                               add(2, 3),
                               add(4, 5)])
print([r.value for r in result])  # prints: [3, 5, 9]

orco.serve()
