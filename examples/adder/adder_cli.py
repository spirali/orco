from orco import Runtime, run_cli

runtime = Runtime("./mydb")


def build_fn(config, inputs):
    return config["a"] + config["b"]


add = runtime.register_collection("add", build_fn=build_fn)

run_cli(runtime)
