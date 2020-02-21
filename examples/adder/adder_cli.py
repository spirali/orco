import orco


@orco.builder()
def add(config):
    return config["a"] + config["b"]


runtime = orco.Runtime("./mydb")
orco.run_cli(runtime)
