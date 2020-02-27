import orco


@orco.builder()
def add(config):
    return config["a"] + config["b"]


orco.run_cli()
