import orco


@orco.builder()
def add(a, b):
    return a + b


orco.run_cli()
