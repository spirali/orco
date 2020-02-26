from .builder import Builder

_global_builders = {}


def builder(*, name=None, job_setup=None):
    def _register(fn):
        b = Builder(fn, name=name, job_setup=job_setup, update_wrapper=True)
        _register_builder(b)
        return b

    return _register


def _register_builder(b):
    if b.name in _global_builders:
        raise Exception("Builder {!r} is already globally registered.".format(b.name))
    _global_builders[b.name] = b


def clear_global_builders():
    _global_builders.clear()


def _get_global_builders():
    return _global_builders.values()
