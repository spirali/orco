from .builder import Builder
from .runtime import _BuilderDef

_global_builders = {}


def builder(*, name=None):
    def _register(fn):
        if name is None:
            builder_name = fn.__name__
        else:
            builder_name = name
        _register_builder(_BuilderDef(builder_name, fn, None))
        return Builder(builder_name)

    return _register


def _register_builder(builder_):
    name = builder_.name
    if name in _global_builders:
        raise Exception("Builder '{}' is already globally registered.".format(name))
    _global_builders[name] = builder_


def clear_global_builders():
    _global_builders.clear()


def _get_global_builders():
    return _global_builders.values()
