from .entry import Entry
from .internals.context import _CONTEXT
from .internals.key import make_key


class Builder:
    """
    Builder - a factory for a task (a pair of builder and config)
    """

    def __init__(self, name: str):
        assert isinstance(name, str)
        self.name = name

    def __call__(self, config):
        entry = Entry(self.name, make_key(config), config, None, None, None)
        if not hasattr(_CONTEXT, "on_entry"):
            return entry
        on_entry = _CONTEXT.on_entry
        if on_entry:
            on_entry(entry)
        return entry
