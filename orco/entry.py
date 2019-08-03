

class Entry:

    __slots__ = ("config", "value", "created", "comp_time")

    def __init__(self, config, value, created=None, comp_time=None):
        self.config = config
        self.value = value
        self.created = created
        self.comp_time = comp_time

    @property
    def value_repr(self):
        value_repr = repr(self.value)
        if len(value_repr) > 85:
            value_repr = value_repr[:85] + " ..."
        return value_repr

    @property
    def is_computed(self):
        return bool(self.created)

    def __repr__(self):
        return "<Entry {}: {}>".format(self.config, self.value_repr)
