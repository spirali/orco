class Entry:
    """
    A single computation with its configuration and result

    Attributes
    * config - configuration
    * valu - resulting value of the computation
    * created - datetime when entry was created
    * comp_time - time of computation when entry was created, or None if entry was inserted
    """

    __slots__ = ("config", "value", "created", "comp_time")

    def __init__(self, config, value, created=None, comp_time=None):
        self.config = config
        self.value = value
        self.created = created
        self.comp_time = comp_time

    @property
    def is_computed(self):
        return bool(self.created)

    def __repr__(self):
        return "<Entry {}>".format(self.config)
