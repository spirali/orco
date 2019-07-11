

class Entry:

    __slots__ = ("collection", "config", "value", "created")

    def __init__(self, collection, config, value, created):
        self.collection = collection
        self.config = config
        self.value = value
        self.created = created