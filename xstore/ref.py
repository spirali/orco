

class Ref:

    __slots__ = ["collection", "config"]

    def __init__(self, collection, config):
        self.collection = collection
        self.config = config

    def ref_key(self):
        return (self.collection.name, self.collection.make_key(self.config))
