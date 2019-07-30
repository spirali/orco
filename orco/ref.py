

class Ref:

    __slots__ = ["collection", "config"]

    def __init__(self, collection, config):
        self.collection = collection
        self.config = config

    def ref_key(self):
        return (self.collection.name, self.collection.make_key(self.config))

    def __repr__(self):
        return "<{}/{}>".format(self.collection.name, repr(self.config))
    """
    def __eq__(self, other):
        if not isinstance(other, Ref) or self.collection != other.collection:
            return False
        collection = self.collection
        return collection.make_key(self.config) == collection.make_key(other.config)

    def __hash__(self):
        return hash(self.collection) ^ hash(self.collection.make_key(self.config))
    """