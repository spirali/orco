from .ref import Ref


class CollectionRef:
    """
    Reference to a collection
    """

    def __init__(self, name):
        self.name = name

    def ref(self, config):
        """Create an entry refence"""
        return Ref(self.name, config)

    def refs(self, configs):
        """Create more entry references at once"""
        name = self.name
        return [Ref(name, config) for config in configs]
