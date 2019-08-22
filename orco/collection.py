from .ref import Ref


class CollectionRef:

    def __init__(self, name):
        self.name = name

    def ref(self, config):
        return Ref(self.name, config)

    def refs(self, configs):
        name = self.name
        return [Ref(name, config) for config in configs]
