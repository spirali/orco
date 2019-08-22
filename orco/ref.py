from collections import Iterable, namedtuple


class Ref:
    """
    Reference to a collection.

    Public interface for creating references are methods "ref" and "refs" on CollectionRef

    >>> collection.ref(config)
    """

    __slots__ = ["collection_name", "config", "key"]

    def __init__(self, collection_name, config):
        self.collection_name = collection_name
        self.config = config
        self.key = make_key(config)

    def __eq__(self, other):
        if not isinstance(other, Ref):
            return False
        if self.key != other.key:
            return False
        return self.collection_name == other.collection_name

    def __hash__(self):
        return hash((self.collection_name, self.key))

    def __repr__(self):
        return "<{}/{}>".format(self.collection_name, repr(self.config))

    def ref_key(self):
        return RefKey(self.collection_name, self.key)


def _make_key_helper(obj, stream):
    if isinstance(obj, str) or isinstance(obj, int) or isinstance(obj, float):
        stream.append(repr(obj))
    elif isinstance(obj, list) or isinstance(obj, tuple):
        stream.append("[")
        for value in obj:
            _make_key_helper(value, stream)
            stream.append(",")
        stream.append("]")
    elif isinstance(obj, dict):
        stream.append("{")
        for key, value in sorted(obj.items()):
            if not isinstance(key, str):
                raise Exception("Invalid key in config: '{}', type: {}".format(
                    repr(key), type(key)))
            if key.startswith("_"):
                continue
            stream.append(repr(key))
            stream.append(":")
            _make_key_helper(value, stream)
            stream.append(",")
        stream.append("}")
    else:
        raise Exception("Invalid item in config: '{}', type: {}".format(repr(obj), type(obj)))


def make_key(config):
    stream = []
    _make_key_helper(config, stream)
    return "".join(stream)


RefKey = namedtuple("RefKey", ("collection_name", "key"))