from typing import Dict, Any

import re

key_pattern = re.compile("^[A-Za-z][A-Za-z0-9_]*$")


class Obj:

    """
    Immutable wrapper dictionary.

    1. All keys has to match ^[A-Za-z][A-Za-z0-9_]*$
    2. All values must be Obj, int, float, str, list/tuple of here mentioned values.
       If dict is in value, it is converted into Obj

    >>> x = Obj(a=10, b=20)


    """

    __frozen = False

    def __init__(self, data: Dict[str, Any] = None, **kw):
        if kw:
            if data is not None:
                raise Exception("Either 'data' or keyword arguments can used. Not both")
            data = kw
        else:
            assert isinstance(data, dict)
            data = data.copy()
        self.__dict__ = transform_into_obj(data)
        self.__frozen = True

    def __setattr__(self, name, value):
        if self.__frozen:
            raise Exception("Obj is immutable")
        else:
            object.__setattr__(self, name, value)

    def __delattr__(self, name):
        raise Exception("Obj is immputable")

    def __repr__(self):
        return "<{}>".format(" ".join("{}={}".format(k, v) for k, v in self._data.items()))


def transform_into_obj(dictionary):
    for key in dictionary:
        if not isinstance(key, str) or not key_pattern.match(key):
            raise Exception("Invalid key '{}'".format(key))
        value = dictionary[key]
        if isinstance(value, dict):
            dictionary[key] = Obj(value)
        # TODO: list/tuple
        #elif isinstance(key, list) or isinstance(key, tuple):
        elif not (isinstance(value, int) or isinstance(value, str) or isinstance(value, float)):
            raise Exception("Invalid value for key '{}': {}".format(key, value))
    return dictionary