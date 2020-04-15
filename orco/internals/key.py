from hashlib import sha224


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
                raise Exception(
                    "Invalid key in config: '{}', type: {}".format(repr(key), type(key))
                )
            if key.startswith("__"):
                continue
            stream.append(repr(key))
            stream.append(":")
            _make_key_helper(value, stream)
            stream.append(",")
        stream.append("}")
    else:
        raise Exception(
            "Invalid item in config: '{}', type: {}".format(repr(obj), type(obj))
        )


def make_key(builder_name, config):
    stream = [builder_name, "!"]
    _make_key_helper(config, stream)
    return sha224("".join(stream).encode()).hexdigest()
