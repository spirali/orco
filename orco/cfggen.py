import collections
import itertools
import json
from collections.abc import Iterable

_State = collections.namedtuple("State", ["toplevel", "computed", "resolving"])


def _check_type_all(iterable, type):
    for item in iterable:
        if not isinstance(item, type):
            return False
    return True


def _is_list_like(item):
    return isinstance(item, list) or isinstance(item, tuple)


def _resolve_ref(state, key):
    value = state.computed.get(key)
    if value is None:
        if key in state.resolving:
            raise Exception("Task cycle detected: {}".format(key))
        state.resolving.add(key)
        value = _resolve(state, state.toplevel[key])
        state.resolving.remove(key)
        state.computed[key] = value
    return value


def _resolve_range(state, args):
    if isinstance(args, int):
        return list(range(args))
    elif _is_list_like(args) and 2 <= len(args) <= 3:
        return list(range(*args))
    raise Exception("Invalid argument for range")


def _resolve_concat(state, args):
    assert isinstance(args, Iterable)
    items = [_resolve(state, item) for item in args]
    assert _check_type_all(items, Iterable)
    return list(itertools.chain.from_iterable(items))


def _resolve_product(state, args):
    if _is_list_like(args):
        args = [_resolve(state, item) for item in args]
        assert _check_type_all(args, list) or _check_type_all(args, tuple)
        return list(itertools.product(*args))
    elif isinstance(args, dict):
        values = [_resolve(state, item) for item in args.values()]
        assert _check_type_all(values, Iterable)
        return [dict(zip(args.keys(), items)) for items in itertools.product(*values)]
    else:
        raise Exception("Invalid argument of product")


def _resolve_zip(state, args):
    assert _is_list_like(args)
    assert _check_type_all(args, list) or _check_type_all(args, tuple)
    return list(zip(*[_resolve(state, item) for item in args]))


OPS_SWITCH = {
    "$ref": _resolve_ref,
    "$range": _resolve_range,
    "$+": _resolve_concat,
    "$product": _resolve_product,
    "$zip": _resolve_zip,
}


def _resolve(state, value):
    if isinstance(value, dict) and len(value) == 1:
        key = tuple(value.keys())[0]
        fn = OPS_SWITCH.get(key)
        if fn is not None:
            return fn(state, value[key])

    if _is_list_like(value):
        return [_resolve(state, item) for item in value]
    elif isinstance(value, dict):
        return {key: _resolve(state, item) for (key, item) in value.items()}
    return value


def build_config(data):
    state = _State(data, {}, set())
    return _resolve(state, data)


def build_config_from_file(path: str):
    with open(path) as f:
        data = json.load(f)
    return build_config(data)
