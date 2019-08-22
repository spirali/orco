from ..ref import Ref, RefKey
from typing import Iterable


def collect_refs(obj):
    result = set()
    _collect_refs_helper(obj, result, Ref)
    return result


def _collect_refs_helper(dep_value, ref_set, r_class):
    if isinstance(dep_value, r_class):
        ref_set.add(dep_value)
    elif isinstance(dep_value, dict):
        for val in dep_value.values():
            _collect_refs_helper(val, ref_set, r_class)
    elif isinstance(dep_value, Iterable):
        for val in dep_value:
            _collect_refs_helper(val, ref_set, r_class)


def collect_ref_keys(obj):
    result = set()
    _collect_refs_helper(obj, result, RefKey)
    return result


def _collect_ref_keys_helper(dep_value, ref_set):
    if isinstance(dep_value, Ref):
        ref_set.add(dep_value)
    elif isinstance(dep_value, dict):
        for val in dep_value.values():
            _collect_refs_helper(val, ref_set)
    elif isinstance(dep_value, Iterable):
        for val in dep_value:
            _collect_refs_helper(val, ref_set)


def resolve_ref_keys(dep_value, ref_map):
    return _walk_map(dep_value, RefKey, lambda r: ref_map[r])


def resolve_refs(dep_value, ref_map):
    return _walk_map(dep_value, Ref, lambda r: ref_map[r])


def ref_to_refkey(dep_value):
    return _walk_map(dep_value, Ref, lambda r: r.ref_key())


def _walk_map(value, target_type, final_fn):
    if value is None:
        return None
    elif isinstance(value, target_type):
        return final_fn(value)
    elif isinstance(value, dict):
        return {key: _walk_map(v, target_type, final_fn) for (key, v) in value.items()}
    elif isinstance(value, Iterable):
        return [_walk_map(v, target_type, final_fn) for v in value]
    else:
        return value
