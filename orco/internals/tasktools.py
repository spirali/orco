from ..task import Task, TaskKey
from typing import Iterable


def collect_tasks(obj):
    result = set()
    _collect_tasks_helper(obj, result, Task)
    return result


def _collect_tasks_helper(dep_value, task_set, r_class):
    if isinstance(dep_value, r_class):
        task_set.add(dep_value)
    elif isinstance(dep_value, dict):
        for val in dep_value.values():
            _collect_tasks_helper(val, task_set, r_class)
    elif isinstance(dep_value, Iterable):
        for val in dep_value:
            _collect_tasks_helper(val, task_set, r_class)


def collect_task_keys(obj):
    result = set()
    _collect_tasks_helper(obj, result, TaskKey)
    return result


def _collect_task_keys_helper(dep_value, task_set):
    if isinstance(dep_value, Task):
        task_set.add(dep_value)
    elif isinstance(dep_value, dict):
        for val in dep_value.values():
            _collect_tasks_helper(val, task_set)
    elif isinstance(dep_value, Iterable):
        for val in dep_value:
            _collect_tasks_helper(val, task_set)


def resolve_task_keys(dep_value, task_map):
    return _walk_map(dep_value, TaskKey, lambda r: task_map[r])


def resolve_tasks(dep_value, task_map):
    return _walk_map(dep_value, Task, lambda r: task_map[r])


def task_to_taskkey(dep_value):
    return _walk_map(dep_value, Task, lambda r: r.task_key())


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
