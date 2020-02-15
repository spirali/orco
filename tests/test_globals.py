from orco.globals import builder
import orco
import pytest


def test_global_builders(env):

    @builder()
    def compute1(config):
        return config + 1

    runtime = env.test_runtime(global_builders=True)
    entry = runtime.compute_entry(compute1(10))
    assert entry.value == 11