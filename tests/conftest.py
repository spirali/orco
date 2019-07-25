
import sys
import os
import pytest

TEST_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.dirname(TEST_DIR)
sys.path.insert(0, ROOT_DIR)

import orco  # noqa


class TestEnv:

    def __init__(self):
        self.runtimes = []

    def runtime_in_memory(self):
        r = orco.Runtime(":memory:")
        self.runtimes.append(r)
        return r

    def stop(self):
        for r in self.runtimes:
            r.stop()
        self.runtimes = []


@pytest.fixture()
def env():
    test_env = TestEnv()
    yield test_env
    test_env.stop()