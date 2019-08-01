
import sys
import os
import pytest

TEST_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.dirname(TEST_DIR)
sys.path.insert(0, ROOT_DIR)

import orco  # noqa
import logging

logger = logging.getLogger("test")

class TestEnv:

    def __init__(self, tmpdir):
        self.runtimes = []
        self.tmpdir = tmpdir

    def test_runtime(self):
        db_path = str(self.tmpdir.join("db"))
        logger.info("DB path %s", db_path)
        r = orco.Runtime(db_path)
        self.runtimes.append(r)
        return r

    def stop(self):
        for r in self.runtimes:
            r.stop()
        self.runtimes = []


@pytest.fixture()
def env(tmpdir):
    test_env = TestEnv(tmpdir)
    yield test_env
    test_env.stop()