from test_database import announce
import time
import pytest

from orco import Builder


def test_wait_for_others(env):
    r = env.test_runtime()
    c = r.register_builder(Builder(lambda x: x, "col1"))
    assert announce(r, [c(x="test1"), c(x="test2")])

    start = time.time()
    with pytest.raises(Exception, match="claimed by another"):
        r.compute(c(x="test1"))
    end = time.time()
    assert end - start < 0.5

    start = time.time()
    with pytest.raises(Exception, match="claimed by another"):
       r.compute(c(x="test1"), wait_for_others=4)
    end = time.time()
    assert 3.9 < end - start < 6

    r.drop_unfinished_jobs()
    r.compute(c(x="test1"))