from orco.globals import builder
import orco
import pytest


def test_global_builders(env):
    @builder()
    def compute1(config):
        return config + 1

    runtime = env.test_runtime(global_builders=True)
    job = runtime.compute(compute1(10))
    assert job.value == 11


def test_global_runtime(env):
    try:

        @builder()
        def b1(x):
            return x * 10

        orco.start_runtime("sqlite:///" + env.db_path())

        @builder()
        def b2(x):
            a = b1(x)
            yield
            return a.value + 1

        assert orco.compute(b1(10)).value == 100
        assert orco.compute(b2(10)).value == 101
        assert len(orco.compute_many([b1(10), b2(20)])) == 2

        assert orco.read(b2(10)).value == 101
        with pytest.raises(Exception, match="No finished job"):
            orco.read(b1(100))

        orco.drop(b2(10))
        orco.drop_unfinished_jobs()

        with pytest.raises(Exception, match="No finished job"):
            orco.read(b2(10))

        orco.drop_many([b1(10)])
    finally:
        orco.stop_global_runtime()