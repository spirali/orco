from orco import builder, attach_object, attach_file, attach_directory, attach_text, JobState
import pytest
import random

"""
def test_archive(env):

    @builder()
    def aa(x):
        return random.random()

    @builder()
    def bb(x):
        a = aa(x)
        yield
        return a.value

    @builder()
    def cc(x):
        b = bb(x)
        yield
        return b.value * 10

    runtime = env.test_runtime()

    c1 = cc(1)
    c2 = cc(2)
    runtime.compute_many([c1, c2])

    assert runtime.get_state(bb(1)) == JobState.FINISHED
    runtime.archive(cc(1))
    assert runtime.get_state(bb(1)) == JobState.FINISHED
"""