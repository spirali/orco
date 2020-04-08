from orco import builder, attach_object, attach_file, attach_directory, attach_text, JobState
import pytest
import random


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
    runtime.archive(bb(1))

    assert runtime.get_state(bb(1)) == JobState.NONE
    jobs = runtime.read_jobs(bb(1))
    assert len(jobs) == 1
    assert jobs[0].state == JobState.ARCHIVED

    assert runtime.get_state(cc(1)) == JobState.NONE
    jobs = runtime.read_jobs(cc(1))
    assert len(jobs) == 1
    assert jobs[0].state == JobState.ARCHIVED

    assert runtime.get_state(aa(1)) == JobState.FINISHED
    assert runtime.get_state(cc(2)) == JobState.FINISHED

    runtime.compute_many([cc(1)])
    assert runtime.get_state(bb(1)) == JobState.FINISHED
    assert runtime.get_state(cc(1)) == JobState.FINISHED

    runtime.archive(bb(1))

    jobs = runtime.read_jobs(bb(1))
    assert len(jobs) == 2
    assert all(j.state == JobState.ARCHIVED for j in jobs)

    assert runtime.get_state(cc(1)) == JobState.NONE
    jobs = runtime.read_jobs(cc(1))
    assert len(jobs) == 2
    assert all(j.state == JobState.ARCHIVED for j in jobs)

    assert runtime.get_state(cc(2)) == JobState.FINISHED
    runtime.archive(aa(2))
    assert runtime.get_state(cc(2)) == JobState.NONE

    runtime.compute(cc(3))
    assert runtime.get_state(aa(3)) == JobState.FINISHED
    runtime.archive(cc(3))
    assert runtime.get_state(aa(3)) == JobState.FINISHED

    runtime.compute(cc(3))
    assert runtime.get_state(aa(3)) == JobState.FINISHED
    runtime.archive(cc(3), archive_inputs=True)
    assert runtime.get_state(aa(3)) == JobState.NONE