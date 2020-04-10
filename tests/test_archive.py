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
    assert jobs[0].state == JobState.A_FINISHED

    assert runtime.get_state(cc(1)) == JobState.NONE
    jobs = runtime.read_jobs(cc(1))
    assert len(jobs) == 1
    assert jobs[0].state == JobState.A_FINISHED

    assert runtime.get_state(aa(1)) == JobState.FINISHED
    assert runtime.get_state(cc(2)) == JobState.FINISHED

    runtime.compute_many([cc(1)])
    assert runtime.get_state(bb(1)) == JobState.FINISHED
    assert runtime.get_state(cc(1)) == JobState.FINISHED

    runtime.archive(bb(1))

    jobs = runtime.read_jobs(bb(1))
    assert len(jobs) == 2
    assert all(j.state == JobState.A_FINISHED for j in jobs)

    assert runtime.get_state(cc(1)) == JobState.NONE
    jobs = runtime.read_jobs(cc(1))
    assert len(jobs) == 2
    assert all(j.state == JobState.A_FINISHED for j in jobs)

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

    runtime.compute(cc(3))
    runtime.free(bb(3))
    runtime.archive(aa(3))
    jobs = runtime.read_jobs(bb(3))
    assert len(jobs) == 2
    assert {JobState.A_FINISHED, JobState.A_FREED} == set(j.state for j in jobs)


def test_free(env):

    @builder()
    def aa(x):
        attach_object("Hello", 1234)
        return random.random()

    @builder()
    def bb(x):
        a = aa(0)
        yield
        return a.value

    runtime = env.test_runtime()

    runtime.compute(bb(1))
    assert runtime.get_state(aa(0)) == JobState.FINISHED
    assert runtime.get_state(bb(1)) == JobState.FINISHED

    runtime.free(aa(0))
    assert runtime.get_state(aa(0)) == JobState.FREED
    assert runtime.get_state(bb(1)) == JobState.FINISHED

    with pytest.raises(Exception, match="freed state"):
        runtime.compute(bb(2))

    with pytest.raises(Exception, match="freed state"):
        runtime.compute(aa(0))

    jobs = runtime.read_jobs(aa(0))
    assert len(jobs) == 1
    assert jobs[0].state == JobState.FREED
    with pytest.raises(Exception, match="not found"):
        jobs[0].get_object("Hello")
    assert jobs[0].get_names() == []

    runtime.drop(aa(0))
    runtime.compute(bb(2))