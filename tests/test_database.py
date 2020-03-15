from orco import Builder
from orco.internals.key import make_key
from orco.internals.plan import PlanNode
from orco.internals.database import JobState
import pytest
import time


def make_jn(entry, inputs=None):
    if inputs is None:
        deps = []
        inputs = []
    job = Job(entry=entry, deps=deps, job_setup=None)
    return JobNode(job=job, inputs=inputs)


def test_xdb_announce(env):
    r = env.test_runtime()
    c = r.register_builder(Builder(None, "col1"))
    assert r.db.announce_jobs([make_jn(c(x="test1")), make_jn(c(x="test2"))])
    assert not r.db.announce_jobs([make_jn(c(x="test2")), make_jn(c(x="test3"))])
    assert not r.db.announce_jobs([make_jn(c(x="test2")), make_jn(c(x="test3"))])
    assert r.db.announce_jobs([make_jn(c(x="test3"))])
    assert r.db.get_entry_state(c.name, make_key({'x': "test1"})) == JobState.ANNOUNCED

    r.db.fix_crashed_jobs()

    assert r.db.get_entry_state(c.name, make_key({'x': "test1"})) == JobState.NONE
    assert r.db.announce_jobs([make_jn(c(x="test2"))])
    assert not r.db.announce_jobs([make_jn(c(x="test2")), make_jn(c(x="test3"))])
    assert not r.db.announce_jobs([make_jn(c(x="test2"))])
    assert r.db.announce_jobs([make_jn(c(x="test3"))])


def test_xdb_set_result(env):
    r = env.test_runtime()

    c = r.register_builder(Builder(None, "col1"))
    jn = make_jn(c(x=10))

    assert r.db.announce_jobs([jn])
    assert jn.job.job_id is not None

    assert r.db.get_entry_state(jn.job.entry.builder_name, jn.job.entry.key) == JobState.ANNOUNCED
    with pytest.raises(Exception, match="finished state"):
        r.db.set_finished(jn, 321)
    r.db.set_running(jn)
    assert r.db.get_entry_state(jn.job.entry.builder_name, jn.job.entry.key) == JobState.RUNNING
    with pytest.raises(Exception, match="running state"):
        r.db.set_running(jn)
    assert r.db.get_entry_state(jn.job.entry.builder_name, jn.job.entry.key) == JobState.RUNNING

    r.db.set_finished(jn, 123)
    assert r.db.get_entry_state(jn.job.entry.builder_name, jn.job.entry.key) == JobState.FINISHED