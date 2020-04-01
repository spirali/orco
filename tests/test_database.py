from orco import Builder, builder, consts
from orco.internals.key import make_key
from orco.internals.plan import PlanNode, Plan
from orco.internals.database import JobState
import pytest
import sqlalchemy as sa
import time


def make_pn(entry):
    return PlanNode(entry.builder_name, entry.key, entry.config, None, [], [])


def announce(rt, entries):
    plan = Plan(entries, False)
    plan._create_for_testing()
    r = rt.db.announce_jobs(plan)
    plan.fill_job_ids(rt)
    return r

def test_xdb_announce_basic(env):
    r = env.test_runtime()
    c = r.register_builder(Builder(lambda x: x, "col1"))

    assert announce(r, [c(x="test1"), c(x="test2")])
    assert not announce(r, [c(x="test2"), c(x="test3")])
    assert not announce(r, [c(x="test2"), c(x="test3")])
    not announce(r, [c(x="test3")])
    assert r.db.get_entry_state(c.name, make_key({'x': "test1"})) == JobState.ANNOUNCED

    r.db.fix_crashed_jobs()

    assert r.db.get_entry_state(c.name, make_key({'x': "test1"})) == JobState.NONE
    assert announce(r, [c(x="test2")])
    assert not announce(r, [c(x="test2"), c(x="test3")])
    assert not announce(r, [c(x="test2")])
    assert announce(r, [c(x="test3")])


def test_xdb_set_result(env):
    r = env.test_runtime()

    c = r.register_builder(Builder(None, "col1"))
    jn = make_pn(c(x=10))

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


def test_xdb_running_window(env):

    @builder()
    def c(x):
        pass

    rt = env.test_runtime()

    e1 = c(x="test1")
    e2 = c(x="test2")
    plan = Plan([e1, e2], False)
    plan.compute(rt)
    assert rt.db.announce_jobs(plan)
    plan.fill_job_ids(rt)

    ids = set([e1._job_id, e2._job_id])
    assert ids == set(r[0] for r in rt.db.conn.execute(sa.select([rt.db._get_current_jobs()])))

    rt.db.set_running(e1._job_id)
    assert ids == set(r[0] for r in rt.db.conn.execute(sa.select([rt.db._get_current_jobs()])))
    rt.db.set_finished(e1._job_id, None, "", 1)
    assert ids == set(r[0] for r in rt.db.conn.execute(sa.select([rt.db._get_current_jobs()])))
    rt.db.set_running(e2._job_id)
    assert ids == set(r[0] for r in rt.db.conn.execute(sa.select([rt.db._get_current_jobs()])))
    rt.db.set_finished(e2._job_id, None, "", 1)
    assert set() == set(r[0] for r in rt.db.conn.execute(sa.select([rt.db._get_current_jobs()])))

    time.sleep(1.1)

    e3 = c(x="test3")
    e4 = c(x="test4")
    plan = Plan([e3, e4], False)
    plan.create(rt)
    assert rt.db.announce_jobs(plan)

    plan.fill_job_ids(rt)
    ids = set([e3._job_id, e4._job_id])

    rt.db.set_running(e3._job_id)
    assert ids == set(r[0] for r in rt.db.conn.execute(sa.select([rt.db._get_current_jobs()])))
    rt.db.set_finished(e3._job_id, None, "", 1)
    assert ids == set(r[0] for r in rt.db.conn.execute(sa.select([rt.db._get_current_jobs()])))
    rt.db.set_running(e4._job_id)
    assert ids == set(r[0] for r in rt.db.conn.execute(sa.select([rt.db._get_current_jobs()])))
    rt.db.set_finished(e4._job_id, None, "", 1)
    assert set() == set(r[0] for r in rt.db.conn.execute(sa.select([rt.db._get_current_jobs()])))


def test_xdb_unannounce_with_blobs(env):

    @builder()
    def c(x):
        pass

    rt = env.test_runtime()

    e1 = c(x="test1")
    e2 = c(x="test2")
    plan = Plan([e1, e2], False)
    plan.create(rt)
    assert rt.db.announce_jobs(plan)
    plan.fill_job_ids(rt)

    job_id = e1._job_id
    rt.db.set_running(job_id)
    rt.db.insert_blob(job_id, "hello", b"1234", consts.MIME_BYTES, "xxx")

    rt.db.unannounce_jobs(plan)