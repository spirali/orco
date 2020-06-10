import time

import pytest
import sqlalchemy as sa

from orco import Builder, builder, consts
from orco.internals.database import JobState
from orco.internals.key import make_key
from orco.internals.plan import PlanNode, Plan


def make_pn(job):
    return PlanNode(job.builder_name, job.key, job.config, None, [], [])


def announce(rt, jobs, return_plan=False):
    plan = Plan(jobs, False)
    plan._create_for_testing()
    r = rt.db.announce_jobs(plan)
    plan._testing_fill_job_ids(rt)
    if return_plan:
        return plan
    else:
        return r


def test_xdb_announce_basic(env):
    r = env.test_runtime()
    c = r.register_builder(Builder(lambda x: x, "col1"))

    assert announce(r, [c(x="test1"), c(x="test2")])
    assert not announce(r, [c(x="test2"), c(x="test3")])
    assert not announce(r, [c(x="test2"), c(x="test3")])
    not announce(r, [c(x="test3")])
    assert r.db.get_active_state(make_key(c.name, {"x": "test1"})) == JobState.ANNOUNCED

    r.db.drop_unfinished_jobs()

    assert r.db.get_active_state(make_key(c.name, {"x": "test1"})) == JobState.DETACHED
    assert announce(r, [c(x="test2")])
    assert not announce(r, [c(x="test2"), c(x="test3")])
    assert not announce(r, [c(x="test2")])
    assert announce(r, [c(x="test3")])


def test_xdb_set_result(env):
    r = env.test_runtime()

    c = r.register_builder(Builder(lambda x: x, "col1"))
    e = c(x=10)
    announce(r, [e])

    job_id = e._job_id
    assert r.db.get_active_state(e.key) == JobState.ANNOUNCED
    with pytest.raises(Exception, match="finished state"):
        r.db.set_finished(job_id, b"321", None, 1)
    r.db.set_running(job_id)
    assert r.db.get_active_state(e.key) == JobState.RUNNING
    with pytest.raises(Exception, match="running state"):
        r.db.set_running(job_id)
    assert r.db.get_active_state(e.key) == JobState.RUNNING

    r.db.set_finished(job_id, b"123", None, 1)
    assert r.db.get_active_state(e.key) == JobState.FINISHED


def test_xdb_running_window(env):
    @builder()
    def c(x):
        pass

    rt = env.test_runtime()

    e1 = c(x="test1")
    e2 = c(x="test2")
    assert announce(rt, [e1, e2])

    ids = set([e1._job_id, e2._job_id])
    assert ids == set(
        r[0] for r in rt.db.conn.execute(sa.select([rt.db._get_current_jobs()]))
    )

    rt.db.set_running(e1._job_id)
    assert ids == set(
        r[0] for r in rt.db.conn.execute(sa.select([rt.db._get_current_jobs()]))
    )
    rt.db.set_finished(e1._job_id, None, "", 1)
    assert ids == set(
        r[0] for r in rt.db.conn.execute(sa.select([rt.db._get_current_jobs()]))
    )
    rt.db.set_running(e2._job_id)
    assert ids == set(
        r[0] for r in rt.db.conn.execute(sa.select([rt.db._get_current_jobs()]))
    )
    rt.db.set_finished(e2._job_id, None, "", 1)
    assert set() == set(
        r[0] for r in rt.db.conn.execute(sa.select([rt.db._get_current_jobs()]))
    )

    time.sleep(1.1)

    e3 = c(x="test3")
    e4 = c(x="test4")
    assert announce(rt, [e3, e4])

    ids = set([e3._job_id, e4._job_id])

    rt.db.set_running(e3._job_id)
    assert ids == set(
        r[0] for r in rt.db.conn.execute(sa.select([rt.db._get_current_jobs()]))
    )
    rt.db.set_finished(e3._job_id, None, "", 1)
    assert ids == set(
        r[0] for r in rt.db.conn.execute(sa.select([rt.db._get_current_jobs()]))
    )
    rt.db.set_running(e4._job_id)
    assert ids == set(
        r[0] for r in rt.db.conn.execute(sa.select([rt.db._get_current_jobs()]))
    )
    rt.db.set_finished(e4._job_id, None, "", 1)
    assert set() == set(
        r[0] for r in rt.db.conn.execute(sa.select([rt.db._get_current_jobs()]))
    )


def test_xdb_unannounce_with_blobs(env):
    @builder()
    def c(x):
        pass

    rt = env.test_runtime()

    e1 = c(x="test1")
    e2 = c(x="test2")
    plan = announce(rt, [e1, e2], return_plan=True)

    job_id = e1._job_id
    rt.db.set_running(job_id)
    rt.db.insert_blob(job_id, "hello", b"1234", consts.MIME_BYTES, "xxx")

    rt.db.unannounce_jobs(plan)
