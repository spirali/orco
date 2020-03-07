import threading
from time import sleep

import pytest

from orco import Builder, JobFailedException, JobSetup
from orco.internals.key import make_key


@pytest.mark.parametrize("n_processes", [1, 2])
def test_executor(env, n_processes):
    def to_dict(lst):
        return {x["id"]: x for x in lst}

    runtime = env.test_runtime()
    r = runtime._executor_summaries()
    assert len(r) == 0

    executor = env.executor(runtime, heartbeat_interval=1, n_processes=n_processes)
    executor.start()

    executor2 = env.executor(runtime, heartbeat_interval=1, n_processes=n_processes)
    executor2._debug_do_not_start_heartbeat = True
    executor2.start()

    executor3 = env.executor(runtime, heartbeat_interval=1, n_processes=n_processes)
    executor3.start()

    c = runtime.register_builder(Builder(None, "abc"))
    runtime.db.announce_entries(executor3.id, [c(a="x")], [])
    assert runtime.db.get_entry_state(c.name, make_key({'a': "x"})) == "announced"
    executor3.stop()
    assert runtime.db.get_entry_state(c.name, make_key({'a': "x"})) is None

    r = to_dict(runtime._executor_summaries())
    assert len(r) == 3
    assert r[executor.id]["status"] == "running"
    assert r[executor2.id]["status"] == "running"
    assert r[executor3.id]["status"] == "stopped"

    sleep(3)

    r = to_dict(runtime._executor_summaries())
    assert len(r) == 3
    assert r[executor.id]["status"] == "running"
    assert r[executor2.id]["status"] == "lost"
    assert r[executor3.id]["status"] == "stopped"


def test_executor_error(env):
    runtime = env.test_runtime()
    executor = env.executor(runtime, heartbeat_interval=1, n_processes=2)
    executor.start()

    col0 = runtime.register_builder(Builder(lambda c: c, "col0"))

    def b1(c):
        d = col0(c)
        yield
        return 100 // d.value

    col1 = runtime.register_builder(Builder(b1, "col1"))

    def b2(c):
        data = [col1(x) for x in c]
        yield
        return sum(d.value for d in data)

    col2 = runtime.register_builder(Builder(b2, "col2"))

    assert not runtime.get_reports()

    with pytest.raises(JobFailedException, match=".*ZeroDivisionError.*"):
        assert runtime.compute(col2([10, 0, 20]))

    reports = runtime.get_reports()
    assert len(reports) == 2
    assert reports[0].report_type == "error"
    assert reports[0].builder_name == "col1"
    assert reports[0].config == {'c': 0}
    assert "ZeroDivisionError" in reports[0].message

    assert runtime.get_entry_state(col0(0)) == "finished"

    assert runtime.compute(col2([10, 20])).value == 15
    assert runtime.compute(col2([1, 2, 4])).value == 175

    with pytest.raises(JobFailedException, match=".*ZeroDivisionError.*"):
        assert runtime.compute(col1(0))
    assert runtime.get_entry_state(col0(0)) == "finished"

    assert runtime.compute(col2([10, 20])).value == 15
    assert runtime.compute(col2([1, 2, 4])).value == 175

    r1 = [col2([100 + x, 101 + x, 102 + x]) for x in range(20)]
    r2 = [col2([200 + x, 201 + x, 202 + x]) for x in range(20)]
    result = runtime.compute_many(r1 + [col2([303, 0, 304])] + r2, continue_on_error=True)
    for i in range(20):
        print(">>>>>>>>>>>>>>>", i, result[i], r1[i])
        assert result[i] is not None
    for i in range(21, 41):
        assert result[i] is not None
    assert result[20] is None
    reports = runtime.get_reports()
    reports[0].report_type = "error"
    reports[0].config = [303, 0, 304]
    print(result)


def test_executor_timeout(env):
    runtime = env.test_runtime()
    executor = env.executor(runtime, heartbeat_interval=1, n_processes=2)
    executor.start()

    def compute(time, **kwargs):
        sleep(time)
        return time

    def job_setup(c):
        return JobSetup(timeout=c.get("timeout"))

    col0 = runtime.register_builder(Builder(compute, "col0", job_setup=job_setup))

    config0 = {"time": 1, "timeout": 0.2}
    with pytest.raises(JobFailedException, match=".*timeout.*"):
        assert runtime.compute(col0(**config0))

    reports = runtime.get_reports()
    assert len(reports) == 2
    assert reports[0].report_type == "timeout"
    print(reports[0])
    assert reports[0].builder_name == "col0"
    assert reports[0].config == config0
    assert "timeout" in reports[0].message

    assert runtime.compute(col0(time=1)).value == 1
    assert runtime.compute(col0(time=0.2, timeout=5)).value == 0.2


def test_executor_conflict(env, tmpdir):
    def compute_0(c):
        path = tmpdir.join("test-{}".format(c))
        assert not path.check()
        path.write("Done")
        sleep(1)
        return c

    def compute_1(c):
        col0 = Builder(None, "col0")
        d = [col0(c=x) for x in c]
        yield
        return sum([x.value for x in d])

    def init():
        runtime = env.test_runtime()
        col0 = runtime.register_builder(Builder(compute_0, "col0"))
        col1 = runtime.register_builder(Builder(compute_1, "col1"))
        return runtime, col0, col1

    runtime1, col0_0, col1_0 = init()
    runtime2, col0_1, col1_1 = init()

    results = [None, None]

    def comp1(runtime, col0, col1):
        results[0] = runtime1.compute(col1([0, 2, 3, 7, 10]))

    def comp2(runtime, col0, col1):
        results[1] = runtime2.compute(col1([2, 3, 7, 11]))

    t1 = threading.Thread(target=comp1, args=(runtime1, col0_0, col1_0))
    t1.start()
    sleep(0.5)
    assert runtime2.get_entry_state(Builder(None, "col0")(c=0)) == "announced"

    t2 = threading.Thread(target=comp2, args=(runtime2, col0_1, col1_1))
    t2.start()
    t1.join()
    t2.join()
    assert results[0].value == 22
    assert results[1].value == 23

    # assert tmpdir.join("test-10").mtime() > tmpdir.join("test-11").mtime()

    results = [None, None]

    def comp3(runtime, col0, col1):
        results[0] = runtime1.compute(col1([2, 7, 10, 30]))

    def comp4(runtime, col0, col1):
        results[1] = runtime2.compute(col0(30))

    t1 = threading.Thread(target=comp3, args=(runtime1, col0_0, col1_0))
    t1.start()
    t2 = threading.Thread(target=comp4, args=(runtime2, col0_1, col1_1))
    t2.start()
    t1.join()
    t2.join()
    assert results[0].value == 49
    assert results[1].value == 30

    t1 = threading.Thread(target=comp1, args=(runtime1, col0_0, col1_0))
    t1.start()
    t2 = threading.Thread(target=comp2, args=(runtime1, col1_0, col1_1))
    t2.start()
    t1.join()
    t2.join()
    assert results[0].value == 22
    assert results[1].value == 23
