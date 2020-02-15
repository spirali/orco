import time
import pickle
import pytest

from orco.internals.key import make_key


def test_db_announce(env):
    r = env.test_runtime()
    e1 = env.executor(r, heartbeat_interval=1)
    e1._debug_do_not_start_heartbeat = True
    e1.start()

    e2 = env.executor(r, heartbeat_interval=1)
    e2.runtime = r
    e2.start()

    c = r.register_builder("col1")
    assert r.db.announce_entries(e1.id, [c("test1"), c("test2")], [])
    assert not r.db.announce_entries(e2.id, [c("test2"), c("test3")], [])
    assert not r.db.announce_entries(e1.id, [c("test2"), c("test3")], [])
    assert r.db.announce_entries(e2.id, [c("test3")], [])
    assert r.db.get_entry_state(c.name, make_key("test1")) == "announced"
    time.sleep(3)
    assert r.db.get_entry_state(c.name, make_key("test1")) is None
    assert not r.db.announce_entries(e2.id, [c("test2"), c("test3")], [])
    assert r.db.announce_entries(e2.id, [c("test2")], [])
    assert not r.db.announce_entries(e2.id, [c("test2")], [])


def make_raw_entry(runtime, c, cfg, value, comp_time=1):
    builder = runtime._builders[c.name]
    return builder.make_raw_entry(c.name, make_key(cfg), cfg, pickle.dumps(value), None, comp_time)


def test_db_set_value(env):
    r = env.test_runtime()
    r.configure_executor(heartbeat_interval=1)
    e1 = r.start_executor()

    c = r.register_builder("col1")
    assert r.db.get_entry_state(c.name, make_key("cfg1")) is None

    r.db.announce_entries(e1.id, [c("cfg1")], [])
    assert r.db.get_entry_state(c.name, make_key("cfg1")) == "announced"

    assert r.try_read_entry(c("cfg1")) is None
    assert r.try_read_entry(c("cfg1"), include_announced=True) is not None

    e = make_raw_entry(r, c, "cfg1", "value1")
    r.db.set_entry_values(e1.id, [e])
    assert r.db.get_entry_state(c.name, make_key("cfg1")) == "finished"

    assert r.try_read_entry(c("cfg1")) is not None
    assert r.try_read_entry(c("cfg1"), include_announced=True) is not None

    with pytest.raises(Exception):
        r.db.set_entry_values(e1.id, [e])

    e2 = make_raw_entry(r, c, "cfg2", "value2")
    with pytest.raises(Exception):
        r.db.set_entry_values(e1.id, [e2])
    r.db.announce_entries(e1.id, [c("cfg2")], [])
    r.db.set_entry_values(e1.id, [e2])

    with pytest.raises(Exception):
        r.db.create_entries([e2])

    e3 = make_raw_entry(r, c, "cfg3", "value3")
    r.db.create_entries([e3])


def test_db_run_stats(env):
    runtime = env.test_runtime()
    runtime.configure_executor(heartbeat_interval=1)
    e1 = runtime.start_executor()

    c = runtime.register_builder("col1")
    _ = runtime.register_builder("col2")

    assert runtime.db.announce_entries(
        e1.id, [c("a"), c("b"), c("c"),
                c("d"), c("e")], [])
    assert runtime.db.get_entry_state(c.name, make_key("a")) == "announced"
    runtime.db._dump()
    entry = make_raw_entry(runtime, c, "a", "value", comp_time=1)
    runtime.db.set_entry_values(e1.id, [entry])
    entry = make_raw_entry(runtime, c, "b", "value", comp_time=2)
    runtime.db.set_entry_values(e1.id, [entry])
    entry = make_raw_entry(runtime, c, "c", "value", comp_time=3)
    runtime.db.set_entry_values(e1.id, [entry])
    entry = make_raw_entry(runtime, c, "d", "value", comp_time=4)
    runtime.db.set_entry_values(e1.id, [entry])

    r = runtime.db.get_run_stats("col1")
    assert pytest.approx(2.5) == r["avg"]
    assert pytest.approx(1.29099, 0.00001) == r["stdev"]
    assert r["count"] == 4

    r = runtime.db.get_run_stats("col2")
    assert None is r["avg"]
    assert None is r["stdev"]
    assert r["count"] == 0
