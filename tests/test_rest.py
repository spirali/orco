from orco import Runtime, Builder


def test_rest_builders():
    rt = Runtime(":memory:")
    with rt.serve(testing=True).test_client() as client:
        r = client.get("rest/builders")
        assert r.get_json() == []

        r = client.get("rest/builders")
        assert r.get_json() == []

        c = rt.register_builder(Builder(None, "hello"))

        rt.insert(c({"x": 1, "y": [1, 2, 3]}), "ABC")
        rt.insert(c("e2"), "A" * (1024 * 1024))

        rt.register_builder(Builder(None, "hello2"))

        r = client.get("rest/builders")
        rr = r.get_json()
        assert len(rr) == 2

        assert rr[1] == {"name": "hello2", "count": 0, "size": 0}
        assert rr[0]["name"] == "hello"
        assert rr[0]["count"] == 2
        assert (1024 * 1024) < rr[0]["size"] < (1024 * 1024 + 2000)

        r = client.get("rest/entries/hello")
        rr = r.get_json()
        rr.sort(key=lambda x: x["key"])
        for item in rr:
            assert item.get("key")
            del item["key"]
            assert item.get("size")
            del item["size"]
        assert len(rr) == 2
        assert rr[0]["config"] == "e2"
        assert rr[1]["config"] == {'x': 1, 'y': [1, 2, 3]}


def test_rest_executors(env):
    rt = env.test_runtime()
    with rt.serve(testing=True).test_client() as client:
        r = client.get("rest/executors").get_json()
        assert len(r) == 0
        rt.start_executor()
        r = client.get("rest/executors").get_json()
        assert len(r) == 1
        assert r[0]["status"] == "running"


def test_rest_reports(env):
    rt = env.test_runtime()
    col1 = rt.register_builder(Builder(lambda c: c * 10, "col1"))
    rt.compute_many([col1(20), col1(30)])
    with rt.serve(testing=True).test_client() as client:
        r = client.get("rest/reports").get_json()
        assert len(r) == 1
        assert r[0]["type"] == "info"
        assert r[0]["builder"] is None
