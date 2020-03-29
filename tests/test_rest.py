from orco import Runtime, Builder


def test_rest_builders(env):
    rt = env.test_runtime()
    c = rt.register_builder(Builder(None, "hello"))
    rt.register_builder(Builder(None, "hello2"))

    with rt.serve(testing=True).test_client() as client:
        r = client.get("rest/builders").get_json()
        assert len(r) == 2
        for v in r:
            assert v["name"] in ("hello", "hello2")
            assert v["n_finished"] == 0
            assert v["n_failed"] == 0
            assert v["n_in_progress"] == 0

        rt.insert(c(x=1, y=[1, 2, 3]), "ABC")
        rt.insert(c(e="e2"), "A" * (1024 * 1024))

        r = client.get("rest/builders").get_json()
        bmap = {v["name"]: v for v in r}
        assert "hello" in bmap and "hello2" in bmap

        rr = bmap["hello"]
        assert rr["n_finished"] == 2
        assert (1024 * 1024) < rr["size"] < (1024 * 1024 + 2000)

        r = client.get("rest/entries/hello")
        rr = r.get_json()
        rr.sort(key=lambda x: x["key"])
        job_ids = []
        for item in rr:
            assert item.get("key")
            del item["key"]
            assert item.get("size")
            del item["size"]
            job_ids.append(item.pop("id"))
        assert len(rr) == 2
        assert rr[0]["config"] == {'e': "e2"}
        assert rr[1]["config"] == {'x': 1, 'y': [1, 2, 3]}

        r = client.get("rest/blobs/" + str(job_ids[0])).get_json()
        assert len(r) == 1
        v = r[0]
        assert 50 < len(v["repr"]) <= 85
        assert v["size"] > 1000
        assert v["data_type"] == "pickle"

"""
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
"""