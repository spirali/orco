import threading
import time

from orco import Runtime, Builder, builder, JobFailedException, consts


def test_rest_builders(env):
    rt = env.test_runtime()
    c = rt.register_builder(Builder(None, "hello", is_frozen=True))
    rt.register_builder(Builder(None, "hello2", is_frozen=True))

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

        r = client.get("rest/jobs/hello")
        rr = r.get_json()
        rr.sort(key=lambda x: x["id"])
        job_ids = []
        for item in rr:
            assert item.get("key")
            del item["key"]
            assert item.get("size")
            del item["size"]
            job_ids.append(item.pop("id"))
        assert len(rr) == 2
        assert rr[1]["config"] == {"e": "e2"}
        assert rr[0]["config"] == {"x": 1, "y": [1, 2, 3]}

        r = client.get("rest/blobs/" + str(job_ids[1])).get_json()
        assert len(r) == 1
        v = r[0]
        assert 50 < len(v["repr"]) <= 85
        assert v["size"] > 1000
        assert v["mime"] == consts.MIME_PICKLE


def test_rest_status(env):
    def compute1(url):
        @builder()
        def ff(x=0):
            time.sleep(0.5)

        new_rt = Runtime(url)
        try:
            new_rt.compute(ff(0))
        finally:
            new_rt.stop()

    class MyException(Exception):
        pass

    def compute2(url):
        @builder()
        def aa(x=1):
            return x

        @builder()
        def bb(x=1):
            aa(x=1)
            aa(x=2)
            yield
            time.sleep(0.5)

        @builder()
        def cc():
            d = bb(x=1)
            yield
            raise MyException("MyError")

        new_rt = Runtime(url)
        try:
            new_rt.compute(cc())
        except JobFailedException:
            pass
        finally:
            new_rt.stop()

    rt = env.test_runtime()
    with rt.serve(testing=True).test_client() as client:
        r = client.get("rest/status/").get_json()
        assert r["counts"] == {
            "n_finished": 0,
            "n_running": 0,
            "n_announced": 0,
            "n_failed": 0,
        }

        thread = threading.Thread(target=compute1, args=(rt.db.url,))
        thread.start()
        time.sleep(0.21)
        r = client.get("rest/status/").get_json()
        thread.join()
        assert r["counts"] == {
            "n_finished": 0,
            "n_running": 1,
            "n_announced": 0,
            "n_failed": 0,
        }
        time.sleep(1.0)

        thread = threading.Thread(target=compute2, args=(rt.db.url,))
        thread.start()
        time.sleep(0.25)
        r = client.get("rest/status/").get_json()
        thread.join()
        assert r["counts"] == {
            "n_finished": 2,
            "n_running": 1,
            "n_announced": 1,
            "n_failed": 0,
        }
        assert len(r["errors"]) == 0

        r = client.get("rest/status/").get_json()
        assert r["counts"] == {
            "n_finished": 0,
            "n_running": 0,
            "n_announced": 0,
            "n_failed": 0,
        }
        assert len(r["errors"]) == 1


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
