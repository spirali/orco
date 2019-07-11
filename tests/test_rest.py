from orco import Runtime
from multiprocessing import Process
from contextlib import contextmanager

import requests
import time


def test_rest_collections():
    rt = Runtime(":memory:")
    with rt.serve(testing=True).test_client() as client:
        r = client.get("collections")
        assert r.get_json() == []

        r = client.get("collections")
        assert r.get_json() == []

        c = rt.register_collection("hello")

        c.insert({"x": 1, "y": [1,2,3]}, "ABC")
        c.insert("e2", "A" * (1024 * 1024))

        rt.register_collection("hello2")

        r = client.get("collections")
        rr = r.get_json()
        assert len(rr) == 2

        assert rr[1] == {"name": "hello2", "count": 0, "size": 0}
        assert rr[0]["name"] == "hello"
        assert rr[0]["count"] == 2
        assert (1024 * 1024) < rr[0]["size"] < (1024 * 1024 + 2000)

        r = client.get("entries/hello")
        rr = r.get_json()
        rr.sort(key=lambda x: x["key"])
        for item in rr:
                assert item.get("key")
                del item["key"]
                assert item.get("size")
                del item["size"]
        print(r.get_json())
        assert rr == [{'config': 'e2'}, {'config': {'x': 1, 'y': [1, 2, 3]}}]
