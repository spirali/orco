import os
import threading

from flask import Flask, Response
from flask_cors import CORS
from flask_restful import Resource, Api

from .database import Database

STATIC_ROOT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static"
)
app = Flask(__name__, static_url_path=STATIC_ROOT)
cors = CORS(app)
api = Api(app, prefix="/rest")

thread_local_db = threading.local()


def get_db():
    # TODO: Do something better then creating a DB each time
    db = Database(app.db_url)
    return db


class Builders(Resource):
    def get(self):
        return get_db().builder_summaries(app.builders)


api.add_resource(Builders, "/builders")


class Jobs(Resource):
    def get(self, builder_name):
        return get_db().job_summaries(builder_name)


api.add_resource(Jobs, "/jobs/<string:builder_name>")


class Blobs(Resource):
    def get(self, job_id):
        return get_db().blob_summaries(job_id)


api.add_resource(Blobs, "/blobs/<int:job_id>")


class Status(Resource):
    def get(self):
        return get_db().get_running_status()


api.add_resource(Status, "/status/")


def from_gzipped_file(filename):
    assert not os.path.isabs(filename)
    filename = os.path.join(STATIC_ROOT, filename)
    with open(filename, "rb") as f:
        data = f.read()
    headers = {"Content-Encoding": "gzip", "Content-Length": len(data)}
    if filename.endswith("css.gz"):
        headers["Content-Type"] = "text/css"
    return Response(data, headers=headers)


#    filename = os.path.join(STATIC_ROOT, "main.js.gz")
#    return from_gzipped_file(filename)


@app.route("/static/<path:path>")
def static_serve(path):
    return from_gzipped_file(path + ".gz")


@app.route("/manifest.json")
def static_manifest():
    return from_gzipped_file("manifest.json.gz")


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def static_index(path):
    return from_gzipped_file("index.html.gz")


def init_service(runtime):
    app.builders = list(runtime._builders.values())
    app.db_url = runtime.db.url
    return app
