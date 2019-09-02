from flask import Flask, request, current_app, Response
from flask_restful import Resource, Api
from flask_cors import CORS
import json
import os

STATIC_ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static")
app = Flask(__name__, static_url_path=STATIC_ROOT)
cors = CORS(app)
api = Api(app, prefix="/rest")


class Builders(Resource):

    def get(self):
        return current_app.runtime._builder_summaries()


api.add_resource(Builders, '/builders')


class Entries(Resource):

    def get(self, builder_name):
        return current_app.runtime._entry_summaries(builder_name)


api.add_resource(Entries, '/entries/<string:builder_name>')


class Executors(Resource):

    def get(self):
        return current_app.runtime._executor_summaries()


api.add_resource(Executors, '/executors')


class Reports(Resource):

    def get(self):
        reports = current_app.runtime.get_reports()
        return [report.to_dict() for report in reports]


api.add_resource(Reports, '/reports')


def from_gzipped_file(filename):
    assert not os.path.isabs(filename)
    filename = os.path.join(STATIC_ROOT, filename)
    with open(filename, "rb") as f:
        data = f.read()
    headers = {'Content-Encoding': 'gzip', 'Content-Length': len(data)}
    if filename.endswith("css.gz"):
        headers["Content-Type"] = "text/css"
    return Response(data, headers=headers)


#    filename = os.path.join(STATIC_ROOT, "main.js.gz")
#    return from_gzipped_file(filename)


@app.route('/static/<path:path>')
def static_serve(path):
    return from_gzipped_file(path + ".gz")


@app.route('/manifest.json')
def static_manifest():
    return from_gzipped_file("manifest.json.gz")


@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def static_index(path):
    return from_gzipped_file("index.html.gz")


def init_service(runtime):
    app.runtime = runtime
    return app
