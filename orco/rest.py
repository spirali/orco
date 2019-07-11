

from flask import Flask, request, current_app
from flask_restful import Resource, Api
from flask_cors import CORS
import json

app = Flask(__name__)
cors = CORS(app)
api = Api(app)


class Collections(Resource):

    def get(self):
        return current_app.runtime.collection_summaries()

api.add_resource(Collections, '/collections')


class Entries(Resource):

    def get(self, collection_name):
        return current_app.runtime.entry_summaries(collection_name)


api.add_resource(Entries, '/entries/<string:collection_name>')



def init_service(runtime):
    app.runtime = runtime
    return app