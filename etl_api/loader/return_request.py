
from etl_api.loader.base import AbstractLoader

from flask import jsonify


class ReturnRequest(AbstractLoader):
    @staticmethod
    def load(data):
        return jsonify(data)
