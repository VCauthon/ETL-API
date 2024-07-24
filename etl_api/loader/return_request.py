
from flask import jsonify

from etl_api.loader.base import AbstractLoader, ModuleDetail


class ReturnRequest(AbstractLoader):
    @staticmethod
    def load(data):
        return jsonify(data)

    @classmethod
    def get_context_needed(cls) -> ModuleDetail:
        return ModuleDetail(
            name=ReturnRequest.__name__,
            description="Retrieve a response with the results"
        )