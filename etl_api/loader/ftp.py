from etl_api.loader.base import AbstractLoader
from etl_api.base import ModuleDetail


class FTP(AbstractLoader):
    @staticmethod
    def load(data):
        raise NotImplementedError("Pending")  # TODO: Pending

    @classmethod
    def get_context_needed(cls):
        return ModuleDetail(
            name=FTP.__name__, description="Load the results into an FTP"
        )
