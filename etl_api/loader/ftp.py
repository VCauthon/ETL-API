
from etl_api.loader.base import AbstractLoader


class FTP(AbstractLoader):
    @staticmethod
    def load(data):
        raise NotImplementedError("Pending")  # TODO: Pending