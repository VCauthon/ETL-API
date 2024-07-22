from enum import Enum
from typing import Type

from etl_api.loader.base import AbstractLoader

from etl_api.loader.return_request import ReturnRequest
from etl_api.loader.ftp import FTP


class LoaderType(Enum):
    RETURN_REQUEST = "RETURN_REQUEST"
    FTP = "FTP"


class _LoaderFactory:
    _loader_map = {
        LoaderType.RETURN_REQUEST: ReturnRequest,
        LoaderType.FTP: FTP
    }

    @classmethod
    def create_loader(cls, loader: LoaderType) -> Type[AbstractLoader]:
        extractor_class = cls._loader_map.get(loader)
        if extractor_class is None:
            raise ValueError(f"No loader found for the given extraction type: {loader.value}")
        return extractor_class


class Loader:
    @staticmethod
    def load(type: LoaderType, data):
        return _LoaderFactory.create_loader(type).load(data)
