from dataclasses import dataclass
from enum import Enum
from typing import Type, List, Dict

from etl_api.loader.base import AbstractLoader

from etl_api.loader.return_request import ReturnRequest
from etl_api.loader.ftp import FTP
from etl_api.base import ModuleDetail


class LoaderType(Enum):
    RETURN_REQUEST = "RETURN_REQUEST"
    FTP = "FTP"


class _LoaderFactory:
    _loader_map: Dict[str, AbstractLoader] = {
        LoaderType.RETURN_REQUEST: ReturnRequest,
        LoaderType.FTP: FTP
    }

    @classmethod
    def create_loader(cls, loader: LoaderType) -> Type[AbstractLoader]:
        extractor_class = cls._loader_map.get(loader)
        if extractor_class is None:
            raise ValueError(f"No loader found for the given extraction type: {loader.value}")
        return extractor_class

    @classmethod
    def list_modules(cls) -> List[ModuleDetail]:
        return [module.get_context_needed() for module in cls._loader_map.values()]


class Loader:

    @dataclass
    class Arguments:
        type: LoaderType
        data = None

    @staticmethod
    def load(type: LoaderType, data):
        return _LoaderFactory.create_loader(type).load(data)

    @staticmethod
    def get_options() -> List[Dict[str, str]]:  # TODO: This is mandatory to a abstract class from base
        return _LoaderFactory.list_modules()  # TODO: This object must be transformer into a dict to be able to jsonify
