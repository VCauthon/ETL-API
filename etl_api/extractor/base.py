from abc import ABC, abstractmethod

from pandas import DataFrame

from etl_api.base import ModuleDetail


class AbstractExtractor(ABC):
    def __init__(self, **kwargs) -> None:
        self._data_raw = None
        self._data_type = None

    @abstractmethod
    def _retrieve_data(self) -> DataFrame:
        # Sets the _data_raw and _data_type
        ...

    @classmethod
    @abstractmethod
    def get_context_needed(cls, type) -> ModuleDetail:
        # Returns the parameters that the module will need
        ...

    @property
    def data_raw(self):
        return self._data_raw

    @property
    def data_schema(self):
        return self._data_type

    @property
    def data_type(self):
        return self._data_type
