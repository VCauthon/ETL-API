from abc import ABC, abstractmethod


from pandas import DataFrame


class AbstractExtractor(ABC):
    def __init__(self, **kwargs) -> None:
        self._data_raw = None
        self._data_schema = None

    @abstractmethod
    def _retrieve_data(self) -> DataFrame:
        # Sets the _data_raw and _data_schema
        ...

    @property
    def data_raw(self):
        return self._data_raw

    @property
    def data_schema(self):
        return self._data_schema
