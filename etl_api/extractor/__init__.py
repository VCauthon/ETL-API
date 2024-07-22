from enum import Enum
from typing import Type, Tuple, Dict, Any

from etl_api.extractor.base import AbstractExtractor
from etl_api.extractor.yahoofinance import YahooFinance


class ExtractionTypes(Enum):
    YahooFinance = "YahooFinance"


class _ExtractorFactory:
    _extractor_map = {
        ExtractionTypes.YahooFinance: YahooFinance
    }

    @classmethod
    def create_extractor(cls, api: ExtractionTypes) -> Type[AbstractExtractor]:
        extractor_class = cls._extractor_map.get(api)
        if extractor_class is None:
            raise ValueError(f"No extractor found for the given extraction type: {api.value}")
        return extractor_class


class Extractor:
    @classmethod
    def extract(cls, api: ExtractionTypes, **kwargs) -> Tuple[Any, Dict[str, str]]:
        if api not in ExtractionTypes:
            raise ValueError(f"Invalid extraction type: {api}. Must be a value from ExtractionTypes")

        results_request = _ExtractorFactory.create_extractor(api)(**kwargs)
        return results_request.data_raw, results_request.data_schema
