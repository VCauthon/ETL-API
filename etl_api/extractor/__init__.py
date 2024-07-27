from dataclasses import dataclass
from enum import Enum
from typing import Type, Tuple, Dict, Any, List, overload, Union

from etl_api.extractor.base import AbstractExtractor, ModuleDetail
from etl_api.extractor.yahoofinance import YahooFinance


class ExtractionTypes(Enum):
    YahooFinance = "YahooFinance"


class _ExtractorFactory:
    _extractor_map: Dict[str, AbstractExtractor] = {
        ExtractionTypes.YahooFinance: YahooFinance
    }

    @classmethod
    def create_extractor(cls, api: ExtractionTypes) -> Type[AbstractExtractor]:
        extractor_class = cls._extractor_map.get(api)
        if extractor_class is None:
            raise ValueError(
                f"No extractor found for the given extraction type: {api.value}"
            )
        return extractor_class

    @classmethod
    def list_modules(cls) -> List[ModuleDetail]:
        return [module.get_context_needed() for module in cls._extractor_map.values()]


class Extractor:
    @dataclass
    class Arguments:
        api: ExtractionTypes
        arguments: Dict[str, Any]

    @classmethod
    def extract(cls, api: ExtractionTypes, **kwargs) -> Tuple[Any, Dict[str, str]]:
        if api not in ExtractionTypes:
            raise ValueError(
                f"Invalid extraction type: {api}. Must be a value from ExtractionTypes"
            )

        results_request = _ExtractorFactory.create_extractor(api)(**kwargs)
        return results_request.data_raw, results_request.data_schema

    @overload
    @staticmethod
    def get_options() -> List[ModuleDetail]:
        ...

    @overload
    @staticmethod
    def get_options(module_details: ExtractionTypes) -> ModuleDetail:
        ...

    @staticmethod
    def get_options(
        module_details: Union[None, ExtractionTypes] = None
    ) -> Union[List[ModuleDetail], ModuleDetail]:
        all_modules = _ExtractorFactory.list_modules()

        if module_details is None:
            return all_modules

        return next(val for val in all_modules if val.name == module_details.value)
