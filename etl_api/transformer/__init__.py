
from enum import Enum
from typing import Union, Type, Dict

from pandas import DataFrame

from etl_api.transformer.base import AbstractTransformation
from etl_api.transformer.dict_to import DictTransformation
from etl_api.transformer.dataframe_to import DataFrameTransformation


class TransformationTypes(Enum):
    JSON = "JSON"
    CSV = "CSV"
    RAW = "RAW"


class _TransformerFactory:

    @staticmethod
    def create_transformer(data_raw, data_schema) -> Type['AbstractTransformation']:
        if isinstance(data_raw, DataFrame):
            class_selected = DataFrameTransformation
        elif isinstance(data_raw, dict):
            class_selected = DictTransformation
        else:
            raise ValueError('The type of data retrieved can\'t be transformed')
        return class_selected(data_raw, data_schema)


class Transformer:
    @staticmethod
    def transform(
            type: TransformationTypes,
            data_raw: Union[dict, DataFrame],
            data_schema: Dict[str, Union[dict, str]] = None):
        if type not in TransformationTypes:
            raise ValueError("The type of transformation doesn\'t exist")

        transformer = _TransformerFactory.create_transformer(data_raw, data_schema)
        if type == TransformationTypes.JSON:
            return transformer.to_json()
        elif type == TransformationTypes.CSV:
            return transformer.to_csv()
        elif type == TransformationTypes.RAW:
            return transformer.to_raw()
