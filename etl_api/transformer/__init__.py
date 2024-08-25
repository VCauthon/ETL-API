from dataclasses import dataclass
from enum import Enum
from typing import Union, Type, Dict

from pandas import DataFrame

from etl_api.base import DataTypes
from etl_api.transformer.base import AbstractTransformation
from etl_api.transformer.dict_to import DictTransformation
from etl_api.transformer.dataframe_to import DataFrameTransformation
from etl_api.transformer.csv_to import CSVTransformation


class TransformationTypes(Enum):
    JSON = "JSON"
    CSV = "CSV"
    RAW = "RAW"


class _TransformerFactory:
    @staticmethod
    def create_transformer(data_raw, data_type: DataTypes, data_schema = None) -> Type["AbstractTransformation"]:
        if data_type == DataTypes.DATAFRAME:
            class_selected = DataFrameTransformation
        elif data_type == DataTypes.DICT:
            class_selected = DictTransformation
        elif data_type == DataTypes.CSV:
            class_selected = CSVTransformation
        else:
            raise ValueError("The type of data retrieved can't be transformed")
        return class_selected(data_raw, data_schema)


class Transformer:
    @dataclass
    class Arguments:
        type: TransformationTypes
        data_raw = None
        data_schema = None

    @staticmethod
    def transform(
        type: TransformationTypes,
        data_raw: Union[dict, DataFrame],
        data_type: DataTypes,
        data_schema: Dict[str, Union[dict, str]] = None,
    ):
        if type not in TransformationTypes:
            raise ValueError("The type of transformation doesn't exist")

        transformer = _TransformerFactory.create_transformer(data_raw, data_type, data_schema)
        if type == TransformationTypes.JSON:
            return transformer.to_json()
        elif type == TransformationTypes.CSV:
            return transformer.to_csv()
        elif type == TransformationTypes.RAW:
            return transformer.to_raw()
