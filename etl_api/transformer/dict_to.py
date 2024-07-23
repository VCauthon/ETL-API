from typing import Union, Dict, Any
from json import dumps

from pandas import DataFrame

from etl_api.transformer.base import AbstractTransformation, ErrorTransformation


class DictTransformation(AbstractTransformation):
    def __init__(self, data: Any, schema: Dict[str, Union[dict, str]]) -> None:
        super().__init__(data, schema)
        self.data: dict

    @ErrorTransformation.try_catch_handler("Wrong schema for the data retrieved", (KeyError))
    def to_csv(self):
        flatten_data = {}
        for key, val in self.schema.items():
            flatten_data[key] = self.__get_data_from_schema(val, self.data)
        return DataFrame([flatten_data]).to_csv(index=False)

    def __get_data_from_schema(self, val: Union[str, list, Any], all_data: Dict[str, str]):
        if isinstance(val, str):
            return all_data[val]
        elif isinstance(val, list):
            data = all_data
            for key in val:
                data = data[key]
            return data
        raise ValueError("The schema has something that isn't an str/list")

    @ErrorTransformation.try_catch_handler("Wrong data format", (ValueError))
    def to_json(self):
        return dumps(self.data, indent=4)
