from typing import Union, Dict, Any
from io import StringIO

from pandas import DataFrame, read_csv, errors

from etl_api.transformer.base import AbstractTransformation, ErrorTransformation


class CSVTransformation(AbstractTransformation):
    def __init__(self, data: Any, schema: Dict[str, Union[dict, str]]) -> None:
        super().__init__(read_csv(StringIO(data)), schema)
        self.data: DataFrame

    @ErrorTransformation.try_catch_handler(
        "Wrong data format", (ValueError, KeyError, TypeError, errors.EmptyDataError)
    )
    def to_csv(self):
        return self.data.to_csv(index=False)

    @ErrorTransformation.try_catch_handler(
        "Wrong data format", (ValueError, KeyError, TypeError, errors.EmptyDataError)
    )
    def to_json(self):
        return self.data.to_json(index=False)
