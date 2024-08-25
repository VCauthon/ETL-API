import pytest

import io
import csv
from pandas import DataFrame

from etl_api.base import DataTypes
from etl_api.transformer import Transformer, TransformationTypes
from etl_api.transformer.base import ErrorTransformation


DUMMY_DICT = {
    "id": 1,
    "name": "Alice Smith",
    "age": 28,
    "email": "alice.smith@example.com",
    "address": {"street": "123 Main St", "city": "Springfield", "zipcode": "12345"},
    "preferences": ["reading", "hiking", "coding"],
}


DUMMY_DATAFRAME = DataFrame(
    [
        {
            "id": [1, 2, 3],
            "name": ["Alice Smith", "Bob Johnson", "Carol Williams"],
            "age": [28, 34, 29],
            "email": [
                "alice.smith@example.com",
                "bob.johnson@example.com",
                "carol.williams@example.com",
            ],
            "city": ["Springfield", "Rivertown", "Mapleton"],
        }
    ]
)


@pytest.fixture
def DUMMY_CSV_RAW():
        string_output = io.StringIO()
        writer = csv.writer(string_output)

        # Write some dummy data
        writer.writerow(['Column1', 'Column2', 'Column3'])
        writer.writerow(['Data1', 'Data2', 'Data3'])
        writer.writerow(['Data4', 'Data5', 'Data6'])
        
        # Get the string content from the StringIO object
        return string_output.getvalue()


class TestTransformer:
    def test_transformation_exist(self):
        assert Transformer.transform(TransformationTypes.JSON, DUMMY_DATAFRAME, DataTypes.DATAFRAME)

    def test_raise_error_transformation_not_exist(self):
        with pytest.raises(ValueError):
            assert Transformer.transform(
                TransformationTypes("NonExistent"), DUMMY_DATAFRAME
            )


class TestTransformerDataFrame:
    def test_transform_to_json(self):
        assert isinstance(
            Transformer.transform(TransformationTypes.JSON, DUMMY_DATAFRAME, DataTypes.DATAFRAME), str
        )

    def test_transform_to_csv(self):
        assert isinstance(
            Transformer.transform(TransformationTypes.CSV, DUMMY_DATAFRAME, DataTypes.DATAFRAME), str
        )


class TestTransformerDict:
    def test_transform_to_json(self):
        assert isinstance(
            Transformer.transform(TransformationTypes.JSON, DUMMY_DICT, DataTypes.DICT), str
        )

    def test_transform_to_csv(self):
        schema = {
            "id": "id",
            "name": "name",
            "age": "age",
            "email": "email",
            "address-street": ["address", "street"],
            "address-city": ["address", "city"],
            "address-zipcode": ["address", "zipcode"],
            "preferences": "preferences",
        }
        assert isinstance(
            Transformer.transform(TransformationTypes.CSV, DUMMY_DICT, DataTypes.DICT, schema), str
        )

    def test_raise_error_transform_to_csv_wrong_schema(self):
        with pytest.raises(ErrorTransformation):
            Transformer.transform(
                TransformationTypes.CSV, DUMMY_DICT, DataTypes.DICT, {"wrong": "wrong"}
            )


class TestTransformerCSV:
    def test_transform_to_json(self, DUMMY_CSV_RAW: str):
        assert isinstance(
            Transformer.transform(TransformationTypes.JSON, DUMMY_CSV_RAW, DataTypes.CSV), str
        )

    def test_transform_to_csv(self, DUMMY_CSV_RAW: str):
        assert isinstance(
            Transformer.transform(TransformationTypes.CSV, DUMMY_CSV_RAW, DataTypes.CSV), str
        )


if __name__ == "__main__":
    pytest.main()
