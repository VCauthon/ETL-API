import pytest

from pandas import DataFrame

from etl_api.transformer import Transformer, TransformationTypes
from etl_api.transformer.base import ErrorTransformation


DUMMY_DICT = {
    "id": 1,
    "name": "Alice Smith",
    "age": 28,
    "email": "alice.smith@example.com",
    "address": {
        "street": "123 Main St",
        "city": "Springfield",
        "zipcode": "12345"
    },
    "preferences": ["reading", "hiking", "coding"]
}


DUMMY_DATAFRAME = DataFrame(
    [
        {
            "id": [1, 2, 3],
            "name": ["Alice Smith", "Bob Johnson", "Carol Williams"],
            "age": [28, 34, 29],
            "email": ["alice.smith@example.com", "bob.johnson@example.com", "carol.williams@example.com"],
            "city": ["Springfield", "Rivertown", "Mapleton"]
            }
        ]
    )


class TestTransformer:
    def test_transformation_exist(self):
        assert Transformer.transform(TransformationTypes.JSON, DUMMY_DATAFRAME)

    def test_raise_error_transformation_not_exist(self):
        with pytest.raises(ValueError):
            assert Transformer.transform(TransformationTypes("NonExistent"), DUMMY_DATAFRAME)


class TestTransformerDataFrame:
    def test_transform_to_json(self):
        assert isinstance(
            Transformer.transform(TransformationTypes.JSON, DUMMY_DATAFRAME),
            str)

    def test_transform_to_csv(self):
        assert isinstance(
            Transformer.transform(TransformationTypes.CSV, DUMMY_DATAFRAME),
            str)


class TestTransformerDict:
    def test_transform_to_json(self):
        assert isinstance(
            Transformer.transform(TransformationTypes.JSON, DUMMY_DICT),
            str)

    def test_transform_to_csv(self):
        schema = {
            "id": "id",
            "name": "name",
            "age": "age",
            "email": "email",
            "address-street": ["address", "street"],
            "address-city": ["address", "city"],
            "address-zipcode": ["address", "zipcode"],
            "preferences": "preferences"
        }
        assert isinstance(
            Transformer.transform(TransformationTypes.CSV, DUMMY_DICT, schema),
            str)

    def test_raise_error_transform_to_csv_wrong_schema(self):
        with pytest.raises(ErrorTransformation):
            Transformer.transform(TransformationTypes.CSV, DUMMY_DICT, {"wrong": "wrong"})


if __name__ == "__main__":
    pytest.main()
