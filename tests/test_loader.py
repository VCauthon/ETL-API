from unittest.mock import patch
import pytest

from flask import Response, Flask

from etl_api.loader import Loader, LoaderType
from etl_api.loader.base import AbstractLoader


DUMMY_DICT = {
    "id": 1,
    "name": "John Doe",
    "email": "john.doe@example.com",
    "phone": "123-456-7890",
    "address": {
        "street": "123 Main St",
        "city": "Anytown",
        "state": "CA",
        "zip": "12345",
    },
}


class TestLoader:
    class MockDummyLoader(AbstractLoader):
        @staticmethod
        def load(_: str = None) -> bool:
            return True

    @patch(
        "etl_api.loader._LoaderFactory._loader_map",
        {LoaderType.RETURN_REQUEST: MockDummyLoader},
    )
    def test_loader_with_context(self):
        assert Loader.load(type=LoaderType.RETURN_REQUEST, data="dummy_data")

    def test_raise_error_none_existing_extraction_type(self):
        with pytest.raises(ValueError):
            Loader.load(type=LoaderType("NonExistent"), data="dummy_data")


class TestReturnRequest:
    def test_loader_without_context(self):
        app = Flask(__name__)
        with app.app_context():
            response = Loader.load(type=LoaderType.RETURN_REQUEST, data=DUMMY_DICT)
            assert isinstance(response, Response)
            assert response.json == DUMMY_DICT


if __name__ == "__main__":
    pytest.main()
