import pytest
from unittest.mock import MagicMock, patch
from flask.testing import FlaskClient
from flask import Flask, request

from etl_api.app import app, parse_arguments
from etl_api.extractor import ExtractionTypes, Extractor
from etl_api.base import ModuleConfiguration, ModuleDetail


@pytest.fixture
def client() -> FlaskClient:  # type: ignore
    with app.test_client() as client:
        yield client


@pytest.fixture
def mock_extractor():
    with patch("etl_api.app.Extractor") as mock:
        mock.Arguments.return_value = MagicMock()
        mock.extract.return_value = (
            {"sample_data": "data"},
            {"sample_schema": "schema"},
        )
        yield mock


@pytest.fixture
def mock_transformer():
    with patch("etl_api.app.Transformer") as mock:
        mock.Arguments.return_value = MagicMock()
        mock.transform.return_value = {"transformed_data": "data"}
        yield mock


@pytest.fixture
def mock_loader():
    with patch("etl_api.app.Loader") as mock:
        mock.Arguments.return_value = MagicMock()
        mock.load.return_value = {"status": "success"}
        yield mock


def test_list_extraction_options(client: FlaskClient):
    response = client.get("/help")
    assert response.status_code == 200
    assert response.json


@pytest.mark.parametrize("args_extractor", [val for val in ExtractionTypes])
def test_parse_arguments(args_extractor: ModuleDetail):
    args = Extractor.get_options(args_extractor)  # TODO: The action of get_options must be done by the own decorator
    assert isinstance(args.config[0], ModuleConfiguration)

    @parse_arguments(args)
    def dummy_function(*args, **kwargs):
        return True

    expected_arguments = {key.name: True for key in args.config}
    with app.test_request_context(query_string=expected_arguments):
        with patch('etl_api.app.request.args.to_dict', return_value=expected_arguments):
            assert dummy_function(**expected_arguments)


def test_yahoo_finance_get_data(
    client: FlaskClient, mock_extractor, mock_transformer, mock_loader
):
    response = client.get(
        "/api/yahoofinance", query_string={"period": "1mo", "ticker": "MSFT"}
    )
    assert response.status_code == 200
    assert response.json == {"status": "success"}


def test_request_get_csv(
    client: FlaskClient, mock_extractor, mock_transformer, mock_loader
):
    response = client.get(
        "/request/get_csv", query_string={"url": "www.dummy_csv.com/endpoint"}
    )
    assert response.status_code == 200
    assert response.json == {"status": "success"}


if __name__ == "__main__":
    pytest.main()
