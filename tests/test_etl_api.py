import pytest
from unittest.mock import MagicMock, patch
from flask.testing import FlaskClient
from etl_api.main import app


@pytest.fixture
def client() -> FlaskClient:
    with app.test_client() as client:
        yield client


@pytest.fixture
def mock_extractor():
    with patch('etl_api.main.Extractor') as mock:
        mock.Arguments.return_value = MagicMock()
        mock.extract.return_value = ({"sample_data": "data"}, {"sample_schema": "schema"})
        yield mock


@pytest.fixture
def mock_transformer():
    with patch('etl_api.main.Transformer') as mock:
        mock.Arguments.return_value = MagicMock()
        mock.transform.return_value = {"transformed_data": "data"}
        yield mock


@pytest.fixture
def mock_loader():
    with patch('etl_api.main.Loader') as mock:
        mock.Arguments.return_value = MagicMock()
        mock.load.return_value = {"status": "success"}
        yield mock


def test_list_extraction_options(client: FlaskClient):
    response = client.get('/api/help')
    assert response.status_code == 200
    assert response.json


def test_yahoofinance_get_data(client: FlaskClient, mock_extractor, mock_transformer, mock_loader):
    response = client.get('/api/yahoofinance', query_string={'period': '1mo', 'ticker': 'MSFT'})
    assert response.status_code == 200
    assert response.json == {"status": "success"}

if __name__ == "__main__":
    pytest.main()
