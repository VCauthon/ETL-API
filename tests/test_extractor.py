from unittest.mock import patch

import csv
import io
import pytest
import pandas as pd
import requests as rq

from etl_api.base import DataTypes
from etl_api.extractor import Extractor, ExtractionTypes
from etl_api.extractor.base import AbstractExtractor

# TODO: Add a concrete error for the modules
# TODO: Add a test to check that get_context_needed returns the expected values


class TestExtractor:
    class MockDummyExtractor(AbstractExtractor):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self._retrieve_data(**kwargs)

        @classmethod
        def get_context_needed(cls) -> None:
            ...

        def _retrieve_data(self, dummy: str = None) -> None:
            self._data_raw = {"mock_key": "mock_value"}
            if dummy:
                self._data_raw.update({"dummy": dummy})

            self._data_type = DataTypes.DICT

    @patch(
        "etl_api.extractor._ExtractorFactory._extractor_map",
        {ExtractionTypes.YahooFinance: MockDummyExtractor},
    )
    def test_extract_without_context(self):
        raw_data, data_type = Extractor.extract(ExtractionTypes.YahooFinance)
        assert raw_data == {"mock_key": "mock_value"}
        assert data_type == DataTypes.DICT

    @patch(
        "etl_api.extractor._ExtractorFactory._extractor_map",
        {ExtractionTypes.YahooFinance: MockDummyExtractor},
    )
    def test_extract_with_context(self):
        raw_data, data_type = Extractor.extract(ExtractionTypes.YahooFinance, dummy=1)
        assert raw_data == {"mock_key": "mock_value", "dummy": 1}
        assert data_type == DataTypes.DICT

    def test_raise_error_none_existing_extraction_type(self):
        with pytest.raises(ValueError):
            Extractor.extract(ExtractionTypes("NonExistent"))


class TestExtractorYahooFinance:
    class MockTicker:
        def history(self, period: str):
            return pd.DataFrame(
                {
                    "Date": pd.date_range(start="2023-01-01", periods=5, freq="D"),
                    "Open": [100, 101, 102, 103, 104],
                    "High": [110, 111, 112, 113, 114],
                    "Low": [90, 91, 92, 93, 94],
                    "Close": [105, 106, 107, 108, 109],
                    "Volume": [1000, 1100, 1200, 1300, 1400],
                }
            ).set_index("Date")

    @patch("etl_api.extractor.yahoofinance.yf.Ticker", return_value=MockTicker())
    def test_extract_with_context(self, mock_yf):
        raw_data, schema = Extractor.extract(
            ExtractionTypes.YahooFinance, ticker="MSFT", period="1mo"
        )
        assert isinstance(raw_data, pd.DataFrame)
        assert schema is DataTypes.DATAFRAME

    @patch("etl_api.extractor.yahoofinance.yf.Ticker", return_value=MockTicker())
    def test_raise_error_extract_without_context(self, mock_yf):
        with pytest.raises(TypeError):
            Extractor.extract(ExtractionTypes.YahooFinance)


class TestExtractorRequest:

    class MockRawCSVResponse:

        def __init__(self) -> None:
            self.__content = None
            self.status_code = 200

        @property
        def content(self) -> bytes:
            string_output = io.StringIO()
            writer = csv.writer(string_output)

            # Write some dummy data
            writer.writerow(['Column1', 'Column2', 'Column3'])
            writer.writerow(['Data1', 'Data2', 'Data3'])
            writer.writerow(['Data4', 'Data5', 'Data6'])
            
            # Get the string content from the StringIO object
            return string_output.getvalue().encode('utf-8')

    class MockWrongResponse:
        def __init__(self) -> None:
            self.status_code = 400

    @patch("etl_api.extractor.request.get", return_value=MockRawCSVResponse())
    def test_extract_csv_from_endpoint(self, mock_requests_get):
        raw_data, schema = Extractor.extract(
            ExtractionTypes.Request,
            url="www.dummy.com",
            data_type=DataTypes.CSV
        )
        assert isinstance(raw_data, str)
        assert schema is DataTypes.CSV

    def test_raise_error_extract_without_context(self):
        with pytest.raises(TypeError):
            Extractor.extract(ExtractionTypes.Request)

    @patch("etl_api.extractor.request.get", return_value=MockWrongResponse())
    def test_raise_error_status_code_wrong(self, mock_requests_get):
        with pytest.raises(rq.exceptions.RequestException):
            Extractor.extract(
                ExtractionTypes.Request, url="┐(シ)┌", data_type=DataTypes.CSV)


# TODO: End using black to format all the project

if __name__ == "__main__":
    pytest.main()
