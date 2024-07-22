from unittest.mock import patch

import pytest
import pandas as pd

from etl_api.extractor import Extractor, ExtractionTypes
from etl_api.extractor.base import AbstractExtractor

# TODO: Add a concrete error for the modules

class TestExtractor:
    
    class MockDummyExtractor(AbstractExtractor):

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self._retrieve_data(**kwargs)

        def _retrieve_data(self, dummy: str = None) -> None:
            self._data_raw = {"mock_key": "mock_value"}
            if dummy:
                self._data_raw.update({"dummy": dummy})
            
            self._data_schema = {"mock_key": "string"}

    @patch('etl_api.extractor._ExtractorFactory._extractor_map', {ExtractionTypes.YahooFinance: MockDummyExtractor})
    def test_extract_without_context(self):
        raw_data, schema = Extractor.extract(ExtractionTypes.YahooFinance)
        assert raw_data == {"mock_key": "mock_value"}
        assert schema == {"mock_key": "string"}

    @patch('etl_api.extractor._ExtractorFactory._extractor_map', {ExtractionTypes.YahooFinance: MockDummyExtractor})
    def test_extract_with_context(self):
        raw_data, schema = Extractor.extract(ExtractionTypes.YahooFinance, dummy=1)
        assert raw_data == {"mock_key": "mock_value", "dummy": 1}
        assert schema == {"mock_key": "string"}

    def test_raise_error_none_existing_extraction_type(self):
        with pytest.raises(ValueError):
            Extractor.extract(ExtractionTypes("NonExistent"))


class TestExtractorYahooFinance:

    class MockTicker:
        def history(self, period: str):
            return pd.DataFrame(
                {
                    'Date': pd.date_range(start='2023-01-01', periods=5, freq='D'),
                    'Open': [100, 101, 102, 103, 104],
                    'High': [110, 111, 112, 113, 114],
                    'Low': [90, 91, 92, 93, 94],
                    'Close': [105, 106, 107, 108, 109],
                    'Volume': [1000, 1100, 1200, 1300, 1400]
                    }
                ).set_index('Date')

    @patch('etl_api.extractor.yahoofinance.yf.Ticker', return_value=MockTicker())
    def test_extract_with_context(self, mock_yf):
        raw_data, schema = Extractor.extract(ExtractionTypes.YahooFinance, ticket="MSFT", action="history", period="1mo")
        assert isinstance(raw_data, pd.DataFrame)
        assert schema is pd.DataFrame

    @patch('etl_api.extractor.yahoofinance.yf.Ticker', return_value=MockTicker())
    def test_raise_error_extract_without_context(self, mock_yf):
        with pytest.raises(TypeError):
            Extractor.extract(ExtractionTypes.YahooFinance, action="history")

if __name__ == "__main__":
    pytest.main()
