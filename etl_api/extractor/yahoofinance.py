from pandas import DataFrame
import yfinance as yf

from etl_api.extractor.base import AbstractExtractor


class YahooFinance(AbstractExtractor):

    def __init__(self, **kwargs) -> None:
        super().__init__()
        self._retrieve_data(**kwargs)

    def _retrieve_data(self, ticket: str, period: str, action: str) -> None:
        if action == "history":    
            obj_yf = yf.Ticker(ticket)
            self._data_raw = obj_yf.history(period=period)
            self._data_schema = DataFrame
        else:
            raise ValueError("Invalid action")
