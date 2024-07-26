from pandas import DataFrame
import yfinance as yf

from etl_api.extractor.base import AbstractExtractor, ModuleDetail
from etl_api.base import ModuleConfiguration


class YahooFinance(AbstractExtractor):
    def __init__(self, **kwargs) -> None:
        super().__init__()
        self._retrieve_data(**kwargs)

    def _retrieve_data(self, ticker: str, period: str) -> None:
        obj_yf = yf.Ticker(ticker)
        self._data_raw = obj_yf.history(period=period)
        self._data_schema = DataFrame

    @classmethod
    def get_context_needed(cls) -> ModuleDetail:
        return ModuleDetail(
            api_entry_point=True,
            name=YahooFinance.__name__,
            description="Get historical market data from a ticker or ISIN",
            config=[
                ModuleConfiguration(
                    name="ticker", type="str", desc="Ticker symbol or ISIN"
                ),
                ModuleConfiguration(
                    name="period",
                    type="str",
                    desc="Time period from which the data will be retrieved",
                ),
            ],
        )
