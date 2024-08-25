from requests import get, exceptions

from etl_api.extractor.base import AbstractExtractor, ModuleDetail#, ErrorExtract
from etl_api.base import ModuleConfiguration, DataTypes


class Request(AbstractExtractor):
    def __init__(self, data_type: DataTypes, **kwargs) -> None:
        super().__init__()
        self._retrieve_data(**kwargs)
        self._data_type = data_type

    # TODO: This has to be implemented
    # @ErrorExtract.try_catch_handler(
    #     "Failed to retrieve data from the endpoint",
    #     (exceptions.RequestException)
    # )
    def _retrieve_data(self, url: str) -> None:        
        self._data_raw = get(url)
        if self._data_raw.status_code != 200:
            raise exceptions.RequestException(f"Failed to retrieve data from {url}")
        self._data_raw = self._data_raw.content.decode("utf-8")

    @classmethod
    def get_context_needed(cls) -> ModuleDetail:
        return ModuleDetail(
            name=Request.__name__,
            description="Retrieve data from an endpoint",
            config=[
                ModuleConfiguration(
                    name="url", type="str", desc="Link to the endpoint"
                ),
            ],
        )
