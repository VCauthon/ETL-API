from abc import ABC, abstractmethod
from typing import Any


from etl_api.base import ModuleDetail


class AbstractLoader(ABC):
    @staticmethod
    @abstractmethod
    def load(self, data: Any):
        ...

    @classmethod
    @abstractmethod
    def get_context_needed(cls) -> ModuleDetail:
        # Returns the parameters that the module will need
        ...