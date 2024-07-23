from abc import ABC, abstractmethod
from typing import Any


class AbstractLoader(ABC):
    @staticmethod
    @abstractmethod
    def load(self, data: Any):
        ...
