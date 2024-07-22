from abc import ABC, abstractmethod
from typing import Union, Dict, Any


class ErrorTransformation(Exception):
    def __init__(self, msg: str, *args: object) -> None:
        super().__init__(*args)
        self.message = msg

    @staticmethod
    def try_catch_handler(message_error: str, concrete_error: tuple):
        def wrapper(func):
            def args_wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except concrete_error as c_error:
                    raise ErrorTransformation(message_error) from c_error
                except Exception as b_error: 
                    raise b_error
            return args_wrapper
        return wrapper


class AbstractTransformation(ABC):
    
    def __init__(self, data: Any, schema: Dict[str, Union[dict, str]]) -> None:
        super().__init__()
        self.__data = data
        self.__schema = schema

    @property
    def data(self) -> Any:
        return self.__data

    @property
    def schema(self) -> Dict[str, Union[dict, str]]:
        return self.__schema    

    @abstractmethod
    def to_csv(self) -> str:
        ...
    
    @abstractmethod
    def to_json(self) -> str:
        ...

    def to_raw(self):
        return self.data
