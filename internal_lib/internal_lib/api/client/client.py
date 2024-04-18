from typing import Protocol, Union
from abc import abstractmethod


class Client(Protocol):
    @abstractmethod
    def query_endpoint(
        self,
        path: str,
        params: dict = {},
        data_key: str = "",
    ) -> Union[dict, list]:
        pass
