from typing import Protocol, Optional, Union
from abc import abstractmethod


class Client(Protocol):
    @abstractmethod
    def query_endpoint(
        self, path: str, params: Optional[dict] = None, data_key: Optional[str] = None
    ) -> Union[dict, list]:
        pass
