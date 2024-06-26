import requests
from typing import Any

from internal_lib.logger import logger
from internal_lib.api.client import Client


class IMFClient(Client):
    def __init__(self, host: str) -> None:
        self.base_url = f"https://{host}"

    def query_endpoint(
        self,
        path: str,
        params: dict = {},
        data_key: str = "",
    ) -> Any:
        logger.info(f"Fetching path: {path}...")

        url = self._get_url(path)
        response = self._send_request(url)

        logger.info(f"Num records fetched: {len(response)}")

        return response

    def _get_url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def _send_request(self, url: str) -> Any:
        indicator = IMFClient._get_indicator(url)
        return requests.get(url).json()["values"][indicator]

    @staticmethod
    def _get_indicator(url: str) -> str:
        return url.split("/")[-1]
