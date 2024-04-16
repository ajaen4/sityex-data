import requests
from typing import Any
import boto3

from internal_lib.aws.ssm import SSM

from internal_lib.logger import logger
from internal_lib.api.client import Client
from .exceptions.limit_exceeded import APILimitExceeded


class OpenMeteoClient(Client):
    def __init__(self, host: str) -> None:
        self.base_url = f"https://{host}"

        ssm_client = SSM(boto3.Session())
        self.api_key = ssm_client.get_parameter(
            "/open_meteo/api_key", decrypt=True
        )

    def query_endpoint(
        self,
        path: str,
        params: dict = {},
        data_key: str = "",
    ) -> Any:
        logger.info(f"Fetching path: {path}...")

        url = self._get_url(path)
        api_key_url = f"{url}?&apikey={self.api_key}"
        response = self._send_request(api_key_url, params)

        if "error" in response:
            raise Exception(response["reason"])

        logger.info(f"Num records fetched: {len(response)}")

        return response

    def _send_request(self, url: str, params) -> Any:
        response = requests.get(url, params).json()

        if "error" in response and "exceeded" in response["reason"]:
            raise APILimitExceeded(url)

        return response

    def _get_url(self, path: str) -> str:
        return f"{self.base_url}{path}"
