import requests
import boto3
from typing import Any
import time

from internal_lib.aws.ssm import SSM

from internal_lib.logger import logger
from internal_lib.api.client import Client


class GeoDBClient(Client):
    def __init__(self, host: str):
        ssm_client = SSM(boto3.Session())
        self.api_key = ssm_client.get_parameter(
            "/rapid_api/api_key", decrypt=True
        )
        self.base_url = f"https://{host}"
        self.headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": host,
        }

    def query_endpoint(
        self,
        path: str,
        params: dict = {},
        data_key: str = "",
    ) -> list:
        records = []
        next_link = ""

        logger.info(f"Fetching path: {path}...")
        while True:
            params = {} if next_link else params
            url = self._get_next_url(path, next_link)
            response = self._send_request(url, params)

            records.extend(response["data"])
            time.sleep(2)
            next_link = GeoDBClient._get_next_link(response)

            if not next_link:
                break

        logger.info(f"Num records fetched: {len(records)}")

        return records

    def _get_next_url(self, path: str, next_link: str = "") -> str:
        return (
            f"{self.base_url}{next_link}"
            if next_link
            else f"{self.base_url}{path}"
        )

    def _send_request(self, url, params) -> Any:
        return requests.get(url, headers=self.headers, params=params).json()

    @staticmethod
    def _get_next_link(response) -> str:
        for link in response["links"]:
            if link["rel"] == "next":
                return link["href"]
        return ""
