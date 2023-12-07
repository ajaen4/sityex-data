import requests
import boto3
import time

from aws_lib.ssm import SSM

from f_api_extractor.logger import logger
from .client import Client


class CountCitClient(Client):
    def __init__(self, host: str):
        ssm_client = SSM(boto3.Session())
        self.api_key = ssm_client.get_parameter("/rapid_api/api_key", decrypt=True)
        self.base_url = f"https://{host}"
        self.headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": host,
        }

    def query_endpoint(self, path: str, params: dict = None, data_key: str = None):
        records = []
        next_link = None

        num_requests = 0
        logger.info(f"Fetching path: {path}...")
        while True:
            params = None if next_link else params
            url = self._get_next_url(path, next_link)
            response = self._send_request(url, params)
            time.sleep(2)
            num_requests += 1
            next_link = self._get_next_link(response)

            if response["status"] == "failed":
                error_mess = response["error"]["message"]

                if error_mess == "No data found":
                    logger.info(f"No data found for: {path}...")
                    return None

                raise Exception(f"Request failed for {url}, message: {error_mess}")

            data = response if not data_key else response[data_key]
            if isinstance(data, dict):
                data = [data]

            records.extend(data)

            if not next_link:
                break

        logger.info(f"Num records fetched: {len(records)}")

        return records

    def _get_next_url(self, path: str, next_link: str = None):
        return f"{self.base_url}{next_link}" if next_link else f"{self.base_url}{path}"

    def _send_request(self, url, params):
        return requests.get(url, headers=self.headers, params=params).json()

    def _get_next_link(self, response):
        if "links" not in response:
            return None

        for link_type, link in response["links"].items():
            if link_type == "next" and link:
                return f"/location{link}"
        return None
