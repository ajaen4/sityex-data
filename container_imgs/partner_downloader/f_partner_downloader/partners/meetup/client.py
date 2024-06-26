import requests
import boto3
import jwt
import datetime
import json
from typing import Any

from internal_lib.aws.ssm import SSM
from internal_lib.api.client import Client

from internal_lib.logger import logger


class MeetupClient(Client):
    def __init__(self, host: str) -> None:
        self.base_url = f"https://{host}"
        self.authenticate()

    def authenticate(self) -> None:
        ssm_client = SSM(boto3.Session())
        raw_key = ssm_client.get_parameter("/meetup/signing_key", decrypt=True)
        meetup_key = json.loads(raw_key)

        EXPIRE_SECONDS = 3600
        auth_payload = {
            "sub": meetup_key["member_id"],
            "iss": meetup_key["client_key"],
            "aud": "api.meetup.com",
            "exp": datetime.datetime.utcnow()
            + datetime.timedelta(seconds=EXPIRE_SECONDS),
        }

        key_headers = {
            "kid": meetup_key["key_id"],
            "typ": "JWT",
            "alg": "RS256",
        }

        signed_jwt = jwt.encode(
            auth_payload,
            meetup_key["private_key"],
            algorithm="RS256",
            headers=key_headers,
        )

        auth_headers = {"Content-Type": "application/x-www-form-urlencoded"}
        auth_data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": signed_jwt,
        }
        auth_response = self._send_request(
            "https://secure.meetup.com/oauth2/access",
            data=auth_data,
            headers=auth_headers,
        )
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {auth_response['access_token']}",
        }

    def query_endpoint(
        self,
        path: str,
        params: dict = {},
        data_key: str = "",
    ) -> Any:
        logger.info("Querying Meetup api...")

        if not params:
            params["query"] = ""
            params["variables"] = {}

        data = {
            "query": params["query"],
            "variables": {**params["variables"]},
        }
        response = self._send_request(
            self._get_url(path), self.headers, json=data
        )

        logger.info("Queried Meetup api")

        return response

    def _get_url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def _send_request(
        self,
        url: str,
        headers: dict,
        data: dict = {},
        json: dict = {},
    ) -> Any:
        return requests.post(url, headers=headers, data=data, json=json).json()
