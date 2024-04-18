import json
from json import JSONDecodeError
from boto3.session import Session
from typing import Any


from internal_lib.logger import logger
from internal_lib.files import format_dics


class S3:
    def __init__(self, session: Session) -> None:
        self.s3_client = session.client("s3")

    def upload_file(self, file_path: str, bucket_name: str, key: str) -> None:
        return self.s3_client.upload_file(file_path, bucket_name, key)

    def read_dics(
        self, bucket_name: str, file_key: str
    ) -> list[dict[str, Any]]:
        s3_obj = self.s3_client.get_object(Bucket=bucket_name, Key=file_key)
        csv_content = s3_obj["Body"].read().decode("utf-8")
        return format_dics(csv_content)

    def read_json(self, bucket_name: str, file_key: str) -> list[dict]:
        s3_obj = self.s3_client.get_object(Bucket=bucket_name, Key=file_key)
        content = s3_obj["Body"].read().decode("utf-8")

        json_objects = []
        for line in content.splitlines():
            try:
                json_objects.append(json.loads(line))
            except JSONDecodeError as e:
                logger.error(f"Error reading line: {line}")
                logger.error(e)
                continue

        return json_objects

    def list_files(
        self, bucket_name: str, prefix: str, suffix: str = ""
    ) -> list[str]:
        paginator = self.s3_client.get_paginator("list_objects_v2")
        file_names = []

        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                file_names.append(obj["Key"])

        if suffix:
            file_names = [
                file_name
                for file_name in file_names
                if file_name.endswith(suffix)
            ]

        return file_names
