from typing import Union

from aws_lib.s3 import S3
from files.file_paths import FilePaths

from files import write_dics, write_lists, create_folder


class ContentWriter:
    def __init__(self, s3_client: S3):
        self.s3_client = s3_client

    def write_content(self, output: list[Union[dict, list]], file_paths: FilePaths):
        local_path = self._write_local_content(file_paths, output)
        self._write_s3_content(file_paths, local_path)

    def _write_local_content(
        self, file_paths: FilePaths, output: list[Union[dict, list]]
    ) -> str:
        LOCAL_PATH = f"{file_paths.local_prefix}{file_paths.file_name}"
        create_folder(file_paths.local_prefix)

        if isinstance(output, list):
            write_lists(
                LOCAL_PATH,
                output,
            )

        if isinstance(output, dict):
            write_dics(
                LOCAL_PATH,
                output,
            )

        if not isinstance(output, list) or isinstance(output, dict):
            raise TypeError(
                f"Output type {type(output)} is not supported, must be list or dict"
            )

        return LOCAL_PATH

    def _write_s3_content(self, file_paths: FilePaths, local_path: str):
        S3_PATH = f"{file_paths.s3_prefix}{file_paths.file_name}"
        self.s3_client.upload_file(
            local_path,
            file_paths.bucket_name,
            S3_PATH,
        )
