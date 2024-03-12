from typing import Union

from internal_lib.aws.s3 import S3
from internal_lib.files.file_paths import FilePaths

from internal_lib.files import (
    write_dics,
    write_lists,
    write_json,
    create_folder,
    write_xml,
)
from internal_lib.files.content_writer.content_type import ContentType


class ContentWriter:
    def __init__(self, s3_client: S3):
        self.s3_client = s3_client

    def write_content(
        self,
        output: list[Union[dict, list]],
        file_paths: FilePaths,
        content_type: ContentType = None,
    ):
        local_path = self._write_local_content(file_paths, output, content_type)
        self._write_s3_content(file_paths, local_path)

    def _write_local_content(
        self,
        file_paths: FilePaths,
        output: list[Union[dict, list]],
        content_type: ContentType,
    ) -> str:
        LOCAL_PATH = f"{file_paths.local_prefix}{file_paths.file_name}"
        create_folder(file_paths.local_prefix)

        dics_output = content_type == ContentType.LIST_DICS or (
            content_type is None and isinstance(output[0], dict)
        )
        lists_output = content_type == ContentType.LIST_LISTS or (
            content_type is None and isinstance(output[0], list)
        )
        json_output = content_type == ContentType.JSON
        xml_output = content_type == ContentType.XML

        if dics_output:
            write_dics(
                LOCAL_PATH,
                output,
            )

        if lists_output:
            write_lists(
                LOCAL_PATH,
                output,
            )

        if json_output:
            write_json(
                LOCAL_PATH,
                output,
            )

        if xml_output:
            write_xml(
                LOCAL_PATH,
                output,
            )

        if not (dics_output or lists_output or json_output or xml_output):
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
