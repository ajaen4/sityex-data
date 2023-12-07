from dataclasses import dataclass


@dataclass
class FilePaths:
    bucket_name: str
    s3_prefix: str
    local_prefix: str
    file_name: str
