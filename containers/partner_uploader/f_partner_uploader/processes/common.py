from aws_lib.s3 import S3

import f_partner_uploader.config as cfg


def get_file_path(s3_client: S3, directory_path: str):
    all_paths = s3_client.list_files(
        cfg.DATA_BUCKET_NAME,
        directory_path,
    )

    if len(all_paths) == 0:
        return None

    return [path for path in all_paths if path.endswith(".csv")][0]
