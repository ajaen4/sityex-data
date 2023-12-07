from aws_lib.s3 import S3

import f_data_uploader.config as cfg


def get_file_path(s3_client: S3, directory_path: str):
    all_cities_paths = s3_client.list_files(
        cfg.DATA_BUCKET_NAME,
        directory_path,
    )

    if len(all_cities_paths) == 0:
        return None

    return [
        all_cities_path
        for all_cities_path in all_cities_paths
        if all_cities_path.endswith(".csv")
    ][0]
